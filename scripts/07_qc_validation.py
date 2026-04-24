"""
Step 07: Quality Control and Metadata Validation.

Checks:
- CRS consistency across all PostGIS layers
- Geometry validity (invalid, self-intersecting)
- Raster statistics and value ranges
- Attribute completeness
- Metadata registry population

Usage:
    conda activate geospatial
    python scripts/07_qc_validation.py
"""

import psycopg2
import rasterio
import numpy as np
import os
from datetime import date

DB = {
    "host": "127.0.0.1",
    "port": 5433,
    "dbname": "geospatial",
    "user": "geouser",
    "password": "geopass",
}

PROC_DIR = r"C:\RMTPROJECTS\dataengineering\geospatial\data\processed"
EXPECTED_SRID = 32613
DENVER_ELEV_RANGE = (1500, 2200)  # meters


def get_connection():
    return psycopg2.connect(**DB)


def check_crs_consistency():
    """Verify all geometry columns use EPSG:32613."""
    print("\n[1] CRS Consistency")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT f_table_schema, f_table_name, f_geometry_column, srid
                FROM geometry_columns
                WHERE f_table_schema IN ('raw','processed','analysis')
                ORDER BY f_table_schema, f_table_name
            """)
            rows = cur.fetchall()

    failures = 0
    for schema, table, col, srid in rows:
        status = "OK" if srid == EXPECTED_SRID else "FAIL"
        if status == "FAIL":
            failures += 1
        print(f"  [{status}] {schema}.{table}.{col} — SRID={srid}")

    cur_r = get_connection().cursor()
    cur_r.execute("SELECT r_table_schema, r_table_name, srid FROM raster_columns WHERE r_table_schema='raw'")
    for schema, table, srid in cur_r.fetchall():
        status = "OK" if srid == EXPECTED_SRID else "FAIL"
        if status == "FAIL":
            failures += 1
        print(f"  [{status}] {schema}.{table} (raster) — SRID={srid}")

    print(f"  → CRS failures: {failures}")
    return failures


def check_geometry_validity():
    """Count invalid and self-intersecting geometries."""
    print("\n[2] Geometry Validity")
    checks = [
        ("raw.buildings",     "NOT ST_IsValid(geom)", "Invalid geometries"),
        ("raw.roads",         "NOT ST_IsSimple(geom)", "Self-intersecting roads"),
        ("raw.landuse",       "NOT ST_IsValid(geom)", "Invalid land use polys"),
        ("processed.buildings", "NOT ST_IsValid(geom)", "Invalid proc. buildings"),
    ]
    total_issues = 0
    with get_connection() as conn:
        with conn.cursor() as cur:
            for table, condition, label in checks:
                try:
                    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {condition}")
                    invalid = cur.fetchone()[0]
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    total = cur.fetchone()[0]
                    pct = invalid / total * 100 if total else 0
                    status = "OK" if pct < 1 else "WARN"
                    print(f"  [{status}] {label}: {invalid}/{total} ({pct:.2f}%)")
                    total_issues += invalid
                except Exception as e:
                    print(f"  [ERROR] {table}: {e}")
    print(f"  → Total validity issues: {total_issues}")
    return total_issues


def check_raster_statistics():
    """Validate raster value ranges for DEM and nDSM."""
    print("\n[3] Raster Statistics")
    rasters = [
        (f"{PROC_DIR}/dem_filled.tif", "DEM (filled)", DENVER_ELEV_RANGE),
        (f"{PROC_DIR}/ndsm.tif", "nDSM", (0, 250)),
        (f"{PROC_DIR}/slope.tif", "Slope", (0, 90)),
    ]
    issues = 0
    for path, label, (expected_min, expected_max) in rasters:
        if not os.path.exists(path):
            print(f"  [SKIP] {label} — file not found: {path}")
            continue
        with rasterio.open(path) as src:
            data = src.read(1, masked=True)
            rmin, rmax, rmean = float(data.min()), float(data.max()), float(data.mean())
            nodata_pct = data.mask.sum() / data.size * 100

        ok_range = expected_min <= rmin and rmax <= expected_max * 1.2
        status = "OK" if ok_range else "WARN"
        if not ok_range:
            issues += 1
        print(f"  [{status}] {label}: min={rmin:.1f} max={rmax:.1f} mean={rmean:.1f}  nodata={nodata_pct:.1f}%")
        print(f"         Expected range: {expected_min}–{expected_max}")

    return issues


def check_attribute_completeness():
    """Check null rates for key attributes."""
    print("\n[4] Attribute Completeness")
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Buildings height completeness
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(height_m) AS has_height,
                    COUNT(name) AS has_name,
                    ROUND(COUNT(height_m) * 100.0 / COUNT(*), 1) AS height_pct
                FROM processed.buildings
            """)
            row = cur.fetchone()
            if row:
                print(f"  Buildings: {row[0]:,} total | height: {row[2]:,} ({row[3]}%) | name: {row[1]:,}")

            # Road type completeness
            cur.execute("""
                SELECT COUNT(*) AS total, COUNT(highway) AS has_type
                FROM raw.roads
            """)
            row = cur.fetchone()
            if row:
                print(f"  Roads: {row[0]:,} total | highway type: {row[1]:,} ({row[1]/row[0]*100:.1f}%)")

            # Land use completeness
            cur.execute("SELECT COUNT(*), COUNT(landuse) FROM raw.landuse")
            row = cur.fetchone()
            if row:
                print(f"  Land use: {row[0]:,} total | tagged: {row[1]:,} ({row[1]/row[0]*100:.1f}%)")


def populate_metadata():
    """Insert layer metadata records."""
    print("\n[5] Populating Metadata Registry")
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS analysis.layer_metadata (
                    id SERIAL PRIMARY KEY,
                    layer_name TEXT NOT NULL,
                    schema_name TEXT NOT NULL,
                    data_type TEXT,
                    source TEXT,
                    source_date DATE,
                    crs TEXT DEFAULT 'EPSG:32613',
                    resolution_m NUMERIC,
                    feature_count INT,
                    lineage TEXT,
                    loaded_at TIMESTAMPTZ DEFAULT NOW()
                )
            """)

            layers = [
                ("buildings",  "raw",       "vector",  "OpenStreetMap",             "2024-01-01", None,
                 "SELECT COUNT(*) FROM raw.buildings",
                 "osmnx download → ogr2ogr → PostGIS"),
                ("roads",      "raw",       "vector",  "OpenStreetMap",             "2024-01-01", None,
                 "SELECT COUNT(*) FROM raw.roads",
                 "osmnx download → ogr2ogr → PostGIS"),
                ("landuse",    "raw",       "vector",  "OpenStreetMap",             "2024-01-01", None,
                 "SELECT COUNT(*) FROM raw.landuse",
                 "osmnx download → ogr2ogr → PostGIS"),
                ("dem",        "raw",       "raster",  "USGS 3DEP 1/3 arc-sec",    "2023-01-01", 10,
                 "SELECT COUNT(*) FROM raw.dem",
                 "gdalwarp UTM → gdal_fillnodata → raster2pgsql"),
                ("ndsm",       "raw",       "raster",  "PDAL+USGS LiDAR",          "2023-01-01", 1,
                 "SELECT COUNT(*) FROM raw.ndsm",
                 "PDAL SMRF → gdal_calc DSM-DTM → raster2pgsql"),
            ]

            for (name, schema, dtype, source, src_date, res, count_sql, lineage) in layers:
                try:
                    cur.execute(count_sql)
                    count = cur.fetchone()[0]
                    cur.execute("""
                        INSERT INTO analysis.layer_metadata
                            (layer_name, schema_name, data_type, source, source_date, resolution_m, feature_count, lineage)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
                    """, (name, schema, dtype, source, src_date, res, count, lineage))
                    print(f"  Registered: {schema}.{name} ({count:,} records)")
                except Exception as e:
                    print(f"  WARN: {schema}.{name} — {e}")

        conn.commit()


def print_summary(crs_fail, geom_issues, raster_issues):
    print("\n" + "=" * 60)
    print("QC SUMMARY")
    print("=" * 60)
    print(f"  CRS mismatches:         {crs_fail}")
    print(f"  Geometry issues:        {geom_issues}")
    print(f"  Raster range warnings:  {raster_issues}")
    total = crs_fail + geom_issues + raster_issues
    if total == 0:
        print("  STATUS: ALL CHECKS PASSED")
    else:
        print(f"  STATUS: {total} issue(s) found — review above")


def main():
    print("=" * 60)
    print("Geospatial Pipeline — Step 07: Quality Control")
    print("=" * 60)

    crs_fail    = check_crs_consistency()
    geom_issues = check_geometry_validity()
    raster_iss  = check_raster_statistics()
    check_attribute_completeness()
    populate_metadata()
    print_summary(crs_fail, geom_issues, raster_iss)
    print("\nStep 07 complete. Proceed to Step 08 (Visualization).")


if __name__ == "__main__":
    main()
