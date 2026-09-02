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

PROC_DIR = r"C:\RMTPROJECTS\dataengineering\geospatial-data-pipeline\data\processed"
EXPECTED_SRID = 32613
DENVER_ELEV_RANGE = (1500, 2200)  # meters


def get_connection():
    return psycopg2.connect(**DB)


def check_crs_consistency():
    """Verify all geometry columns use EPSG:32613."""
    print("\n[1] CRS Consistency")  # section header for console output

    # open a DB connection; "with" auto-commits/rolls back and ends the
    # transaction when the block exits (does not close the socket itself)
    with get_connection() as conn:
        # open a cursor scoped to this block; auto-closed on exit
        with conn.cursor() as cur:
            # geometry_columns is a PostGIS system view (OGC Simple Features
            # spec) that PostGIS maintains automatically as a registry of
            # every geometry column in the database. Relevant fields:
            #   f_table_schema   - schema the table lives in (raw/processed/analysis)
            #   f_table_name     - the table name (e.g. buildings, roads)
            #   f_geometry_column- name of the geometry-typed column (e.g. "geom")
            #   srid             - the coordinate system that column is stored in
            # It's populated automatically whenever a geometry column is
            # created (e.g. CREATE TABLE ... geom geometry(Polygon, 32613)),
            # so this query works generically without hardcoding table names.
            cur.execute("""
                SELECT f_table_schema, f_table_name, f_geometry_column, srid
                FROM geometry_columns
                WHERE f_table_schema IN ('raw','processed','analysis')
                ORDER BY f_table_schema, f_table_name
            """)
            # filtered to our three schemas and sorted for stable, readable output
            rows = cur.fetchall()  # pull all matching rows into memory as tuples

    failures = 0  # running count of CRS mismatches, returned at the end
    for schema, table, col, srid in rows:  # unpack each row's 4 columns
        # compare this column's SRID to the pipeline's expected SRID (32613)
        status = "OK" if srid == EXPECTED_SRID else "FAIL"
        if status == "FAIL":
            failures += 1  # tally the mismatch
        # print one line per geometry column with its pass/fail verdict
        print(f"  [{status}] {schema}.{table}.{col} — SRID={srid}")

    # NOTE: opens a *second* connection/cursor directly, without a "with"
    # block — unlike above, this one is never explicitly closed/committed.
    # Works fine for a short-lived script, but is inconsistent with the
    # cleaner pattern used earlier in this same function.
    cur_r = get_connection().cursor()

    # raster_columns is the raster equivalent of geometry_columns; PostGIS
    # rasters (DEM, nDSM) only live in the 'raw' schema, so we scope to it
    cur_r.execute("SELECT r_table_schema, r_table_name, srid FROM raster_columns WHERE r_table_schema='raw'")

    for schema, table, srid in cur_r.fetchall():  # fetch + iterate raster rows
        # same OK/FAIL comparison as for geometry columns above
        status = "OK" if srid == EXPECTED_SRID else "FAIL"
        if status == "FAIL":
            failures += 1  # adds to the SAME shared failures counter
        # "(raster)" label distinguishes these lines from geometry columns
        print(f"  [{status}] {schema}.{table} (raster) — SRID={srid}")

    print(f"  → CRS failures: {failures}")  # print grand total (geom + raster)
    return failures  # caller can use this count to decide pass/fail / exit code


def check_geometry_validity():
    """Count invalid and self-intersecting geometries."""
    print("\n[2] Geometry Validity")  # section header for console output

    # each tuple = (table to check, SQL WHERE condition flagging bad geoms, human label)
    # ST_IsValid: geometry follows OGC validity rules (no self-intersections,
    #   no ring self-touching, correct ring orientation, etc.) — used for polygons
    # ST_IsSimple: geometry doesn't cross/touch itself — used here for roads
    #   (a LINESTRING can be "valid" but still self-intersect, so a different
    #   check applies to lines than to polygons)
    checks = [
        ("raw.buildings",     "NOT ST_IsValid(geom)", "Invalid geometries"),
        ("raw.roads",         "NOT ST_IsSimple(geom)", "Self-intersecting roads"),
        ("raw.landuse",       "NOT ST_IsValid(geom)", "Invalid land use polys"),
        ("processed.buildings", "NOT ST_IsValid(geom)", "Invalid proc. buildings"),
    ]
    total_issues = 0  # running total of bad geometries across all tables

    with get_connection() as conn:  # open connection; auto-commit/close on exit
        with conn.cursor() as cur:  # open cursor scoped to this block
            for table, condition, label in checks:  # unpack each check tuple
                try:
                    # count rows where the geometry fails the validity condition
                    # NOTE: table/condition are f-string interpolated (not
                    # parameterized) — safe here only because they come from
                    # the hardcoded `checks` list above, never user input
                    cur.execute(f"SELECT COUNT(*) FROM {table} WHERE {condition}")
                    invalid = cur.fetchone()[0]  # fetchone() returns a single row tuple; [0] grabs the count value

                    # count all rows in the table, to compute a percentage
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    total = cur.fetchone()[0]

                    # avoid ZeroDivisionError if the table happens to be empty
                    pct = invalid / total * 100 if total else 0

                    # flag anything over 1% invalid as a warning
                    status = "OK" if pct < 1 else "WARN"
                    print(f"  [{status}] {label}: {invalid}/{total} ({pct:.2f}%)")
                    total_issues += invalid  # add this table's invalid count to the running total
                except Exception as e:
                    # catches cases like the table not existing yet, so one
                    # missing table doesn't crash the whole QC run
                    print(f"  [ERROR] {table}: {e}")

    print(f"  → Total validity issues: {total_issues}")  # grand total across all checks
    return total_issues  # caller can use this to gate pass/fail


def check_raster_statistics():
    """Validate raster value ranges for DEM and nDSM."""
    print("\n[3] Raster Statistics")  # section header for console output

    # each tuple = (file path, human label, (expected_min, expected_max) value range)
    # DENVER_ELEV_RANGE (module-level, line 32) = (1500, 2200) meters —
    # sanity bounds for Denver's actual elevation; nDSM (normalized surface
    # model = height above ground) and slope have their own physically
    # sensible ranges (0-250m tall, 0-90 degrees)
    rasters = [
        (f"{PROC_DIR}/dem_filled.tif", "DEM (filled)", DENVER_ELEV_RANGE),
        (f"{PROC_DIR}/ndsm.tif", "nDSM", (0, 250)),
        (f"{PROC_DIR}/slope.tif", "Slope", (0, 90)),
    ]
    issues = 0  # running count of rasters whose values fall outside expected range

    for path, label, (expected_min, expected_max) in rasters:
        # nested tuple unpacking: pulls expected_min/expected_max straight
        # out of the (expected_min, expected_max) pair in each raster tuple

        if not os.path.exists(path):
            # raster may not have been generated yet (e.g. step not run) —
            # skip rather than error, since this isn't a data-quality issue
            print(f"  [SKIP] {label} — file not found: {path}")
            continue

        with rasterio.open(path) as src:  # open the GeoTIFF; auto-closed on exit
            # read band 1 as a numpy masked array — "masked=True" means
            # nodata pixels are excluded from min/max/mean instead of
            # skewing the stats with placeholder values (e.g. -9999)
            data = src.read(1, masked=True)
            rmin, rmax, rmean = float(data.min()), float(data.max()), float(data.mean())
            # data.mask is True where a pixel is nodata; summing it counts
            # nodata pixels, divided by total pixel count for a percentage
            nodata_pct = data.mask.sum() / data.size * 100

        # value is acceptable if it's not below the expected minimum, and
        # not more than 20% above the expected maximum (some tolerance
        # since real-world data can slightly exceed a theoretical bound)
        ok_range = expected_min <= rmin and rmax <= expected_max * 1.2
        status = "OK" if ok_range else "WARN"
        if not ok_range:
            issues += 1  # tally out-of-range rasters
        print(f"  [{status}] {label}: min={rmin:.1f} max={rmax:.1f} mean={rmean:.1f}  nodata={nodata_pct:.1f}%")
        print(f"         Expected range: {expected_min}–{expected_max}")  # shows what was checked against

    return issues  # caller can use this to gate pass/fail


def check_attribute_completeness():
    """Check null rates for key attributes."""
    print("\n[4] Attribute Completeness")  # section header for console output
    with get_connection() as conn:  # open connection; auto-commit/close on exit
        with conn.cursor() as cur:  # open cursor scoped to this block
            # Buildings height completeness
            cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(height_m) AS has_height,
                    COUNT(name) AS has_name,
                    ROUND(COUNT(height_m) * 100.0 / COUNT(*), 1) AS height_pct
                FROM processed.buildings
            """)
            # NOTE: COUNT(col) only counts non-NULL values (unlike COUNT(*),
            # which counts every row) — that's what makes this a completeness
            # check: total rows vs. rows where height_m/name are actually filled in
            row = cur.fetchone()  # single row of 4 aggregate values, or None if the table is empty
            if row:
                # row[0]=total, row[1]=has_height, row[2]=has_name, row[3]=height_pct
                print(f"  Buildings: {row[0]:,} total | height: {row[1]:,} ({row[3]}%) | name: {row[2]:,}")

            # Road type completeness
            cur.execute("""
                SELECT COUNT(*) AS total, COUNT(highway) AS has_type
                FROM raw.roads
            """)
            row = cur.fetchone()
            if row:
                # percentage computed inline here instead of in SQL (unlike the
                # buildings query above, which used ROUND(...) in the SELECT)
                print(f"  Roads: {row[0]:,} total | highway type: {row[1]:,} ({row[1]/row[0]*100:.1f}%)")

            # Land use completeness
            cur.execute("SELECT COUNT(*), COUNT(landuse) FROM raw.landuse")
            row = cur.fetchone()
            if row:
                print(f"  Land use: {row[0]:,} total | tagged: {row[1]:,} ({row[1]/row[0]*100:.1f}%)")
            # NOTE: this function prints results but never returns a count or
            # status — unlike the other check_* functions, it isn't factored
            # into print_summary()'s pass/fail total (see main() below)


def populate_metadata():
    """Insert layer metadata records."""
    print("\n[5] Populating Metadata Registry")  # section header for console output
    with get_connection() as conn:  # open connection; commit happens explicitly below
        with conn.cursor() as cur:  # open cursor scoped to this block
            # create the metadata registry table the first time this runs;
            # "IF NOT EXISTS" makes this safe to re-run on every QC pass
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

            # static catalog describing every raw layer in the pipeline:
            # (layer_name, schema, data_type, source, source_date, resolution_m,
            #  count_sql, lineage) — "lineage" is a human-readable description
            # of the processing steps that produced the layer (documentation,
            # not something the code executes)
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
                    # run this layer's row-count query to get a live feature_count
                    cur.execute(count_sql)
                    count = cur.fetchone()[0]

                    # insert (or skip, via ON CONFLICT DO NOTHING) a metadata row.
                    # %s placeholders are parameterized — psycopg2 escapes these
                    # values safely, unlike the f-string SQL used elsewhere in
                    # this file, since these values could include arbitrary text
                    cur.execute("""
                        INSERT INTO analysis.layer_metadata
                            (layer_name, schema_name, data_type, source, source_date, resolution_m, feature_count, lineage)
                        VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
                        ON CONFLICT DO NOTHING
                    """, (name, schema, dtype, source, src_date, res, count, lineage))
                    print(f"  Registered: {schema}.{name} ({count:,} records)")
                except Exception as e:
                    # if a source table doesn't exist yet, log and continue
                    # rather than aborting metadata registration for every layer
                    print(f"  WARN: {schema}.{name} — {e}")

        conn.commit()  # persist all inserts (table creation + metadata rows) to the database


def print_summary(crs_fail, geom_issues, raster_issues):
    # takes the three numeric results returned by earlier checks and prints
    # a final rollup; note attribute completeness and metadata population
    # aren't included since check_attribute_completeness() and
    # populate_metadata() don't return a count (see main() below)
    print("\n" + "=" * 60)
    print("QC SUMMARY")
    print("=" * 60)
    print(f"  CRS mismatches:         {crs_fail}")
    print(f"  Geometry issues:        {geom_issues}")
    print(f"  Raster range warnings:  {raster_issues}")
    total = crs_fail + geom_issues + raster_issues  # combined issue count across all three checks
    if total == 0:
        print("  STATUS: ALL CHECKS PASSED")
    else:
        print(f"  STATUS: {total} issue(s) found — review above")


def main():
    print("=" * 60)
    print("Geospatial Pipeline — Step 07: Quality Control")
    print("=" * 60)

    # run each check in turn; the three that return a count are captured
    # into variables so print_summary() can total them up at the end
    crs_fail    = check_crs_consistency()
    geom_issues = check_geometry_validity()
    raster_iss  = check_raster_statistics()
    check_attribute_completeness()  # prints its own results; return value unused
    populate_metadata()             # side-effecting only — writes to analysis.layer_metadata
    print_summary(crs_fail, geom_issues, raster_iss)
    print("\nStep 07 complete. Proceed to Step 08 (Visualization).")


if __name__ == "__main__":
    # only runs main() when this file is executed directly
    # (e.g. `python scripts/07_qc_validation.py`), not when imported elsewhere
    main()
