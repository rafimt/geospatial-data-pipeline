"""
Step 06: Spatial Analysis & Classification in PostGIS.

Runs overlay, raster extraction, clustering, and accessibility queries
against the schemas loaded in Step 05.

Usage:
    conda activate geospatial
    python scripts/06_spatial_analysis.py
"""

import psycopg2

DB = {
    "host": "127.0.0.1",
    "port": 5433,
    "dbname": "geospatial",
    "user": "geouser",
    "password": "geopass",
}


def get_connection():
    return psycopg2.connect(**DB)


def run_sql(conn, sql, description=""):
    with conn.cursor() as cur:
        cur.execute(sql)
    conn.commit()
    if description:
        print(f"  OK: {description}")


def fetch_sql(conn, sql):
    with conn.cursor() as cur:
        cur.execute(sql)
        return cur.fetchall(), [d[0] for d in cur.description]


def print_table(rows, headers):
    if not rows:
        print("  (no rows)")
        return
    widths = [max(len(str(h)), max(len(str(r[i])) for r in rows)) for i, h in enumerate(headers)]
    fmt = "  " + "  ".join(f"{{:<{w}}}" for w in widths)
    print(fmt.format(*headers))
    print("  " + "-" * (sum(widths) + 2 * len(widths)))
    for row in rows:
        print(fmt.format(*[str(v) if v is not None else "NULL" for v in row]))


# ── 1. Buffer Analysis ────────────────────────────────────────────────────────

def analysis_road_influence(conn):
    print("\n[1] Buffer Analysis — Road Influence Zones")
    run_sql(conn, "DROP TABLE IF EXISTS analysis.road_influence;")
    run_sql(conn, """
        CREATE TABLE analysis.road_influence AS
        SELECT
            highway,
            ST_SetSRID(ST_Union(ST_Buffer(geom, 50)), 32613) AS geom
        FROM raw.roads
        WHERE highway IN ('motorway','trunk','primary','secondary','tertiary')
        GROUP BY highway;

        CREATE INDEX road_influence_geom_idx ON analysis.road_influence USING GIST(geom);
    """, "analysis.road_influence created")

    rows, cols = fetch_sql(conn, """
        SELECT highway, ROUND(ST_Area(geom) / 1e6, 3) AS area_km2
        FROM analysis.road_influence
        ORDER BY area_km2 DESC;
    """)
    print_table(rows, cols)


# ── 2. Overlay Analysis ───────────────────────────────────────────────────────

def analysis_buildings_near_roads(conn):
    print("\n[2] Overlay Analysis — Buildings Within Road Influence")
    run_sql(conn, "DROP TABLE IF EXISTS analysis.buildings_near_roads;")
    run_sql(conn, """
        CREATE TABLE analysis.buildings_near_roads AS
        SELECT b.id, b.height_m, b.area_m2, ST_SetSRID(b.geom, 32613) AS geom
        FROM processed.buildings b
        JOIN analysis.road_influence ri ON ST_Within(b.geom, ri.geom);
    """, "analysis.buildings_near_roads created")

    rows, cols = fetch_sql(conn, """
        SELECT
            COUNT(*) AS buildings_near_roads,
            ROUND(AVG(height_m)::numeric, 1) AS avg_height,
            ROUND(SUM(area_m2)::numeric, 1) AS total_footprint_m2
        FROM analysis.buildings_near_roads;
    """)
    print_table(rows, cols)


# ── 3. Elevation Extraction ───────────────────────────────────────────────────

def analysis_buildings_with_elevation(conn):
    print("\n[3] Raster Extraction — Building Elevations from DEM")
    run_sql(conn, "DROP TABLE IF EXISTS analysis.buildings_with_elevation;")
    run_sql(conn, """
        CREATE TABLE analysis.buildings_with_elevation AS
        SELECT
            b.id,
            ST_SetSRID(b.geom, 32613) AS geom,
            b.height_m,
            ST_Value(d.rast, ST_Centroid(b.geom)) AS ground_elevation_m
        FROM processed.buildings b
        JOIN raw.dem d ON ST_Intersects(d.rast, ST_Centroid(b.geom));
    """, "analysis.buildings_with_elevation created")

    rows, cols = fetch_sql(conn, """
        SELECT id, ground_elevation_m, height_m,
               (ground_elevation_m + height_m) AS top_elevation_m
        FROM analysis.buildings_with_elevation
        WHERE height_m IS NOT NULL
        ORDER BY top_elevation_m DESC
        LIMIT 10;
    """)
    print_table(rows, cols)


# ── 4. nDSM LiDAR Height ──────────────────────────────────────────────────────

def analysis_buildings_lidar_height(conn):
    print("\n[4] nDSM Height Analysis — LiDAR-derived Building Heights")
    run_sql(conn, "DROP TABLE IF EXISTS analysis.buildings_lidar_height;")
    run_sql(conn, """
        CREATE TABLE analysis.buildings_lidar_height AS
        SELECT
            b.id,
            ST_SetSRID(b.geom, 32613) AS geom,
            (ST_SummaryStats(ST_Clip(n.rast, b.geom))).mean AS lidar_height_m,
            (ST_SummaryStats(ST_Clip(n.rast, b.geom))).max  AS lidar_max_m
        FROM processed.buildings b
        JOIN raw.ndsm n ON ST_Intersects(n.rast, b.geom)
        WHERE ST_Area(b.geom) > 50;
    """, "analysis.buildings_lidar_height created")

    rows, cols = fetch_sql(conn, """
        SELECT b.id,
               ROUND(b.height_m::numeric, 1) AS osm_height,
               ROUND(lh.lidar_height_m::numeric, 1) AS lidar_height_m,
               ROUND(ABS(b.height_m - lh.lidar_height_m)::numeric, 1) AS discrepancy_m
        FROM processed.buildings b
        JOIN analysis.buildings_lidar_height lh USING (id)
        WHERE b.height_m IS NOT NULL
        ORDER BY discrepancy_m DESC
        LIMIT 10;
    """)
    print_table(rows, cols)


# ── 5. NLCD Land Cover Classification ────────────────────────────────────────

def analysis_landuse_nlcd(conn):
    print("\n[5] NLCD Land Cover Classification")
    run_sql(conn, "DROP TABLE IF EXISTS analysis.landuse_nlcd;")
    run_sql(conn, """
        CREATE TABLE analysis.landuse_nlcd AS
        WITH nlcd_extract AS (
            SELECT
                lu.id AS lu_id,
                lu.landuse AS osm_landuse,
                (ST_ValueCount(ST_Clip(n.rast, 1, lu.geom, true))).value AS nlcd_code,
                (ST_ValueCount(ST_Clip(n.rast, 1, lu.geom, true))).count AS pixel_count
            FROM raw.landuse lu
            JOIN raw.nlcd n ON ST_Intersects(n.rast, lu.geom)
            WHERE ST_Area(lu.geom) > 1000
        )
        SELECT
            lu_id, osm_landuse, nlcd_code, pixel_count,
            CASE nlcd_code
                WHEN 11 THEN 'Open Water'
                WHEN 21 THEN 'Developed Open'
                WHEN 22 THEN 'Developed Low'
                WHEN 23 THEN 'Developed Medium'
                WHEN 24 THEN 'Developed High'
                WHEN 31 THEN 'Barren Land'
                WHEN 41 THEN 'Deciduous Forest'
                WHEN 42 THEN 'Evergreen Forest'
                WHEN 52 THEN 'Shrub/Scrub'
                WHEN 71 THEN 'Grassland'
                WHEN 81 THEN 'Hay/Pasture'
                WHEN 82 THEN 'Cultivated Crops'
                WHEN 90 THEN 'Woody Wetlands'
                ELSE 'Other'
            END AS nlcd_class
        FROM nlcd_extract
        ORDER BY lu_id, pixel_count DESC;
    """, "analysis.landuse_nlcd created")

    rows, cols = fetch_sql(conn, """
        SELECT osm_landuse, nlcd_class, SUM(pixel_count) AS total_pixels
        FROM analysis.landuse_nlcd
        GROUP BY osm_landuse, nlcd_class
        ORDER BY total_pixels DESC
        LIMIT 10;
    """)
    print_table(rows, cols)


# ── 6. Spatial Clustering ─────────────────────────────────────────────────────

def analysis_building_clusters(conn):
    print("\n[6] Spatial Clustering — Urban Density (DBSCAN)")
    run_sql(conn, "DROP TABLE IF EXISTS analysis.building_clusters;")
    run_sql(conn, """
        CREATE TABLE analysis.building_clusters AS
        SELECT id, ST_SetSRID(geom, 32613) AS geom,
               ST_ClusterDBSCAN(geom, 100, 5) OVER () AS cluster_id
        FROM processed.buildings;
    """, "analysis.building_clusters created")

    rows, cols = fetch_sql(conn, """
        SELECT
            cluster_id,
            COUNT(*) AS building_count,
            ROUND(SUM(area_m2)::numeric, 0) AS total_footprint_m2
        FROM analysis.building_clusters
        JOIN processed.buildings USING (id)
        WHERE cluster_id IS NOT NULL
        GROUP BY cluster_id
        ORDER BY building_count DESC
        LIMIT 10;
    """)
    print_table(rows, cols)


# ── 7. Accessibility Analysis ─────────────────────────────────────────────────

def analysis_buildings_road_access(conn):
    print("\n[7] Accessibility Analysis — Distance to Nearest Road")
    run_sql(conn, "DROP TABLE IF EXISTS analysis.buildings_road_access;")
    run_sql(conn, """
        CREATE TABLE analysis.buildings_road_access AS
        SELECT
            b.id,
            ST_SetSRID(b.geom, 32613) AS geom,
            MIN(ST_Distance(b.geom, r.geom)) AS dist_to_nearest_road_m
        FROM processed.buildings b
        CROSS JOIN LATERAL (
            SELECT geom FROM raw.roads
            ORDER BY b.geom <-> geom
            LIMIT 1
        ) r
        GROUP BY b.id, b.geom;
    """, "analysis.buildings_road_access created")

    rows, cols = fetch_sql(conn, """
        SELECT
            CASE
                WHEN dist_to_nearest_road_m < 10  THEN '< 10m'
                WHEN dist_to_nearest_road_m < 50  THEN '10-50m'
                WHEN dist_to_nearest_road_m < 100 THEN '50-100m'
                ELSE '> 100m'
            END AS distance_category,
            COUNT(*) AS building_count
        FROM analysis.buildings_road_access
        GROUP BY 1
        ORDER BY 1;
    """)
    print_table(rows, cols)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Geospatial Pipeline — Step 06: Spatial Analysis")
    print("=" * 60)

    conn = get_connection()
    try:
        steps = [
            analysis_road_influence,
            analysis_buildings_near_roads,
            analysis_buildings_with_elevation,
            analysis_buildings_lidar_height,
            analysis_landuse_nlcd,
            analysis_building_clusters,
            analysis_buildings_road_access,
        ]
        for step in steps:
            try:
                step(conn)
            except Exception as e:
                conn.rollback()
                print(f"  SKIP — {e}")
    finally:
        conn.close()

    print("\nStep 06 complete. Proceed to Step 07 (QC Validation).")


if __name__ == "__main__":
    main()
