-- ============================================================
-- Step 06: Spatial Analysis Queries
-- Run against: geospatial database (PostGIS)
-- Connect: psql -U geouser -d geospatial -h localhost
-- ============================================================

-- ============================================================
-- 1. BUFFER ANALYSIS — Road Influence Zones
-- ============================================================

-- $env:PGPASSWORD="geopass"
-- & "C:\Program Files\PostgreSQL\17\bin\psql.exe" -h 127.0.0.1 -p 5433 -U geouser -d geospatial -f "C:\RMTPROJECTS\dataengineering\geospatial\scripts\06_spatial_analysis.sql"


DROP TABLE IF EXISTS analysis.road_influence;
CREATE TABLE analysis.road_influence AS
SELECT
    highway,
    ST_Union(ST_Buffer(geom, 50)) AS geom
FROM raw.roads
WHERE highway IN ('motorway','trunk','primary','secondary','tertiary')
GROUP BY highway;

CREATE INDEX road_influence_geom_idx ON analysis.road_influence USING GIST(geom);

-- Summary
SELECT highway, ROUND((ST_Area(geom) / 1e6)::numeric, 3) AS area_km2
FROM analysis.road_influence
ORDER BY area_km2 DESC;


-- ============================================================
-- 2. OVERLAY — Buildings Within Road Influence
-- ============================================================

DROP TABLE IF EXISTS analysis.buildings_near_roads;
CREATE TABLE analysis.buildings_near_roads AS
SELECT b.id, b.height_m, b.area_m2, b.geom
FROM processed.buildings b
JOIN analysis.road_influence ri ON ST_Within(b.geom, ri.geom);

CREATE INDEX bldg_near_roads_geom_idx ON analysis.buildings_near_roads USING GIST(geom);

SELECT
    COUNT(*)                         AS buildings_near_roads,
    ROUND(AVG(height_m)::numeric, 1) AS avg_height_m,
    ROUND(SUM(area_m2)::numeric)     AS total_footprint_m2
FROM analysis.buildings_near_roads;


-- ============================================================
-- 3. RASTER VALUE EXTRACTION — Elevation at Building Centroids
-- ============================================================

DROP TABLE IF EXISTS analysis.buildings_with_elevation;
CREATE TABLE analysis.buildings_with_elevation AS
SELECT DISTINCT ON (b.id)
    b.id,
    b.height_m,
    b.area_m2,
    b.geom,
    ST_Value(d.rast, ST_Centroid(b.geom)) AS ground_elevation_m
FROM processed.buildings b
JOIN raw.dem d ON ST_Intersects(d.rast, ST_Centroid(b.geom))
ORDER BY b.id;

CREATE INDEX bldg_elev_geom_idx ON analysis.buildings_with_elevation USING GIST(geom);

-- Denver check: elevations should be ~1600-1800m
SELECT
    ROUND(MIN(ground_elevation_m)::numeric, 1) AS elev_min,
    ROUND(MAX(ground_elevation_m)::numeric, 1) AS elev_max,
    ROUND(AVG(ground_elevation_m)::numeric, 1) AS elev_mean
FROM analysis.buildings_with_elevation;

-- Top 10 tallest building tops (ground elevation + OSM height)
SELECT
    id,
    ROUND(ground_elevation_m::numeric, 1) AS ground_m,
    ROUND(height_m::numeric, 1)           AS osm_height_m,
    ROUND((ground_elevation_m + height_m)::numeric, 1) AS top_elevation_m
FROM analysis.buildings_with_elevation
WHERE height_m IS NOT NULL
ORDER BY top_elevation_m DESC
LIMIT 10;


-- ============================================================
-- 4. LIDAR HEIGHT ANALYSIS — nDSM Clipped per Building
-- ============================================================

DROP TABLE IF EXISTS analysis.buildings_lidar_height;
CREATE TABLE analysis.buildings_lidar_height AS
SELECT DISTINCT ON (b.id)
    b.id,
    b.geom,
    (ST_SummaryStats(ST_Clip(n.rast, b.geom))).mean  AS lidar_height_m,
    (ST_SummaryStats(ST_Clip(n.rast, b.geom))).max   AS lidar_max_m,
    (ST_SummaryStats(ST_Clip(n.rast, b.geom))).count AS pixel_count
FROM processed.buildings b
JOIN raw.ndsm n ON ST_Intersects(n.rast, b.geom)
WHERE ST_Area(b.geom) > 50
ORDER BY b.id;

CREATE INDEX bldg_lidar_geom_idx ON analysis.buildings_lidar_height USING GIST(geom);

-- Height distribution
SELECT
    CASE
        WHEN lidar_height_m < 3   THEN '< 3m (single storey)'
        WHEN lidar_height_m < 10  THEN '3–10m (low rise)'
        WHEN lidar_height_m < 30  THEN '10–30m (mid rise)'
        WHEN lidar_height_m < 60  THEN '30–60m (high rise)'
        ELSE '> 60m (skyscraper)'
    END AS height_category,
    COUNT(*) AS building_count
FROM analysis.buildings_lidar_height
GROUP BY 1
ORDER BY MIN(lidar_height_m);

-- OSM vs LiDAR discrepancy (where both exist)
SELECT
    b.id,
    ROUND(b.height_m::numeric, 1)        AS osm_height_m,
    ROUND(lh.lidar_height_m::numeric, 1) AS lidar_height_m,
    ROUND(ABS(b.height_m - lh.lidar_height_m)::numeric, 1) AS discrepancy_m
FROM processed.buildings b
JOIN analysis.buildings_lidar_height lh USING (id)
WHERE b.height_m IS NOT NULL AND lh.lidar_height_m IS NOT NULL
ORDER BY discrepancy_m DESC
LIMIT 20;


-- ============================================================
-- 5. NLCD LAND COVER CLASSIFICATION
-- ============================================================

DROP TABLE IF EXISTS analysis.landuse_nlcd;
CREATE TABLE analysis.landuse_nlcd AS
WITH nlcd_extract AS (
    SELECT
        lu.id          AS lu_id,
        lu.landuse     AS osm_landuse,
        lu.area_m2,
        (ST_ValueCount(ST_Clip(n.rast, 1, lu.geom, true))).value  AS nlcd_code,
        (ST_ValueCount(ST_Clip(n.rast, 1, lu.geom, true))).count  AS pixel_count
    FROM raw.landuse lu
    JOIN raw.nlcd n ON ST_Intersects(n.rast, lu.geom)
    WHERE ST_Area(lu.geom) > 1000
)
SELECT
    lu_id,
    osm_landuse,
    area_m2,
    nlcd_code,
    pixel_count,
    ROUND((pixel_count * 900.0 / SUM(pixel_count) OVER (PARTITION BY lu_id))::numeric, 1) AS coverage_pct,
    CASE nlcd_code
        WHEN 11 THEN 'Open Water'
        WHEN 21 THEN 'Developed - Open'
        WHEN 22 THEN 'Developed - Low'
        WHEN 23 THEN 'Developed - Medium'
        WHEN 24 THEN 'Developed - High'
        WHEN 31 THEN 'Barren Land'
        WHEN 41 THEN 'Deciduous Forest'
        WHEN 42 THEN 'Evergreen Forest'
        WHEN 43 THEN 'Mixed Forest'
        WHEN 52 THEN 'Shrub/Scrub'
        WHEN 71 THEN 'Grassland/Herbaceous'
        WHEN 81 THEN 'Hay/Pasture'
        WHEN 82 THEN 'Cultivated Crops'
        WHEN 90 THEN 'Woody Wetlands'
        ELSE 'Other (' || nlcd_code::text || ')'
    END AS nlcd_class
FROM nlcd_extract
ORDER BY lu_id, pixel_count DESC;

-- NLCD class area totals across study area
SELECT
    nlcd_class,
    COUNT(DISTINCT lu_id) AS polygon_count,
    ROUND((SUM(pixel_count * 900.0) / 1e6)::numeric, 2) AS total_area_km2
FROM analysis.landuse_nlcd
GROUP BY nlcd_class
ORDER BY total_area_km2 DESC;


-- ============================================================
-- 6. BUILDING CLUSTERING (DBSCAN)
-- ============================================================

DROP TABLE IF EXISTS analysis.building_clusters;
CREATE TABLE analysis.building_clusters AS
SELECT
    id,
    geom,
    ST_ClusterDBSCAN(geom, eps := 100, minpoints := 5) OVER () AS cluster_id
FROM processed.buildings;

CREATE INDEX bldg_cluster_geom_idx ON analysis.building_clusters USING GIST(geom);

-- Top clusters by building count
SELECT
    cluster_id,
    COUNT(*)                                       AS building_count,
    ROUND(SUM(b.area_m2)::numeric)                 AS total_footprint_m2,
    ST_AsText(ST_Centroid(ST_Union(bc.geom)))      AS centroid_wkt
FROM analysis.building_clusters bc
JOIN processed.buildings b USING (id)
WHERE cluster_id IS NOT NULL
GROUP BY cluster_id
ORDER BY building_count DESC
LIMIT 10;

-- Percentage of buildings in clusters
SELECT
    COUNT(*) FILTER (WHERE cluster_id IS NOT NULL) AS in_cluster,
    COUNT(*) AS total,
    ROUND(COUNT(*) FILTER (WHERE cluster_id IS NOT NULL) * 100.0 / COUNT(*), 1) AS pct_in_cluster
FROM analysis.building_clusters;


-- ============================================================
-- 7. ACCESSIBILITY — Distance to Nearest Road
-- ============================================================

DROP TABLE IF EXISTS analysis.buildings_road_access;
CREATE TABLE analysis.buildings_road_access AS
SELECT
    b.id,
    b.geom,
    (SELECT ROUND(ST_Distance(b.geom, r.geom)::numeric, 1)
     FROM raw.roads r
     ORDER BY b.geom <-> r.geom
     LIMIT 1) AS dist_to_nearest_road_m
FROM processed.buildings b;

CREATE INDEX bldg_access_geom_idx ON analysis.buildings_road_access USING GIST(geom);

-- Distribution
SELECT
    CASE
        WHEN dist_to_nearest_road_m < 10  THEN '< 10m'
        WHEN dist_to_nearest_road_m < 50  THEN '10–50m'
        WHEN dist_to_nearest_road_m < 100 THEN '50–100m'
        WHEN dist_to_nearest_road_m < 200 THEN '100–200m'
        ELSE '> 200m'
    END AS distance_band,
    COUNT(*) AS building_count
FROM analysis.buildings_road_access
GROUP BY 1
ORDER BY MIN(dist_to_nearest_road_m);


-- ============================================================
-- 8. LAND SUITABILITY SCORING
-- ============================================================

DROP TABLE IF EXISTS analysis.land_suitability;
CREATE TABLE analysis.land_suitability AS
SELECT
    b.id,
    b.geom,
    b.area_m2,
    GREATEST(0, LEAST(100,
        -- Slope penalty (flat terrain preferred)
        50 - COALESCE(
            (SELECT ST_Value(sl.rast, ST_Centroid(b.geom))
             FROM raw.slope sl
             WHERE ST_Intersects(sl.rast, ST_Centroid(b.geom))
             LIMIT 1), 0
        ) * 2
        -- Lot size bonus (up to 30 pts, scaled by area)
        + LEAST(30, b.area_m2 / 100.0)
        -- Road proximity bonus
        + CASE WHEN COALESCE(ba.dist_to_nearest_road_m, 9999) < 50 THEN 20 ELSE 0 END
    ))::int AS suitability_score
FROM processed.buildings b
LEFT JOIN analysis.buildings_road_access ba USING (id);

CREATE INDEX land_suit_geom_idx ON analysis.land_suitability USING GIST(geom);

-- Score distribution
SELECT
    (ROUND(suitability_score / 10.0) * 10)::int AS score_bucket,
    COUNT(*) AS parcel_count
FROM analysis.land_suitability
GROUP BY 1
ORDER BY 1;


-- ============================================================
-- FIX SRID REGISTRATION
-- CREATE TABLE AS does not register SRID in geometry_columns.
-- UpdateGeometrySRID writes the correct SRID so QC checks pass.
-- ============================================================

SELECT UpdateGeometrySRID('analysis', 'road_influence',          'geom', 32613);
SELECT UpdateGeometrySRID('analysis', 'buildings_near_roads',    'geom', 32613);
SELECT UpdateGeometrySRID('analysis', 'buildings_with_elevation','geom', 32613);
SELECT UpdateGeometrySRID('analysis', 'buildings_lidar_height',  'geom', 32613);
SELECT UpdateGeometrySRID('analysis', 'building_clusters',       'geom', 32613);
SELECT UpdateGeometrySRID('analysis', 'buildings_road_access',   'geom', 32613);
SELECT UpdateGeometrySRID('analysis', 'land_suitability',        'geom', 32613);
SELECT UpdateGeometrySRID('processed','buildings',               'geom', 32613);
