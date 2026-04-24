# End-to-End Geospatial Data Pipeline

This project started as a hands-on way to learn **PostGIS** and **Docker** — setting up a spatial database, loading real-world data into it, and running spatial queries manually. As the project grew, each step was automated with Python scripts, turning the manual workflow into a reproducible pipeline. The study area is **Denver, Colorado**, using freely available datasets from USGS, OpenStreetMap, and Google Earth Engine.

---

## Study Area

**Denver, Colorado, USA** — bounding box: `-105.05, 39.60, -104.85, 39.80` (WGS84)

---

## Scripts

### `01_download_data.py`
Downloads all raw data for the study area. Fetches a 10m DEM from the USGS National Map API, a 30m SRTM DEM from OpenTopography, roads and buildings from OpenStreetMap via `osmnx`, and a Sentinel-2 surface reflectance composite from Google Earth Engine using `geemap`.

### `02_lidar_processing.py`
Processes a LiDAR point cloud (`.laz`) using PDAL pipelines. Runs ground classification, filters noise, and produces a Digital Terrain Model (DTM) and Digital Surface Model (DSM) as GeoTIFFs.

### `03_dem_processing.py`
Takes the raw DEM and LiDAR-derived rasters through GDAL processing. Reprojects to a local CRS, generates a hillshade, slope, and normalized DSM (nDSM = DSM − DTM) which represents above-ground heights like buildings and trees.

### `04_vector_extraction.py`
Cleans and prepares OSM vector data. Reprojects roads and building footprints, fixes invalid geometries, and exports them as GeoJSON ready for database loading.

### `05_load_postgis.py`
Loads all processed data into a PostGIS database running in Docker. Creates schemas, loads vector layers with `geopandas`, and uses `raster2pgsql` to import raster tiles.

### `06_spatial_analysis.py` / `06_spatial_analysis.sql`
Runs spatial analysis queries inside PostGIS. Generates 50m road influence buffers, extracts LiDAR height values for each building using raster sampling, clusters buildings with `ST_ClusterDBSCAN`, and scores parcels for land suitability based on road proximity and land use.

### `07_qc_validation.py`
Validates the processed data. Checks for geometry errors, null values, CRS mismatches, and out-of-range raster values. Prints a summary report of any issues found.

### `08_visualization.py`
Exports PostGIS layers to GeoJSON and builds an interactive Folium map showing buildings colored by LiDAR height, road influence zones, and land suitability scores. Also generates a side-by-side raster preview (hillshade, slope, nDSM) using Matplotlib.

---

## Notebook

### `notebooks/visualization.ipynb`
Interactive visualization using Google Earth Engine and Matplotlib. Shows a Sentinel-2 true color composite, NDVI distribution, monthly NDVI time series, and OSM building/road maps for the Denver study area.

---

## Technology Stack

| Category | Tools |
|---|---|
| Raster processing | GDAL, rasterio, numpy |
| Vector processing | OGR, geopandas, shapely, osmnx |
| Point cloud | PDAL, laspy |
| Spatial database | PostgreSQL 15 + PostGIS 3.4 |
| Satellite imagery | Google Earth Engine, geemap |
| Visualization | Folium, Matplotlib |
| Language | Python 3.11 |
| Infrastructure | Docker, Docker Compose |

---

## Data Sources

| Dataset | Source | Format |
|---|---|---|
| DEM 10m | USGS 3DEP (nationalmap.gov) | GeoTIFF |
| DEM 30m | NASA SRTM via OpenTopography | GeoTIFF |
| Satellite imagery | Sentinel-2 via Google Earth Engine | GeoTIFF |
| Roads & buildings | OpenStreetMap via osmnx | GeoJSON |
| LiDAR point cloud | OpenTopography.org | LAZ |

---

## Quick Start

```bash
# 1. Start PostGIS
docker-compose up -d

# 2. Activate environment
.venv\Scripts\activate       # Windows
source .venv/bin/activate    # Mac/Linux

# 3. Run pipeline steps in order
python scripts/01_download_data.py
python scripts/02_lidar_processing.py
python scripts/03_dem_processing.py
python scripts/04_vector_extraction.py
python scripts/05_load_postgis.py
python scripts/06_spatial_analysis.py
python scripts/07_qc_validation.py
python scripts/08_visualization.py
```

---

## Directory Structure

```
geospatial-data-pipeline/
├── scripts/
│   ├── 01_download_data.py
│   ├── 02_lidar_processing.py
│   ├── 03_dem_processing.py
│   ├── 04_vector_extraction.py
│   ├── 05_load_postgis.py
│   ├── 06_spatial_analysis.py
│   ├── 06_spatial_analysis.sql
│   ├── 07_qc_validation.py
│   └── 08_visualization.py
├── notebooks/
│   └── visualization.ipynb
├── data/
│   ├── raw/
│   ├── processed/
│   └── output/
├── docker-compose.yml
├── requirements.txt
└── environment.yml
```
