# End-to-End Geospatial Data Pipeline

**Learning project** — building a multi-source geospatial data pipeline using freely available datasets, GDAL, PostGIS, and Python.

Inspired by urban planning intelligence workflows, adapted for open-source reproducibility.

---

## Study Area

**Denver, Colorado, USA**
- Excellent USGS 3DEP LiDAR + DEM coverage
- Rich OpenStreetMap data
- NLCD land use data available
- Bounding box: `-105.05, 39.60, -104.85, 39.80` (WGS84)

---

## Pipeline Steps

| Step | File | Description |
|------|------|-------------|
| 0 | [00_environment_setup.md](00_environment_setup.md) | Docker, PostGIS, GDAL, Python env |
| 1 | [01_data_acquisition.md](01_data_acquisition.md) | Download free datasets |
| 2 | [02_lidar_point_cloud.md](02_lidar_point_cloud.md) | LiDAR processing with PDAL |
| 3 | [03_surface_modeling_dem_dsm.md](03_surface_modeling_dem_dsm.md) | DEM/DSM processing with GDAL |
| 4 | [04_vector_feature_extraction.md](04_vector_feature_extraction.md) | OSM vector data via OGR |
| 5 | [05_postgis_integration.md](05_postgis_integration.md) | Load all data into PostGIS |
| 6 | [06_spatial_analysis.md](06_spatial_analysis.md) | Spatial queries and classification |
| 7 | [07_quality_control.md](07_quality_control.md) | Validation, topology, metadata |
| 8 | [08_visualization.md](08_visualization.md) | Interactive maps and QGIS |

---

## Technology Stack

| Category | Tools |
|----------|-------|
| Raster processing | GDAL, rasterio, numpy |
| Vector processing | OGR, geopandas, shapely, osmnx |
| Point cloud | PDAL, laspy |
| Spatial database | PostgreSQL 15 + PostGIS 3.4 |
| Visualization | Folium, QGIS, Kepler.gl |
| Language | Python 3.11+ |
| Infrastructure | Docker, Docker Compose |

---

## Free Data Sources

| Dataset | Source | Format | Size |
|---------|--------|--------|------|
| DEM 1m/10m | USGS 3DEP (nationalmap.gov) | GeoTIFF | ~50MB |
| DEM 30m global | NASA SRTM / Copernicus DEM | GeoTIFF | ~5MB |
| Satellite imagery | Sentinel-2 (Copernicus Hub) | GeoTIFF | ~500MB |
| Roads & buildings | OpenStreetMap via osmnx | GeoJSON | ~20MB |
| LiDAR point cloud | OpenTopography.org | LAZ | ~1GB |
| Land use | NLCD 2021 (USGS) | GeoTIFF | ~30MB |

---

## Directory Structure

```
geospatial/
├── README.md
├── plan.md
├── 00_environment_setup.md
├── 01_data_acquisition.md
├── 02_lidar_point_cloud.md
├── 03_surface_modeling_dem_dsm.md
├── 04_vector_feature_extraction.md
├── 05_postgis_integration.md
├── 06_spatial_analysis.md
├── 07_quality_control.md
├── 08_visualization.md
├── scripts/
│   ├── 01_download_data.py
│   ├── 02_lidar_processing.py
│   ├── 03_dem_processing.py
│   ├── 04_vector_extraction.py
│   ├── 05_load_postgis.py
│   ├── 06_spatial_analysis.sql
│   └── 07_qc_validation.py
├── data/
│   ├── raw/
│   ├── processed/
│   └── output/
└── docker-compose.yml
```

---

## Quick Start

```bash
# 1. Start PostGIS
docker-compose up -d

# 2. Create Python environment
conda create -n geospatial python=3.11
conda activate geospatial
pip install gdal rasterio geopandas shapely pyproj osmnx psycopg2-binary laspy pdal folium

# 3. Follow steps 01 → 08 in order
```

---

## Learning Objectives

- Understand raster vs vector geospatial data
- Apply coordinate reference systems (CRS) and reprojection
- Use GDAL CLI tools and Python bindings
- Store and query spatial data in PostGIS
- Perform spatial analysis: buffers, overlays, interpolation
- Validate data quality for geospatial workflows
- Build interactive maps from pipeline outputs
