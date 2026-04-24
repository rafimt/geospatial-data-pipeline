"""
Step 01: Download geospatial datasets for Denver, CO study area.

Datasets:
  - OpenStreetMap (roads, buildings, land use) via osmnx
  - Sentinel-2 Level-2A Surface Reflectance via Google Earth Engine / geemap
  - USGS 3DEP 1/3 arc-second DEM via The National Map (TNM) API
  - SRTM 30m DEM via OpenTopography API

GEE setup:
  1. Register at https://earthengine.google.com (free for research)
  2. Run `earthengine authenticate` once in your terminal
  3. Optionally set GEE_PROJECT in .env if your account requires a cloud project

OpenTopography API:
  - Register at https://portal.opentopography.org to get a free API key
  - Set OPENTOPO_API_KEY in .env

Usage:
    conda activate geospatial
    python scripts/01_download_data.py
"""

import os
import json
import ee
import geemap
import osmnx as ox
import requests
from dotenv import load_dotenv

load_dotenv()

BBOX = (-105.05, 39.60, -104.85, 39.80)  # (west, south, east, north)
RAW_DIR = "data/raw"

START_DATE = "2023-06-01"
END_DATE = "2023-09-01"
MAX_CLOUD_PCT = 10
BANDS = ["B2", "B3", "B4", "B8", "B11"]
SCALE = 10  # metres (Sentinel-2 native resolution)


# ── DEM — USGS 3DEP via The National Map (TNM) API ───────────────────────────

def download_dem_usgs():
    """
    Download USGS 3DEP 1/3 arc-second (~10m) DEM for the study area.

    Equivalent curl command:
        curl "https://tnmaccess.nationalmap.gov/api/v1/products?
              bbox=-105.05,39.60,-104.85,39.80
              &datasets=National+Elevation+Dataset+%28NED%29+1%2F3+arc-second
              &prodFormats=GeoTIFF&outputFormat=JSON" \
          -o data/raw/dem_query.json
    """
    print("Downloading USGS 3DEP DEM via TNM API...")
    west, south, east, north = BBOX

    # Step 1: query the TNM catalog to get the GeoTIFF download URL
    query_url = (
        "https://tnmaccess.nationalmap.gov/api/v1/products"
        f"?bbox={west},{south},{east},{north}"
        "&datasets=National+Elevation+Dataset+%28NED%29+1%2F3+arc-second"
        "&prodFormats=GeoTIFF"
        "&outputFormat=JSON"
    )
    resp = requests.get(query_url, timeout=60)
    resp.raise_for_status()
    products = resp.json()

    # Save raw query response for reference
    query_out = f"{RAW_DIR}/dem_query.json"
    with open(query_out, "w") as f:
        json.dump(products, f, indent=2)
    print(f"  Query result saved: {query_out}")

    # Step 2: extract first GeoTIFF download URL and fetch the file
    items = products.get("items", [])
    if not items:
        print("  No products found — try broadening the bounding box.")
        return

    download_url = items[0]["downloadURL"]
    print(f"  Downloading: {download_url}")
    out = f"{RAW_DIR}/dem_usgs_10m.tif"
    with requests.get(download_url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"  Saved: {out}")


# ── DEM — SRTM 30m via OpenTopography API ────────────────────────────────────

def download_dem_srtm():
    """
    Download SRTM GL1 (~30m) DEM for the study area via OpenTopography API.
    Requires OPENTOPO_API_KEY in .env (free registration at opentopography.org).

    Equivalent curl command:
        curl "https://portal.opentopography.org/API/globaldem?
              demtype=SRTMGL1&south=39.60&north=39.80
              &west=-105.05&east=-104.85
              &outputFormat=GTiff&API_Key=YOUR_KEY" \
          -o data/raw/dem_srtm_30m.tif
    """
    print("Downloading SRTM 30m DEM via OpenTopography API...")
    api_key = os.getenv("OPENTOPO_API_KEY")
    if not api_key:
        print("  OPENTOPO_API_KEY not set in .env — skipping SRTM download.")
        return

    west, south, east, north = BBOX
    url = (
        "https://portal.opentopography.org/API/globaldem"
        f"?demtype=SRTMGL1"
        f"&south={south}&north={north}&west={west}&east={east}"
        f"&outputFormat=GTiff"
        f"&API_Key={api_key}"
    )
    out = f"{RAW_DIR}/dem_srtm_30m.tif"
    with requests.get(url, stream=True, timeout=300) as r:
        r.raise_for_status()
        with open(out, "wb") as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    print(f"  Saved: {out}")


# ── OSM ───────────────────────────────────────────────────────────────────────

def download_osm_roads():
    print("Downloading OSM roads...")
    G = ox.graph_from_bbox(BBOX, network_type="all", retain_all=False)
    _, edges = ox.graph_to_gdfs(G)
    out = f"{RAW_DIR}/osm_roads.geojson"
    edges.to_file(out, driver="GeoJSON")
    print(f"  Saved: {out} ({len(edges):,} road segments)")


def download_osm_buildings():
    print("Downloading OSM buildings...")
    buildings = ox.features_from_bbox(BBOX, tags={"building": True})
    buildings = buildings[buildings.geometry.geom_type.isin(["Polygon", "MultiPolygon"])]
    out = f"{RAW_DIR}/osm_buildings.geojson"
    buildings.to_file(out, driver="GeoJSON")
    print(f"  Saved: {out} ({len(buildings):,} buildings)")


def download_osm_landuse():
    print("Downloading OSM land use...")
    landuse = ox.features_from_bbox(BBOX, tags={"landuse": True})
    landuse = landuse[landuse.geometry.geom_type.isin(["Polygon", "MultiPolygon"])]
    out = f"{RAW_DIR}/osm_landuse.geojson"
    landuse.to_file(out, driver="GeoJSON")
    print(f"  Saved: {out} ({len(landuse):,} land use polygons)")


# ── Google Earth Engine ───────────────────────────────────────────────────────
ee.Authenticate()
ee.Initialize(project='rmtumon')

def download_sentinel2():
    print("Downloading Sentinel-2 composite via GEE...")
    west, south, east, north = BBOX
    region = ee.Geometry.BBox(west, south, east, north)

    collection = (
        ee.ImageCollection("COPERNICUS/S2_SR_HARMONIZED")
        .filterBounds(region)
        .filterDate(START_DATE, END_DATE)
        .filter(ee.Filter.lt("CLOUDY_PIXEL_PERCENTAGE", MAX_CLOUD_PCT))
    )

    count = collection.size().getInfo()
    print(f"  Found {count} scene(s) with <{MAX_CLOUD_PCT}% cloud cover")
    if count == 0:
        print("  No scenes found — try relaxing date range or cloud threshold.")
        return

    composite = collection.median().select(BANDS).clip(region)

    out_dir = f"{RAW_DIR}/sentinel2_gee"
    os.makedirs(out_dir, exist_ok=True)
    out_path = f"{out_dir}/sentinel2_sr_composite.tif"

    geemap.download_ee_image(
        image=composite,
        filename=out_path,
        region=region,
        scale=SCALE,
        crs="EPSG:4326",
    )
    print(f"  Saved: {out_path}")



# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Geospatial Pipeline — Step 01: Data Acquisition")
    print(f"Study area: Denver, CO  BBOX={BBOX}")
    print("=" * 60)

    os.makedirs(RAW_DIR, exist_ok=True)

    download_dem_usgs()       # USGS 3DEP ~10m  — TNM API (no key needed)
    download_dem_srtm()       # SRTM ~30m       — requires OPENTOPO_API_KEY in .env

    # download_osm_roads()
    # download_osm_buildings()
    # download_osm_landuse()

    # download_sentinel2()

    print("\nStep 01 complete. Proceed to Step 02.")


if __name__ == "__main__":
    main()
