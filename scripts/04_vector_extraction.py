"""
Step 04: Vector feature extraction and processing with geopandas.

Operations:
- Reproject OSM layers to UTM Zone 13N
- Compute building areas
- Create road buffers
- Spatial join: buildings × land use
- Export to GeoPackage

Usage:
    conda activate geospatial
    python scripts/04_vector_extraction.py
"""

import os                  # file existence checks and directory creation
import geopandas as gpd    # GeoDataFrame — like pandas but geometry-aware
import pandas as pd        # used for pd.to_numeric (parse OSM height strings)

# Absolute paths so the script works regardless of working directory
RAW_DIR  = r'C:\RMTPROJECTS\dataengineering\geospatial\data\raw'
PROC_DIR = r'C:\RMTPROJECTS\dataengineering\geospatial\data\processed'

# All outputs will be in this projected CRS — UTM Zone 13N, unit = metres
# Required for accurate area, distance, and buffer calculations in Colorado
TARGET_CRS = "EPSG:32613"

# Road types considered "major" for buffer analysis
# motorway=highway, trunk=near-highway, primary/secondary/tertiary=city arterials
MAJOR_ROAD_TYPES = ["motorway", "trunk", "primary", "secondary", "tertiary"]

os.makedirs(PROC_DIR, exist_ok=True)  # create processed/ folder if it doesn't exist


# ── load_and_reproject ────────────────────────────────────────────────────────
def load_and_reproject(path: str, layer: str = None, crs: str = TARGET_CRS) -> gpd.GeoDataFrame:
    """Load a vector file and reproject to target CRS."""

    # gpd.read_file reads GeoJSON, GPKG, Shapefile, etc. automatically
    # layer= is needed for GeoPackage files which can hold multiple layers
    gdf = gpd.read_file(path, layer=layer) if layer else gpd.read_file(path)

    # Some GeoJSON files are missing an explicit CRS declaration
    # OSM data is always WGS84 (lat/lon degrees), so we assign it if absent
    if gdf.crs is None:
        gdf = gdf.set_crs("EPSG:4326")

    # Only reproject if the file is not already in the target CRS
    # to_epsg() returns the integer EPSG code (e.g. 4326 or 32613)
    if gdf.crs.to_epsg() != int(crs.split(":")[1]):
        gdf = gdf.to_crs(crs)   # transform all coordinates to UTM Zone 13N

    return gdf


# ── process_buildings ─────────────────────────────────────────────────────────
def process_buildings(raw_path: str, out_path: str) -> gpd.GeoDataFrame:
    print("Processing buildings...")

    # Load and reproject the raw OSM buildings GeoJSON
    buildings = load_and_reproject(raw_path)

    # OSM sometimes returns Points or Lines alongside Polygons for buildings
    # (e.g. a building node tagged as building=yes).  Keep only solid shapes.
    buildings = buildings[buildings.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()

    # Drop geometrically invalid features (self-intersections, etc.)
    # Invalid geometries crash area calculations and spatial joins
    buildings = buildings[buildings.geometry.is_valid].copy()

    # .area returns the polygon area in CRS units — metres² because CRS is UTM
    # round(1) keeps one decimal place (sub-metre precision is sufficient)
    buildings["area_m2"] = buildings.geometry.area.round(1)

    # OSM height tag is a free-text string like "24" or "24m" or "8 stories"
    # pd.to_numeric converts clean numbers; errors="coerce" sets bad strings to NaN
    if "height" in buildings.columns:
        buildings["height_m"]   = pd.to_numeric(buildings["height"], errors="coerce")

        # Rough floor count: assume 3 metres per storey (typical commercial floor)
        # Int64 (capital I) is a nullable integer type — supports NaN unlike int64
        buildings["est_floors"] = (buildings["height_m"] / 3.0).round(0).astype("Int64")
    else:
        # If the dataset has no height column at all, fill with nulls
        buildings["height_m"]   = None
        buildings["est_floors"] = None

    # Remove slivers — tiny fragments under 10 m² are digitising artefacts,
    # not real buildings, and skew area statistics
    buildings = buildings[buildings["area_m2"] > 10]

    # Save to GeoPackage; layer= names the table inside the .gpkg SQLite file
    buildings.to_file(out_path, driver="GPKG", layer="buildings")
    print(f"  Saved {len(buildings):,} buildings → {out_path}")

    # Print quick area stats to check the data looks reasonable
    print(f"  Area: min={buildings.area_m2.min():.0f}m² | max={buildings.area_m2.max():.0f}m² | mean={buildings.area_m2.mean():.0f}m²")

    # Only print height stats if at least one building has a tagged height
    if buildings["height_m"].notna().any():
        valid_h = buildings["height_m"].dropna()  # exclude NaN rows for stats
        print(f"  Height (OSM): {len(valid_h):,} tagged | mean={valid_h.mean():.1f}m | max={valid_h.max():.1f}m")

    return buildings


# ── process_roads ─────────────────────────────────────────────────────────────
def process_roads(raw_path: str, out_path: str) -> gpd.GeoDataFrame:
    print("\nProcessing roads...")

    # Load OSM roads GeoJSON and reproject to UTM
    roads = load_and_reproject(raw_path)

    # Drop geometrically broken road segments before any analysis
    roads = roads[roads.geometry.is_valid].copy()

    # ── Normalise the highway column ──────────────────────────────────────────
    # OSM roads can carry multiple highway tags (e.g. a road tagged as both
    # "primary" and "bus_guideway").  pyogrio reads these as numpy arrays
    # instead of plain strings.  numpy arrays are not hashable, so they crash
    # value_counts(), isin(), and dissolve(by="highway").
    # Solution: always convert to a single plain Python string.
    def _normalise_highway(val):
        # Handle Python list or tuple (e.g. ["primary", "bus_guideway"])
        if isinstance(val, (list, tuple)):
            return str(val[0]) if len(val) > 0 else "unclassified"

        import numpy as np                  # local import — only needed here
        # Handle numpy array (the form pyogrio actually uses)
        if isinstance(val, np.ndarray):
            return str(val[0]) if val.size > 0 else "unclassified"

        # Scalar value — just stringify it (handles NaN → "nan" which is fine)
        return str(val) if val is not None else "unclassified"

    # Apply the normaliser to every row in the highway column
    roads["highway"] = roads["highway"].apply(_normalise_highway)

    # Count features per road type — useful for understanding the dataset
    road_types = roads["highway"].value_counts()
    print(f"  Road type distribution (top 10):")
    for rtype, count in road_types.head(10).items():
        # str() and int() convert numpy scalars to native Python types
        # so the f-string format specs (:<20, :>6,) work correctly
        print(f"    {str(rtype):<20} {int(count):>6,}")

    # Save all roads (all types) to GeoPackage
    roads.to_file(out_path, driver="GPKG", layer="roads")
    print(f"  Saved {len(roads):,} road segments → {out_path}")

    return roads


# ── process_landuse ───────────────────────────────────────────────────────────
def process_landuse(raw_path: str, out_path: str) -> gpd.GeoDataFrame:
    print("\nProcessing land use...")

    # Load OSM land use polygons and reproject
    landuse = load_and_reproject(raw_path)

    # Keep only polygon geometries — OSM land use should be polygons only
    # but defensive filtering avoids geometry type errors downstream
    landuse = landuse[landuse.geometry.geom_type.isin(["Polygon", "MultiPolygon"])].copy()

    # Drop invalid geometries for the same reason as buildings
    landuse = landuse[landuse.geometry.is_valid].copy()

    # Area of each land use zone in m² — useful for later analysis
    landuse["area_m2"] = landuse.geometry.area.round(1)

    # Print category distribution (e.g. residential, commercial, park)
    lu_dist = landuse["landuse"].value_counts()
    print(f"  Land use categories:")
    for lu, count in lu_dist.head(10).items():
        print(f"    {str(lu):<20} {int(count):>5,}")  # same str/int safety as roads

    # Save to GeoPackage
    landuse.to_file(out_path, driver="GPKG", layer="landuse")
    print(f"  Saved {len(landuse):,} land use polygons → {out_path}")

    return landuse


# ── create_road_buffers ───────────────────────────────────────────────────────
def create_road_buffers(roads: gpd.GeoDataFrame, out_path: str, buffer_m: float = 50.0):
    print(f"\nCreating {buffer_m}m road buffers (major roads only)...")

    # Filter to major road types only — residential streets are excluded
    # isin() works here because highway column is now plain strings
    major = roads[roads["highway"].isin(MAJOR_ROAD_TYPES)].copy()

    # buffer() expands each line geometry into a polygon of the given width
    # Units are metres because we are in UTM — this is an exact 50 m buffer
    major["geometry"] = major.geometry.buffer(buffer_m)

    # dissolve() merges overlapping buffer polygons that share the same highway type
    # e.g. two parallel "primary" road buffers that overlap become one polygon
    # reset_index() brings "highway" back as a regular column (dissolve makes it the index)
    dissolved = major.dissolve(by="highway").reset_index()

    # Save buffered polygons to GeoPackage
    dissolved.to_file(out_path, driver="GPKG", layer="road_buffers")

    # Total buffer area — divide by 1e6 to convert m² → km²
    total_area_km2 = dissolved.geometry.area.sum() / 1e6
    print(f"  Buffer area: {total_area_km2:.2f} km²")
    print(f"  Saved → {out_path}")

    return dissolved


# ── spatial_join_buildings_landuse ────────────────────────────────────────────
def spatial_join_buildings_landuse(buildings: gpd.GeoDataFrame, landuse: gpd.GeoDataFrame, out_path: str):
    print("\nSpatial join: buildings ∩ land use...")

    # gpd.sjoin attaches attributes from the right GDF to the left GDF
    # based on a spatial relationship.
    #
    # how="left"       — keep ALL buildings even if they fall outside any land use zone
    # predicate="within" — building must be fully inside a land use polygon
    #                      (use "intersects" if you want partial overlaps too)
    #
    # We pass only ["landuse", "geometry"] from the right side to avoid
    # bringing in all land use columns (area_m2, etc.) and cluttering the result
    joined = gpd.sjoin(
        buildings,
        landuse[["landuse", "geometry"]],
        how="left",
        predicate="within"
    )

    # ── Resolve column name collision ─────────────────────────────────────────
    # Both buildings and landuse GDFs have a column called "landuse"
    # (buildings carries it as a raw OSM tag on some features).
    # When both sides share a column name, geopandas renames them:
    #   left side  → "landuse_left"   (original OSM tag on the building)
    #   right side → "landuse_right"  (land use zone the building sits in)
    # We want the zone from the right side, so rename it to plain "landuse"
    if "landuse_right" in joined.columns:
        joined = joined.rename(columns={"landuse_right": "landuse"})

    # Drop the original building-level landuse tag — we no longer need it
    if "landuse_left" in joined.columns:
        joined = joined.drop(columns=["landuse_left"])

    # Count how many buildings fall in each land use zone
    # .size() counts rows per group; reset_index() turns the result back into a DataFrame
    summary = joined.groupby("landuse").size().reset_index(name="building_count")

    # Sort descending so the most common zone appears first
    summary = summary.sort_values("building_count", ascending=False)

    print("  Buildings by land use:")
    for _, row in summary.head(10).iterrows():
        # row.landuse — zone name (e.g. "residential")
        # row.building_count — number of buildings in that zone
        print(f"    {row.landuse:<20} {row.building_count:>5,}")

    # Save the joined result — each building row now has a "landuse" attribute
    joined.to_file(out_path, driver="GPKG", layer="buildings_with_landuse")
    print(f"  Saved → {out_path}")

    return joined


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Geospatial Pipeline — Step 04: Vector Feature Extraction")
    print("=" * 60)

    # List of input files that must exist before processing can start
    required = [
        f"{RAW_DIR}/osm_buildings.geojson",
        f"{RAW_DIR}/osm_roads.geojson",
        f"{RAW_DIR}/osm_landuse.geojson",
    ]

    # Guard: stop early with a helpful message if any input is missing
    for p in required:
        if not os.path.exists(p):
            print(f"ERROR: {p} not found. Run Step 01 first.")
            return

    # ── Process each layer ────────────────────────────────────────────────────

    # Buildings: reproject, compute area/height/floors, drop slivers
    buildings = process_buildings(
        f"{RAW_DIR}/osm_buildings.geojson",
        f"{PROC_DIR}/buildings_utm.gpkg"
    )

    # Roads: reproject, normalise highway tags, save
    roads = process_roads(
        f"{RAW_DIR}/osm_roads.geojson",
        f"{PROC_DIR}/roads_utm.gpkg"
    )

    # Land use: reproject, filter polygons, compute area, save
    landuse = process_landuse(
        f"{RAW_DIR}/osm_landuse.geojson",
        f"{PROC_DIR}/landuse_utm.gpkg"
    )

    # Road buffers: 50 m influence zones around major roads, dissolved by type
    create_road_buffers(roads, f"{PROC_DIR}/roads_buffered.gpkg")

    # Spatial join: attach land use zone to each building
    # Output is used in Step 05 for PostGIS loading and Step 06 for analysis
    spatial_join_buildings_landuse(
        buildings, landuse,
        f"{PROC_DIR}/buildings_with_landuse.gpkg"
    )

    print("\nStep 04 complete. Proceed to Step 05 (PostGIS loading).")


if __name__ == "__main__":
    main()  # entry point — only runs when called directly, not when imported
