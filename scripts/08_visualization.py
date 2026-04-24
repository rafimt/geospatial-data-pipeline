"""
Step 08: Export PostGIS results and build interactive Folium map.

Outputs:
- data/output/denver_pipeline_map.html — interactive map
- data/output/buildings_export.geojson
- data/output/road_influence.geojson
- data/output/land_suitability.geojson
- data/output/raster_preview.png — side-by-side raster comparison

Prerequisites:
    pip install folium
    GDAL CLI for GeoJSON export (ogr2ogr)

Usage:
    conda activate geospatial
    python scripts/08_visualization.py
"""

import os
import subprocess
import geopandas as gpd
import folium
import rasterio
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

DB_URI = "PG:host=127.0.0.1 port=5433 dbname=geospatial user=geouser password=geopass"

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_ROOT = os.path.dirname(_SCRIPT_DIR)
OUT_DIR = os.path.join(_ROOT, "data", "output")
PROC_DIR = os.path.join(_ROOT, "data", "processed")

os.makedirs(OUT_DIR, exist_ok=True)

CENTER = [39.70, -104.95]  # Denver, WGS84


# ─── Helper functions ────────────────────────────────────────────────────────

def ogr_export(sql: str, out_path: str):
    """Export a PostGIS SQL query to GeoJSON (WGS84)."""
    cmd = [
        "ogr2ogr",
        "-f", "GeoJSON",
        "-t_srs", "EPSG:4326",
        out_path,
        DB_URI,
        "-sql", sql,
        "-overwrite",
    ]
    result = subprocess.run(cmd, capture_output=True, text=True,
                            env={**os.environ, "PGPASSWORD": "geopass"})
    if result.returncode != 0:
        print(f"  WARN: {result.stderr[:200]}")
    else:
        size = os.path.getsize(out_path) / 1e3
        print(f"  Exported: {out_path} ({size:.0f} KB)")


def height_color(h):
    if h is None or h == 0:
        return "#2166ac"
    if h < 5:    return "#2166ac"
    if h < 15:   return "#74add1"
    if h < 30:   return "#fdae61"
    return "#d73027"


def suitability_color(score):
    if score is None: return "#888888"
    if score < 25:    return "#d73027"
    if score < 50:    return "#fc8d59"
    if score < 75:    return "#91cf60"
    return "#1a9641"


# ─── Export layers ────────────────────────────────────────────────────────────

def export_layers():
    print("Exporting PostGIS layers to GeoJSON...")

    ogr_export(
        """SELECT b.id, b.area_m2, b.height_m,
                  lh.lidar_height_m, bc.cluster_id,
                  ST_SimplifyPreserveTopology(b.geom, 0.00005) AS geom
           FROM processed.buildings b
           LEFT JOIN analysis.buildings_lidar_height lh USING (id)
           LEFT JOIN analysis.building_clusters bc USING (id)
           WHERE b.area_m2 > 50""",
        f"{OUT_DIR}/buildings_export.geojson"
    )

    ogr_export(
        "SELECT highway, ST_SimplifyPreserveTopology(geom, 0.0001) AS geom FROM analysis.road_influence",
        f"{OUT_DIR}/road_influence.geojson"
    )

    ogr_export(
        """SELECT id, suitability_score,
                  ST_SimplifyPreserveTopology(geom, 0.0001) AS geom
           FROM analysis.land_suitability""",
        f"{OUT_DIR}/land_suitability.geojson"
    )


# ─── Raster preview ──────────────────────────────────────────────────────────

def raster_preview():
    rasters = [
        (f"{PROC_DIR}/hillshade.tif", "Hillshade", "gray"),
        (f"{PROC_DIR}/slope.tif",     "Slope (°)", "YlOrRd"),
        (f"{PROC_DIR}/ndsm.tif",      "nDSM — Height (m)", "viridis"),
    ]

    available = [(p, t, c) for p, t, c in rasters if os.path.exists(p)]
    if not available:
        print("  SKIP raster preview — no processed rasters found")
        return

    fig, axes = plt.subplots(1, len(available), figsize=(6 * len(available), 6))
    if len(available) == 1:
        axes = [axes]

    for ax, (path, title, cmap) in zip(axes, available):
        with rasterio.open(path) as src:
            data = src.read(1, masked=True)
        im = ax.imshow(data, cmap=cmap, interpolation="bilinear")
        ax.set_title(title, fontsize=12)
        ax.axis("off")
        plt.colorbar(im, ax=ax, shrink=0.7)

    plt.suptitle("Denver Geospatial Pipeline — Raster Outputs", fontsize=14)
    plt.tight_layout()
    out = f"{OUT_DIR}/raster_preview.png"
    plt.savefig(out, dpi=150, bbox_inches="tight")
    plt.close()
    print(f"  Saved: {out}")


# ─── Folium map ──────────────────────────────────────────────────────────────

def build_folium_map():
    print("Building interactive Folium map...")

    m = folium.Map(location=CENTER, zoom_start=13, tiles="CartoDB dark_matter")

    # Hillshade raster overlay
    hillshade_path = f"{PROC_DIR}/hillshade.tif"
    hillshade_png = f"{OUT_DIR}/hillshade_preview.png"
    if os.path.exists(hillshade_path):
        subprocess.run([
            "gdal_translate", "-of", "PNG", "-scale", "-ot", "Byte",
            hillshade_path, hillshade_png
        ], capture_output=True)
        if os.path.exists(hillshade_png):
            bounds = [[39.60, -105.05], [39.80, -104.85]]
            folium.raster_layers.ImageOverlay(
                image=hillshade_png,
                bounds=bounds,
                opacity=0.4,
                name="Hillshade",
            ).add_to(m)

    # Road influence zones
    road_path = f"{OUT_DIR}/road_influence.geojson"
    if os.path.exists(road_path):
        road_gdf = gpd.read_file(road_path)
        folium.GeoJson(
            road_gdf,
            name="Road Influence (50m)",
            style_function=lambda _: {
                "fillColor": "#FF4444", "color": "#CC0000",
                "weight": 1, "fillOpacity": 0.3
            },
            tooltip=folium.GeoJsonTooltip(fields=["highway"])
        ).add_to(m)

    # Buildings colored by LiDAR height
    bldg_path = f"{OUT_DIR}/buildings_export.geojson"
    if os.path.exists(bldg_path):
        bldg_gdf = gpd.read_file(bldg_path)
        folium.GeoJson(
            bldg_gdf,
            name="Buildings (LiDAR Height)",
            style_function=lambda f: {
                "fillColor": height_color(f["properties"].get("lidar_height_m")),
                "color": "#333333", "weight": 0.3, "fillOpacity": 0.75
            },
            tooltip=folium.GeoJsonTooltip(
                fields=["area_m2", "lidar_height_m", "cluster_id"],
                aliases=["Area (m²)", "LiDAR Height (m)", "Cluster ID"]
            )
        ).add_to(m)

    # Land suitability (hidden by default)
    suit_path = f"{OUT_DIR}/land_suitability.geojson"
    if os.path.exists(suit_path):
        suit_gdf = gpd.read_file(suit_path)
        folium.GeoJson(
            suit_gdf,
            name="Land Suitability",
            show=False,
            style_function=lambda f: {
                "fillColor": suitability_color(f["properties"].get("suitability_score")),
                "color": "none", "fillOpacity": 0.6
            },
            tooltip=folium.GeoJsonTooltip(
                fields=["suitability_score"],
                aliases=["Suitability Score"]
            )
        ).add_to(m)

    # Legend
    legend_html = """
    <div style="position:fixed;bottom:50px;left:50px;z-index:1000;background:white;
         padding:15px;border-radius:8px;font-size:12px;box-shadow:2px 2px 6px rgba(0,0,0,0.3);">
    <b>Building Height (LiDAR)</b><br>
    <span style="background:#2166ac;padding:2px 12px;">&nbsp;</span> &lt; 5m<br>
    <span style="background:#74add1;padding:2px 12px;">&nbsp;</span> 5–15m<br>
    <span style="background:#fdae61;padding:2px 12px;">&nbsp;</span> 15–30m<br>
    <span style="background:#d73027;padding:2px 12px;">&nbsp;</span> &gt; 30m<br>
    </div>
    """
    m.get_root().html.add_child(folium.Element(legend_html))
    folium.LayerControl(collapsed=False).add_to(m)

    out = f"{OUT_DIR}/denver_pipeline_map.html"
    m.save(out)
    print(f"  Saved: {out}")
    print("  Open in browser to explore the interactive map.")


# ─── Main ────────────────────────────────────────────────────────────────────

def main():
    print("=" * 60)
    print("Geospatial Pipeline — Step 08: Visualization")
    print("=" * 60)

    export_layers()
    raster_preview()
    build_folium_map()

    print("\nStep 08 complete — pipeline finished!")
    print(f"  Main output: {OUT_DIR}/denver_pipeline_map.html")


if __name__ == "__main__":
    main()
