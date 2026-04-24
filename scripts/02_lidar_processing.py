"""
Step 02: LiDAR point cloud inspection and PDAL pipeline execution.

This script:
1. Inspects the raw LAZ file using laspy
2. Generates PDAL pipeline JSON files
3. Runs PDAL pipelines for ground classification, DTM, and DSM generation

Prerequisites:
    - PDAL installed (conda install -c conda-forge pdal python-pdal)
    - laspy installed (pip install laspy lazrs-python)

Usage:
    conda activate geospatial
    python scripts/02_lidar_processing.py
"""

import os                        # file path checks and directory creation
import json                      # write PDAL pipeline definitions as JSON files
import subprocess                # run PDAL as a command-line tool from Python
from collections import Counter  # count how many points fall in each LAS class

# Absolute paths so the script works regardless of where you run it from
RAW_DIR    = r'C:\RMTPROJECTS\dataengineering\geospatial\data\raw'
PROC_DIR   = r'C:\RMTPROJECTS\dataengineering\geospatial\data\processed'
SCRIPTS_DIR = r'C:\RMTPROJECTS\dataengineering\geospatial\scripts'

os.makedirs(PROC_DIR, exist_ok=True)  # create processed/ folder if it doesn't exist yet


# ── ASPRS classification lookup ───────────────────────────────────────────────
# LiDAR points carry a "Classification" attribute — a number assigned by the
# sensor or post-processing software.  The ASPRS LAS standard defines what
# each number means so that all software agrees on the labels.
ASPRS_CLASSES = {
    0:  "Never Classified",   # sensor default before any processing
    1:  "Unassigned",         # processed but not placed into a class
    2:  "Ground",             # bare-earth surface — used to build the DTM
    3:  "Low Vegetation",     # plants < ~0.5 m tall
    4:  "Medium Vegetation",  # plants 0.5–2 m tall
    5:  "High Vegetation",    # trees > 2 m (canopy top)
    6:  "Building",           # roof surfaces
    7:  "Low Noise",          # points below expected ground — likely errors
    9:  "Water",              # lakes, rivers, etc.
    17: "Bridge Deck",        # bridge surface (separate from ground)
    18: "High Noise",         # points far above everything — likely errors
}


# ── inspect_laz ───────────────────────────────────────────────────────────────
def inspect_laz(laz_path: str):
    """Print summary statistics for a LAZ file."""

    # laspy is a pure-Python reader for LAS/LAZ files.
    # lazrs-python provides the decompression backend for the compressed LAZ format.
    try:
        import laspy
    except ImportError:
        print("laspy not installed: pip install laspy lazrs-python")
        return

    print(f"\nInspecting: {laz_path}")

    # laspy.open() streams the file — the header is read without loading all points.
    with laspy.open(laz_path) as f:
        header = f.header

        # point_count — total number of 3-D measurements in this file
        print(f"  Point count:   {header.point_count:,}")

        # point_format — LAS standard format ID (0-10).
        # Format 6+ stores GPS time; format 1/6 is most common for 3DEP data.
        print(f"  Point format:  {header.point_format.id}")

        # Bounding box — the spatial extent of all points in the file.
        # X/Y are easting/northing (or lon/lat), Z is elevation in metres.
        print(f"  X range: {header.x_min:.2f} → {header.x_max:.2f}")
        print(f"  Y range: {header.y_min:.2f} → {header.y_max:.2f}")
        print(f"  Z range: {header.z_min:.2f}m → {header.z_max:.2f}m")

        # f.read() loads ALL points into memory as a LasData object.
        # For very large files (>1 GB) you'd chunk this, but it's fine for a city tile.
        las = f.read()

    # Count points per classification code using Counter (like a frequency table)
    class_counts = Counter(las.classification)

    # Total point count used to compute percentages
    total = len(las.classification)

    print("\n  Classification breakdown:")
    for cls_id, count in sorted(class_counts.items()):
        # Look up the human-readable name; fall back to "Class N" if unknown
        name = ASPRS_CLASSES.get(cls_id, f"Class {cls_id}")
        pct  = count / total * 100                  # percentage of all points
        # Print: code | name (padded) | count (right-aligned) | percentage
        print(f"    {cls_id:2d} {name:<25} {count:>10,}  ({pct:5.1f}%)")


# ── write_pipeline ────────────────────────────────────────────────────────────
def write_pipeline(pipeline_dict: dict, out_path: str):
    """Write a PDAL pipeline JSON file."""

    # PDAL reads its processing instructions from a JSON file called a "pipeline".
    # Each pipeline is a list of stages: reader → filters → writer.
    with open(out_path, "w") as f:
        json.dump(pipeline_dict, f, indent=2)  # indent=2 makes it human-readable
    print(f"  Pipeline written: {out_path}")


# ── run_pdal_pipeline ─────────────────────────────────────────────────────────
def run_pdal_pipeline(json_path: str):
    """Execute a PDAL pipeline."""

    print(f"  Running: pdal pipeline {json_path}")

    # subprocess.run() calls the PDAL CLI exactly as if you typed it in a terminal:
    #   pdal pipeline <json_path>
    # capture_output=True  — capture stdout/stderr so we can print errors
    # text=True            — decode bytes to string automatically
    result = subprocess.run(
        ["pdal", "pipeline", json_path],
        capture_output=True, text=True
    )

    if result.returncode != 0:
        # returncode != 0 means PDAL reported an error
        print(f"  ERROR: {result.stderr}")
    else:
        print("  Pipeline completed successfully.")

    # Return True if the pipeline succeeded, False otherwise — used by main()
    # to decide whether to continue to the next step
    return result.returncode == 0


# ── create_smrf_pipeline ──────────────────────────────────────────────────────
def create_smrf_pipeline(laz_in: str, laz_out: str) -> dict:
    """
    Build a PDAL pipeline that:
      1. Reads the raw LAZ file
      2. Reprojects points to UTM Zone 13N (metres)
      3. Classifies ground vs non-ground using SMRF
      4. Writes a new LAZ with updated Classification values

    SMRF = Simple Morphological Filter — it works by progressively analysing
    the local minimum elevation across a sliding window to decide which points
    sit on the bare earth vs on top of objects (buildings, trees).
    """
    return {
        "pipeline": [
            # Stage 1 — Reader: open the raw LAZ file
            # readers.las handles both .las (uncompressed) and .laz (compressed)
            {"type": "readers.las", "filename": laz_in},

            # Stage 2 — Reproject all X/Y/Z coordinates to EPSG:32613
            # (UTM Zone 13N, metres).  Working in metres is required by SMRF
            # because its parameters (slope, window) are expressed in metres.
            {"type": "filters.reprojection", "out_srs": "EPSG:32613"},

            # Stage 3 — Ground classification with SMRF
            {
                "type":      "filters.smrf",
                "scalar":    1.2,   # vertical tolerance multiplier — higher = more lenient
                "slope":     0.2,   # max terrain slope SMRF accepts as ground (20%)
                "threshold": 0.45,  # max height above the local surface to still be ground (m)
                "window":    18.0   # analysis window size in metres — should exceed widest building
            },

            # Stage 4 — Writer: save the classified point cloud back to LAZ
            # compression="laszip" keeps the file compressed (LAZ format)
            {
                "type":        "writers.las",
                "filename":    laz_out,
                "compression": "laszip"
            }
        ]
    }


# ── create_dtm_pipeline ───────────────────────────────────────────────────────
def create_dtm_pipeline(laz_in: str, tif_out: str) -> dict:
    """
    Build a PDAL pipeline that produces a DTM (Digital Terrain Model).

    DTM = bare-earth surface only — buildings and trees are excluded.
    We achieve this by keeping only class-2 (Ground) points and then
    interpolating them into a regular grid (raster).
    """
    return {
        "pipeline": [
            # Stage 1 — Read the classified LAZ produced by the SMRF pipeline
            {"type": "readers.las", "filename": laz_in},

            # Stage 2 — Keep ONLY ground points (Classification == 2)
            # "Classification[2:2]" means: keep points where Classification
            # is in the range 2 to 2 (i.e., exactly class 2 = Ground)
            {"type": "filters.range", "limits": "Classification[2:2]"},

            # Stage 3 — Rasterise the ground points into a GeoTIFF
            {
                "type":        "writers.gdal",
                "filename":    tif_out,
                "resolution":  1.0,        # output pixel size = 1 m × 1 m
                "output_type": "idw",      # IDW = Inverse Distance Weighting interpolation
                                           # fills cells by weighting nearby points by 1/distance
                "radius":      2.0,        # search radius (m) to find points for each cell
                "gdaldriver":  "GTiff",    # output format = GeoTIFF
                "gdalopts":    "COMPRESS=LZW"  # LZW lossless compression to reduce file size
            }
        ]
    }


# ── create_dsm_pipeline ───────────────────────────────────────────────────────
def create_dsm_pipeline(laz_in: str, tif_out: str) -> dict:
    """
    Build a PDAL pipeline that produces a DSM (Digital Surface Model).

    DSM = full surface including buildings, tree canopy, and anything above ground.
    We achieve this by keeping only first-return points — the laser pulse hits
    the topmost surface first and that return captures the highest object.
    """
    return {
        "pipeline": [
            # Stage 1 — Read the classified LAZ
            {"type": "readers.las", "filename": laz_in},

            # Stage 2 — Keep ONLY first-return points (returnnumber == 1)
            # A single laser pulse can bounce multiple times (tree, then ground).
            # returnnumber[1:1] selects the FIRST bounce = topmost surface.
            {"type": "filters.range", "limits": "returnnumber[1:1]"},

            # Stage 3 — Rasterise into a GeoTIFF
            {
                "type":        "writers.gdal",
                "filename":    tif_out,
                "resolution":  1.0,        # 1 m × 1 m pixels
                "output_type": "max",      # use the HIGHEST Z value in each cell
                                           # (captures rooftop peaks, tree tops)
                "radius":      1.5,        # smaller radius than DTM — surface is denser
                "gdaldriver":  "GTiff",
                "gdalopts":    "COMPRESS=LZW"
            }
        ]
    }


# ── main ──────────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("Geospatial Pipeline — Step 02: LiDAR Processing")
    print("=" * 60)

    # Input: raw LAZ downloaded from OpenTopography
    laz_raw        = f"{RAW_DIR}/lidar_denver.laz"

    # Output: same point cloud but with Classification values updated by SMRF
    laz_classified = f"{PROC_DIR}/lidar_classified.laz"

    # Guard: stop early if the LAZ file hasn't been downloaded yet
    if not os.path.exists(laz_raw):
        print(f"ERROR: {laz_raw} not found.")
        print("Download from https://portal.opentopography.org/")
        return

    # ── 1. Inspect raw file ───────────────────────────────────────────────────
    # Read the header and print point count, bounding box, and class breakdown.
    # This helps verify the file is valid before running expensive PDAL pipelines.
    inspect_laz(laz_raw)

    # ── 2. SMRF ground classification ─────────────────────────────────────────
    # Write the pipeline JSON to disk, then execute it with PDAL.
    # Output: lidar_classified.laz — same points, updated Classification field.
    smrf_json = f"{SCRIPTS_DIR}/smrf_pipeline.json"
    write_pipeline(create_smrf_pipeline(laz_raw, laz_classified), smrf_json)
    print("\nRunning SMRF ground classification...")
    if not run_pdal_pipeline(smrf_json):
        return  # stop if SMRF failed — DTM/DSM depend on correct classification

    # ── 3. Generate DTM from ground points ────────────────────────────────────
    # Filter class-2 points only → interpolate → 1 m GeoTIFF bare-earth surface.
    # Output used in Step 03 as the denominator of nDSM = DSM - DTM.
    dtm_json = f"{SCRIPTS_DIR}/dtm_pipeline.json"
    dtm_out  = f"{PROC_DIR}/dtm_from_lidar.tif"
    write_pipeline(create_dtm_pipeline(laz_classified, dtm_out), dtm_json)
    print("\nGenerating DTM (ground points only)...")
    run_pdal_pipeline(dtm_json)

    # ── 4. Generate DSM from first returns ────────────────────────────────────
    # Filter first-return points only → take max Z per cell → 1 m GeoTIFF full surface.
    # Output used in Step 03 as the numerator of nDSM = DSM - DTM.
    dsm_json = f"{SCRIPTS_DIR}/dsm_pipeline.json"
    dsm_out  = f"{PROC_DIR}/dsm_from_lidar.tif"
    write_pipeline(create_dsm_pipeline(laz_classified, dsm_out), dsm_json)
    print("\nGenerating DSM (first returns)...")
    run_pdal_pipeline(dsm_json)

    # ── 5. Inspect classified file ────────────────────────────────────────────
    # Verify SMRF worked — class-2 (Ground) should now make up a significant
    # percentage.  If Ground is 0% something went wrong with the filter.
    if os.path.exists(laz_classified):
        print("\nClassified point cloud stats:")
        inspect_laz(laz_classified)

    print("\nStep 02 complete.")
    print("Next: run Step 03 to compute DEM derivatives and nDSM (DSM - DTM)")


if __name__ == "__main__":
    main()  # entry point — only runs when the script is called directly
