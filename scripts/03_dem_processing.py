"""
Step 03: DEM/DSM processing with rasterio.

Operations:
- Reproject DEM to UTM Zone 13N (EPSG:32613)
- Print summary statistics

For GDAL CLI equivalents, see 03_surface_modeling_dem_dsm.md

Usage:
    conda activate geospatial
    python scripts/03_dem_processing.py
"""

import os
import rasterio
from rasterio.warp import calculate_default_transform, reproject, Resampling
from rasterio.fill import fillnodata
import numpy as np

RAW_DIR = r'C:\RMTPROJECTS\dataengineering\geospatial\data\raw'
PROC_DIR = r'C:\RMTPROJECTS\dataengineering\geospatial\data\processed'
DST_CRS = "EPSG:32613"

os.makedirs(PROC_DIR, exist_ok=True)


def reproject_dem(src_path: str, dst_path: str, dst_crs: str = DST_CRS, resolution: float = 10.0):
    """Reproject a raster to the target CRS and resolution."""
    print(f"Reprojecting {src_path} → {dst_path}")

    with rasterio.open(src_path) as src:
        print(f"  Source CRS: {src.crs}, size: {src.width}x{src.height}, res: {src.res}")

        transform, width, height = calculate_default_transform(
            src.crs, dst_crs, src.width, src.height, *src.bounds,
            resolution=resolution
        )
        kwargs = src.meta.copy()
        kwargs.update({
            "crs": dst_crs,
            "transform": transform,
            "width": width,
            "height": height,
            "compress": "lzw",
            "nodata": src.nodata or -9999,
        })

        with rasterio.open(dst_path, "w", **kwargs) as dst:
            for band_idx in range(1, src.count + 1):
                reproject(
                    source=rasterio.band(src, band_idx),
                    destination=rasterio.band(dst, band_idx),
                    src_transform=src.transform,
                    src_crs=src.crs,
                    dst_transform=transform,
                    dst_crs=dst_crs,
                    resampling=Resampling.bilinear,
                )

    print(f"  Output size: {width}x{height}, res: {resolution}m")


def fill_nodata(src_path: str, dst_path: str, max_search_distance: int = 10):
    """Fill NoData gaps using interpolation."""
    print(f"Filling NoData: {src_path} → {dst_path}")

    with rasterio.open(src_path) as src:
        profile = src.profile.copy()
        data = src.read(1)
        nodata_val = src.nodata or -9999

        mask = (data != nodata_val).astype(np.uint8)
        filled = fillnodata(data, mask=mask, max_search_distance=max_search_distance)

        with rasterio.open(dst_path, "w", **profile) as dst:
            dst.write(filled, 1)

    nodata_count = np.sum(data == nodata_val)
    print(f"  Filled {nodata_count:,} NoData cells")


def print_stats(path: str, label: str = ""):
    """Print raster statistics."""
    with rasterio.open(path) as src:
        data = src.read(1, masked=True)
        print(f"\n{label or path}:")
        print(f"  CRS: {src.crs}")
        print(f"  Size: {src.width}x{src.height} pixels, res={src.res[0]:.1f}m")
        print(f"  Min: {data.min():.2f}m | Max: {data.max():.2f}m | Mean: {data.mean():.2f}m")
        print(f"  NoData count: {data.mask.sum():,} / {data.size:,}")


def compute_ndsm(dsm_path: str, dtm_path: str, out_path: str):
    """Compute normalized DSM (nDSM = DSM - DTM)."""
    print(f"\nComputing nDSM: {dsm_path} - {dtm_path}")

    with rasterio.open(dsm_path) as dsm_src, rasterio.open(dtm_path) as dtm_src:
        dsm = dsm_src.read(1, masked=True)
        dtm = dtm_src.read(1, masked=True)
        ndsm = np.ma.where(dsm - dtm > 0, dsm - dtm, 0)

        profile = dsm_src.profile.copy()
        profile["compress"] = "lzw"

        with rasterio.open(out_path, "w", **profile) as dst:
            dst.write(ndsm.filled(-9999), 1)

    print(f"  nDSM saved: {out_path}")
    print(f"  Max above-ground height: {ndsm.max():.1f}m")


def main():
    print("=" * 60)
    print("Geospatial Pipeline — Step 02: DEM Processing")
    print("=" * 60)

    dem_raw = f"{RAW_DIR}/dem_usgs_10m.tif"
    dem_utm = f"{PROC_DIR}/dem_utm.tif"
    dem_filled = f"{PROC_DIR}/dem_filled.tif"

    if not os.path.exists(dem_raw):
        print(f"ERROR: {dem_raw} not found. Download from nationalmap.gov first.")
        print("See 01_data_acquisition.md for instructions.")
        return

    reproject_dem(dem_raw, dem_utm)
    print_stats(dem_utm, "DEM (UTM)")

    fill_nodata(dem_utm, dem_filled)
    print_stats(dem_filled, "DEM (Filled)")

    # nDSM (only if LiDAR outputs exist from Step 03)
    dsm_path = f"{PROC_DIR}/dsm_from_lidar.tif"
    dtm_path = f"{PROC_DIR}/dtm_from_lidar.tif"
    ndsm_path = f"{PROC_DIR}/ndsm.tif"

    if os.path.exists(dsm_path) and os.path.exists(dtm_path):
        compute_ndsm(dsm_path, dtm_path, ndsm_path)
        print_stats(ndsm_path, "nDSM")
    else:
        print("\nnDSM skipped — run Step 03 (LiDAR) first to generate DSM/DTM.")

    print("\nStep 02 complete. Run gdaldem for hillshade/slope/aspect:")
    print("  gdaldem hillshade data/processed/dem_filled.tif data/processed/hillshade.tif")
    print("  gdaldem slope data/processed/dem_filled.tif data/processed/slope.tif")
    print("  gdaldem aspect data/processed/dem_filled.tif data/processed/aspect.tif")


if __name__ == "__main__":
    main()
