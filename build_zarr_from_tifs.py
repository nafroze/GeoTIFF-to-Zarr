#!/usr/bin/env python3
# ============================================================
# build_zarr_from_tifs.py
# Stack a directory of single-date GeoTIFFs into a consolidated,
# chunked, compressed Zarr data cube with a time dimension.
# Author: Nazia Afroze
# ============================================================

import os
import re
import glob
import shutil
import logging
import argparse
from collections import Counter
from datetime import datetime

import dask.array as da
import numpy as np
import rasterio
import rioxarray as rxr
import xarray as xr
from dask import delayed

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

# Default date extraction: an 8-digit YYYYMMDD token anywhere in the filename.
# Override with --date-regex / --date-format for other naming conventions, e.g.
# day-of-year names like "SENSOR_2019_DOY012.tif":
#   --date-regex "(20\d{2}_DOY\d{1,3})" --date-format "%Y_DOY%j"
DEFAULT_DATE_REGEX = r"(\d{8})"
DEFAULT_DATE_FORMAT = "%Y%m%d"


def _tif_shape_crs(path):
    """Return ((height, width), CRS) without loading pixel values."""
    with rasterio.open(path) as src:
        return (src.height, src.width), src.crs


def _make_date_parser(date_regex, date_format):
    """Build a function that extracts a date from a filename, or None."""
    pattern = re.compile(date_regex)

    def date_from_name(path):
        match = pattern.search(os.path.basename(path))
        if not match:
            return None
        try:
            return datetime.strptime(match.group(1), date_format)
        except ValueError:
            return None

    return date_from_name


def build_zarr_from_tifs(
    tif_dir,
    zarr_out,
    var_name="value",
    date_regex=DEFAULT_DATE_REGEX,
    date_format=DEFAULT_DATE_FORMAT,
    chunk_time=64,
    chunk_xy=512,
):
    """Build a consolidated Zarr store from a directory of dated GeoTIFFs.

    Every GeoTIFF must share the same grid (height, width, CRS) to be stacked.
    Files that do not match the most common grid are reported and skipped.
    """
    logger.info("Building Zarr from: %s", tif_dir)
    if not os.path.isdir(tif_dir):
        raise FileNotFoundError(f"Input directory not found: {tif_dir}")

    all_files = sorted(
        p
        for p in glob.glob(os.path.join(tif_dir, "*.tif"))
        if not os.path.basename(p).startswith("_")
    )
    if not all_files:
        raise FileNotFoundError(f"No GeoTIFFs found in {tif_dir}")

    date_from_name = _make_date_parser(date_regex, date_format)

    # One file per calendar day; if duplicates exist, keep the last by sort order.
    by_date = {}
    for f in all_files:
        d = date_from_name(f)
        if d is not None:
            by_date[d] = f
    files = [(f, d) for d, f in sorted(by_date.items(), key=lambda x: x[0])]
    if not files:
        raise RuntimeError(
            "No valid dated GeoTIFFs found. Check --date-regex and --date-format."
        )

    # Probe grid and CRS for every layer; dask can only stack a consistent grid.
    metas = []
    for f, d in files:
        try:
            shp, crs = _tif_shape_crs(f)
            metas.append((f, d, shp, crs))
        except Exception as exc:
            logger.warning("Skipping unreadable GeoTIFF %s: %s", f, exc)

    if not metas:
        raise RuntimeError("No readable GeoTIFFs after grid probe.")

    key_counts = Counter((m[2], m[3]) for m in metas)
    (ref_shape, ref_crs), ref_n = key_counts.most_common(1)[0]
    kept = [(f, d) for f, d, shp, crs in metas if shp == ref_shape and crs == ref_crs]
    skipped = [(f, d, shp, crs) for f, d, shp, crs in metas
               if not (shp == ref_shape and crs == ref_crs)]

    logger.info(
        "Grid filter: kept %d / %d (reference %dx%d px, CRS=%s; %d matched).",
        len(kept), len(metas), ref_shape[0], ref_shape[1], ref_crs, ref_n,
    )
    if skipped:
        logger.info("Skipped %d off-grid GeoTIFF(s), e.g.:", len(skipped))
        for f, d, shp, crs in skipped[:25]:
            logger.info("  %s date=%s shape=%dx%d crs=%s",
                        os.path.basename(f), d.date(), shp[0], shp[1], crs)
        if len(skipped) > 25:
            logger.info("  ... and %d more", len(skipped) - 25)

    files = kept
    if not files:
        raise RuntimeError(
            "No GeoTIFFs left after grid/CRS filter; inputs differ in size or CRS."
        )

    # First kept file is on the reference grid; use it as the coordinate sample.
    sample = rxr.open_rasterio(files[0][0], masked=True).squeeze("band", drop=True)
    crs = sample.rio.crs
    shape = sample.shape
    dtype = sample.dtype
    logger.info("Sample shape: %s, dtype=%s, CRS=%s", shape, dtype, crs)

    def _read_single(p):
        arr = rxr.open_rasterio(p, masked=True).squeeze("band", drop=True)
        return arr.data

    arrays = [
        da.from_delayed(delayed(_read_single)(f), shape=shape, dtype=dtype)
        for f, _ in files
    ]

    stack = da.stack(arrays, axis=0)
    times = np.array([np.datetime64(d) for _, d in files])

    data_array = xr.DataArray(
        stack,
        dims=("time", "y", "x"),
        coords={"time": times, "y": sample["y"], "x": sample["x"]},
        name=var_name,
    ).rio.write_crs(crs)

    ds = xr.Dataset({var_name: data_array}).chunk(
        {"time": chunk_time, "y": chunk_xy, "x": chunk_xy}
    )

    # numcodecs.Zstd is valid for Zarr v2 stores only. zarr-python 3.x defaults to
    # v3, which expects different codec objects, so v2 is forced below.
    encoding = {var_name: {}}
    try:
        import numcodecs
        encoding[var_name]["compressor"] = numcodecs.Zstd(level=3)
    except Exception:
        logger.warning("numcodecs not available; writing without Zstd compression.")

    if os.path.exists(zarr_out):
        logger.info("Removing existing Zarr store: %s", zarr_out)
        shutil.rmtree(zarr_out, ignore_errors=True)

    zarr_kw = {"mode": "w", "consolidated": True, "encoding": encoding}
    try:
        import importlib.metadata as _imd
        zmaj = int(_imd.version("zarr").split(".")[0])
    except Exception:
        zmaj = 3
    if zmaj >= 3:
        zarr_kw["zarr_format"] = 2

    try:
        ds.to_zarr(zarr_out, **zarr_kw)
    except TypeError:
        # Older xarray uses zarr_version=2 instead of zarr_format=2.
        zarr_kw.pop("zarr_format", None)
        try:
            ds.to_zarr(zarr_out, **zarr_kw, zarr_version=2)
        except TypeError:
            zarr_kw["encoding"] = {var_name: {}}
            ds.to_zarr(zarr_out, **zarr_kw)

    logger.info("Zarr written at: %s", zarr_out)
    logger.info("Date range: %s to %s | layers: %d", times.min(), times.max(), len(files))
    return zarr_out


def parse_args():
    p = argparse.ArgumentParser(
        description="Stack a directory of single-date GeoTIFFs into a Zarr data cube."
    )
    p.add_argument("--input-dir", required=True,
                   help="Directory containing single-date GeoTIFFs.")
    p.add_argument("--output", required=True,
                   help="Path to the output Zarr store.")
    p.add_argument("--var-name", default="value",
                   help="Name of the data variable in the cube (default: value).")
    p.add_argument("--date-regex", default=DEFAULT_DATE_REGEX,
                   help="Regex with one capture group for the date in each filename.")
    p.add_argument("--date-format", default=DEFAULT_DATE_FORMAT,
                   help="strptime format for the captured date string (default: %%Y%%m%%d).")
    p.add_argument("--chunk-time", type=int, default=64,
                   help="Time chunk size (default: 64).")
    p.add_argument("--chunk-xy", type=int, default=512,
                   help="Spatial chunk size (default: 512).")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    build_zarr_from_tifs(
        args.input_dir,
        args.output,
        var_name=args.var_name,
        date_regex=args.date_regex,
        date_format=args.date_format,
        chunk_time=args.chunk_time,
        chunk_xy=args.chunk_xy,
    )
