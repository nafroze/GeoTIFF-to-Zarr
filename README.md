# GeoTIFF-to-Zarr
Stack a directory of single-date GeoTIFFs into a single chunked, compressed Zarr data cube with a time dimension. Useful for turning a pile of per-date raster snapshots into one analysis-ready array that supports lazy, out-of-core access through xarray and Dask.

## Why

Time-series raster analysis is awkward when every date is its own GeoTIFF:
opening hundreds of files is slow, and the data rarely fits in memory. Converting
the stack once into a Zarr cube (time x y x x) makes downstream analysis fast,
chunk-aligned, and parallel-friendly.

## What it does

- Reads every GeoTIFF in an input directory and orders them by date.
- Confirms a consistent grid (CRS, resolution, extent) across inputs.
- Writes a single Zarr store chunked along time and space, with compression.
- Records the date of each layer as a coordinate on the time axis.

## Installation

```bash
pip install -r requirements.txt
```

Requires numpy, rasterio, rioxarray, xarray, zarr, numcodecs, and dask.

## Usage

```bash
python build_zarr_from_tifs.py \
    --input-dir data/daily_tifs \
    --output store/cube.zarr \
    --var-name value \
    --date-regex "(\d{8})" \
    --date-format "%Y%m%d" \
    --chunk-time 64 \
    --chunk-xy 512
```

| Argument | Description |
|----------|-------------|
| `--input-dir` | Directory of single-date GeoTIFFs. |
| `--output` | Path for the output Zarr store. |
| `--var-name` | Name of the data variable in the cube (default: `value`). |
| `--date-regex` | Regex with one capture group for the date in each filename (default: `(\d{8})`). |
| `--date-format` | strptime format for the captured date string (default: `%Y%m%d`). |
| `--chunk-time` | Time chunk size (default: 64). |
| `--chunk-xy` | Spatial chunk size (default: 512). |

For filenames using a year and day-of-year convention, such as
`SENSOR_2019_DOY012.tif`, pass `--date-regex "(20\d{2}_DOY\d{1,3})"` and
`--date-format "%Y_DOY%j"`.

## Output

A Zarr store that can be opened lazily:

```python
import xarray as xr
cube = xr.open_zarr("store/cube.zarr")
cube["value"].sel(time="2023-02-15")
```

## Sample data

A small clipped sample (a handful of tiny GeoTIFFs) lives in `sample_data/`
so the tool can be run end to end without a large download.
