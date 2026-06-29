# GeoTIFF-to-Zarr
Stack a directory of single-date GeoTIFFs into a single chunked, compressed Zarr data cube with a time dimension. Useful for turning a pile of per-date raster snapshots into one analysis-ready array that supports lazy, out-of-core access through xarray and Dask.
