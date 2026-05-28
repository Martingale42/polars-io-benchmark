# polars-io-benchmark

A slim `uv` project for replicating the LinkedIn article's DataFrame I/O
experiment with Polars.

The original study compares CSV, Feather, Pickle, and Parquet using
non-optimized and optimized pandas DataFrames. This repo keeps the same logical
20-column design but translates it to Polars-native schemas:

| Column group | Non-optimized Polars schema | Optimized Polars schema |
| --- | --- | --- |
| `col_1` - `col_4` | `String` | `Categorical` |
| `col_5` | `String` | `Int8` |
| `col_6` - `col_7` | `String` | `Int16` |
| `col_8` | `String` | `Int32` |
| `col_9` - `col_12` | `String` | `Float32` |
| `col_13` - `col_16` | `String` | `Boolean` |
| `col_17` - `col_20` | `Datetime(ns)` | `Datetime(ns)` |

This is not a bit-for-bit reproduction of pandas `object` columns. In Polars,
`String` is the practical non-optimized baseline; `Object` is a Python object
escape hatch and is not representative for columnar I/O benchmarks.

## Setup

```bash
uv sync
```

## Run a smoke benchmark

```bash
uv run polars-io-benchmark --sizes 1000 10000 --repeats 3
```

Results are written to:

```text
benchmark_out/polars_io_benchmark_results.csv
```

Plots are written to:

```text
benchmark_out/plots/
```

Generated benchmark files are also written under `benchmark_out/`.

## Run the default benchmark

```bash
uv run polars-io-benchmark
```

Default row counts are `1_000`, `10_000`, `100_000`, and `1_000_000`.

## Run the fuller sweep

```bash
uv run polars-io-benchmark --full
```

The fuller sweep goes from 1k rows through 5M rows and then writes all plots to
`benchmark_out/plots`. It can take a while, especially for CSV and Pickle.

To run the benchmark without generating plots:

```bash
uv run polars-io-benchmark --full --no-plot
```

## Select formats

```bash
uv run polars-io-benchmark \
  --sizes 1000 10000 \
  --formats csv feather_ipc parquet_snappy parquet_zstd
```

Available formats:

- `csv`
- `feather_ipc`
- `parquet_snappy`
- `parquet_zstd`
- `pickle`

## Plot results

```bash
uv run polars-io-plot
```

Use this only when you already have a results CSV and want to regenerate plots.
By default this reads `benchmark_out/polars_io_benchmark_results.csv` and writes
all plots to `benchmark_out/plots`.

To plot only one metric:

```bash
uv run polars-io-plot --metric read_median_s
```

To also generate combined non-optimized + optimized plots:

```bash
uv run polars-io-plot --combined
```

The benchmark command supports the same combined plot mode:

```bash
uv run polars-io-benchmark --full --plot-combined
```

Supported metrics are:

- `read_median_s`
- `write_median_s`
- `read_write_median_s`
- `file_mb`
- `ram_mb`

## Output columns

The result CSV contains one row per row-count, schema kind, and format:

- `n`
- `kind`: `nonopt` or `opt`
- `format`
- `ram_mb`
- `file_mb`
- `write_min_s`, `write_median_s`, `write_mean_s`
- `read_min_s`, `read_median_s`, `read_mean_s`
- `read_write_min_s`, `read_write_median_s`, `read_write_mean_s`
