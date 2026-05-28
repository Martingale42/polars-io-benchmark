from __future__ import annotations

import gc
import json
import pickle
import statistics as stats
import time
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass
from pathlib import Path

import numpy as np
import polars as pl

DEFAULT_SEED = 42
DEFAULT_REPEATS = 5
DEFAULT_OUTDIR = Path("benchmark_out")
DEFAULT_SIZES = (1_000, 10_000, 100_000, 1_000_000)
FULL_SIZES = (
    *range(1_000, 10_001, 1_000),
    *range(20_000, 100_001, 10_000),
    *range(200_000, 1_000_001, 100_000),
    *range(2_000_000, 5_000_001, 1_000_000),
)

COLS = tuple(f"col_{i}" for i in range(1, 21))
CAT_COLS = tuple(f"col_{i}" for i in range(1, 5))
INT_COLS = tuple(f"col_{i}" for i in range(5, 9))
FLOAT_COLS = tuple(f"col_{i}" for i in range(9, 13))
BOOL_COLS = tuple(f"col_{i}" for i in range(13, 17))
DATE_COLS = tuple(f"col_{i}" for i in range(17, 21))

NONOPT_SCHEMA: dict[str, pl.DataType] = {
    **{c: pl.String for c in (*CAT_COLS, *INT_COLS, *FLOAT_COLS, *BOOL_COLS)},
    **{c: pl.Datetime("ns") for c in DATE_COLS},
}

OPT_SCHEMA: dict[str, pl.DataType] = {
    **{c: pl.Categorical for c in CAT_COLS},
    "col_5": pl.Int8,
    "col_6": pl.Int16,
    "col_7": pl.Int16,
    "col_8": pl.Int32,
    **{c: pl.Float32 for c in FLOAT_COLS},
    **{c: pl.Boolean for c in BOOL_COLS},
    **{c: pl.Datetime("ns") for c in DATE_COLS},
}


@dataclass(frozen=True)
class FormatSpec:
    suffix: str
    reader: Callable[[Path, dict[str, pl.DataType]], pl.DataFrame]
    writer: Callable[[pl.DataFrame, Path], None]


def make_base_df(n: int, seed: int = DEFAULT_SEED) -> pl.DataFrame:
    rng = np.random.default_rng(seed + n)
    categories = np.array(
        ["alpha", "bravo", "charlie", "delta", "echo", "foxtrot", "golf", "hotel"],
        dtype=object,
    )

    data: dict[str, object] = {}
    for col in CAT_COLS:
        data[col] = rng.choice(categories, size=n)

    data["col_5"] = rng.integers(-100, 101, size=n, dtype=np.int16)
    data["col_6"] = rng.integers(-20_000, 20_001, size=n, dtype=np.int32)
    data["col_7"] = rng.integers(0, 30_001, size=n, dtype=np.int32)
    data["col_8"] = rng.integers(-1_000_000, 1_000_001, size=n, dtype=np.int64)

    for col in FLOAT_COLS:
        data[col] = rng.normal(loc=0.0, scale=1.0, size=n).astype(np.float64)

    for col in BOOL_COLS:
        data[col] = rng.choice([False, True], size=n)

    start = np.datetime64("2020-01-01T00:00:00", "ns")
    max_seconds = 5 * 365 * 24 * 60 * 60
    for col in DATE_COLS:
        offsets = rng.integers(0, max_seconds, size=n, dtype=np.int64)
        data[col] = start + offsets.astype("timedelta64[s]")

    return pl.DataFrame(data).select(COLS)


def cast_schema(df: pl.DataFrame, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return df.with_columns(
        pl.col(col).cast(dtype) for col, dtype in schema.items()
    ).select(COLS).rechunk()


def make_nonopt_df(n: int, seed: int = DEFAULT_SEED) -> pl.DataFrame:
    return cast_schema(make_base_df(n, seed), NONOPT_SCHEMA)


def make_opt_df(n: int, seed: int = DEFAULT_SEED) -> pl.DataFrame:
    return cast_schema(make_base_df(n, seed), OPT_SCHEMA)


def csv_parse_schema(schema: dict[str, pl.DataType]) -> dict[str, pl.DataType]:
    csv_schema = dict(schema)
    for col in CAT_COLS:
        if csv_schema[col] == pl.Categorical:
            csv_schema[col] = pl.String
    return csv_schema


def read_csv_typed(path: Path, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    df = pl.read_csv(path, schema=csv_parse_schema(schema), try_parse_dates=True)
    return cast_schema(df, schema)


def write_csv(df: pl.DataFrame, path: Path) -> None:
    df.write_csv(path)


def read_ipc_typed(path: Path, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return cast_schema(pl.read_ipc(path), schema)


def write_ipc(df: pl.DataFrame, path: Path) -> None:
    df.write_ipc(path)


def read_parquet_typed(path: Path, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    return cast_schema(pl.read_parquet(path), schema)


def write_parquet_snappy(df: pl.DataFrame, path: Path) -> None:
    df.write_parquet(path, compression="snappy")


def write_parquet_zstd(df: pl.DataFrame, path: Path) -> None:
    df.write_parquet(path, compression="zstd")


def read_pickle_typed(path: Path, schema: dict[str, pl.DataType]) -> pl.DataFrame:
    with path.open("rb") as file:
        df = pickle.load(file)
    return cast_schema(df, schema)


def write_pickle(df: pl.DataFrame, path: Path) -> None:
    with path.open("wb") as file:
        pickle.dump(df, file, protocol=pickle.HIGHEST_PROTOCOL)


FORMATS: dict[str, FormatSpec] = {
    "csv": FormatSpec(".csv", read_csv_typed, write_csv),
    "feather_ipc": FormatSpec(".feather", read_ipc_typed, write_ipc),
    "parquet_snappy": FormatSpec(".parquet", read_parquet_typed, write_parquet_snappy),
    "parquet_zstd": FormatSpec(".parquet", read_parquet_typed, write_parquet_zstd),
    "pickle": FormatSpec(".pkl", read_pickle_typed, write_pickle),
}


def timed(fn: Callable[[], object], repeats: int) -> dict[str, float]:
    timings: list[float] = []
    for _ in range(repeats):
        gc.collect()
        start = time.perf_counter()
        fn()
        timings.append(time.perf_counter() - start)

    return {
        "min_s": min(timings),
        "median_s": stats.median(timings),
        "mean_s": stats.mean(timings),
        "samples_s": json.dumps(timings),
    }


def file_size_mb(path: Path) -> float:
    return path.stat().st_size / 1024 / 1024


def benchmark_one(
    n: int,
    kind: str,
    df: pl.DataFrame,
    schema: dict[str, pl.DataType],
    outdir: Path,
    repeats: int,
    formats: Sequence[str],
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    ram_mb = df.estimated_size("mb")

    for format_name in formats:
        io = FORMATS[format_name]
        path = outdir / f"{kind}_{n}_{format_name}{io.suffix}"
        roundtrip_path = outdir / f"{kind}_{n}_{format_name}_roundtrip{io.suffix}"

        write_stats = timed(lambda: io.writer(df, path), repeats)
        size_mb = file_size_mb(path)
        read_stats = timed(lambda: io.reader(path, schema), repeats)

        def read_then_write() -> None:
            loaded = io.reader(path, schema)
            io.writer(loaded, roundtrip_path)

        read_write_stats = timed(read_then_write, repeats)
        roundtrip_path.unlink(missing_ok=True)

        rows.append(
            {
                "n": n,
                "kind": kind,
                "format": format_name,
                "ram_mb": ram_mb,
                "file_mb": size_mb,
                "write_min_s": write_stats["min_s"],
                "write_median_s": write_stats["median_s"],
                "write_mean_s": write_stats["mean_s"],
                "write_samples_s": write_stats["samples_s"],
                "read_min_s": read_stats["min_s"],
                "read_median_s": read_stats["median_s"],
                "read_mean_s": read_stats["mean_s"],
                "read_samples_s": read_stats["samples_s"],
                "read_write_min_s": read_write_stats["min_s"],
                "read_write_median_s": read_write_stats["median_s"],
                "read_write_mean_s": read_write_stats["mean_s"],
                "read_write_samples_s": read_write_stats["samples_s"],
            }
        )

    return rows


def run_benchmark(
    *,
    sizes: Iterable[int] = DEFAULT_SIZES,
    outdir: Path = DEFAULT_OUTDIR,
    repeats: int = DEFAULT_REPEATS,
    seed: int = DEFAULT_SEED,
    formats: Sequence[str] = tuple(FORMATS),
) -> pl.DataFrame:
    outdir.mkdir(parents=True, exist_ok=True)
    all_rows: list[dict[str, object]] = []

    for n in sizes:
        print(f"Running n={n:,} non-optimized")
        nonopt_df = make_nonopt_df(n, seed)
        all_rows.extend(
            benchmark_one(n, "nonopt", nonopt_df, NONOPT_SCHEMA, outdir, repeats, formats)
        )
        del nonopt_df
        gc.collect()

        print(f"Running n={n:,} optimized")
        opt_df = make_opt_df(n, seed)
        all_rows.extend(
            benchmark_one(n, "opt", opt_df, OPT_SCHEMA, outdir, repeats, formats)
        )
        del opt_df
        gc.collect()

    results = pl.DataFrame(all_rows).sort(["n", "kind", "format"])
    results.write_csv(outdir / "polars_io_benchmark_results.csv")
    return results
