from __future__ import annotations

import argparse
from pathlib import Path

from polars_io_benchmark.benchmark import (
    DEFAULT_OUTDIR,
    DEFAULT_REPEATS,
    DEFAULT_SEED,
    DEFAULT_SIZES,
    FORMATS,
    FULL_SIZES,
    run_benchmark,
)
from polars_io_benchmark.plot import DEFAULT_METRICS, plot_results


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be a positive integer")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Replicate a pandas CSV/Feather/Pickle/Parquet I/O benchmark with "
            "Polars-native optimized and non-optimized schemas."
        )
    )
    parser.add_argument(
        "--sizes",
        nargs="+",
        type=positive_int,
        default=list(DEFAULT_SIZES),
        help="Row counts to benchmark. Default: 1_000 10_000 100_000 1_000_000.",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Use the fuller size sweep from 1k through 5M rows.",
    )
    parser.add_argument(
        "--repeats",
        type=positive_int,
        default=DEFAULT_REPEATS,
        help="Timing repetitions per operation. Default: 5.",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=DEFAULT_SEED,
        help="Base random seed. Default: 42.",
    )
    parser.add_argument(
        "--outdir",
        type=Path,
        default=DEFAULT_OUTDIR,
        help="Directory for generated data files and results CSV.",
    )
    parser.add_argument(
        "--formats",
        nargs="+",
        choices=tuple(FORMATS),
        default=list(FORMATS),
        help="Formats to benchmark.",
    )
    parser.add_argument(
        "--no-plot",
        action="store_true",
        help="Skip plot generation after the benchmark completes.",
    )
    parser.add_argument(
        "--plot-outdir",
        type=Path,
        default=None,
        help="Directory for plot PNGs. Default: <outdir>/plots.",
    )
    parser.add_argument(
        "--plot-metric",
        nargs="+",
        choices=DEFAULT_METRICS,
        default=list(DEFAULT_METRICS),
        help="Plot metric or metrics. Default: all metrics.",
    )
    parser.add_argument(
        "--plot-combined",
        action="store_true",
        help="Also write combined non-optimized + optimized plots for each metric.",
    )
    return parser


def main() -> None:
    args = build_parser().parse_args()
    sizes = FULL_SIZES if args.full else args.sizes
    results = run_benchmark(
        sizes=sizes,
        outdir=args.outdir,
        repeats=args.repeats,
        seed=args.seed,
        formats=args.formats,
    )
    results_csv = args.outdir / "polars_io_benchmark_results.csv"
    print(f"\nSaved results to: {results_csv}")
    if not args.no_plot:
        plot_outdir = args.plot_outdir or args.outdir / "plots"
        print(f"Saving plots to: {plot_outdir}")
        plot_results(results_csv, plot_outdir, args.plot_metric, args.plot_combined)
    print(results)


if __name__ == "__main__":
    main()
