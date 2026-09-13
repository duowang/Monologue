"""Command-line entry point: `monologue <command>`."""

from __future__ import annotations

import argparse
import logging

from monologue import __version__, db, export, latenighter, newsmax, sample, scraps, stats
from monologue.common import DATA_DIR_ENV, default_data_dir

CRAWLERS = (
    (newsmax, "Newsmax 'Best of Late Nite Jokes' pages (2009-2018)."),
    (latenighter, "LateNighter 'Monologues Round-Up' posts (2024-2025)."),
    (scraps, "Full transcripts from scrapsfromtheloft.com (2017-)."),
)
COMMANDS = (
    ("export", export, "Flatten all CSVs into one TSV or JSONL file."),
    ("stats", stats, "Print dataset statistics."),
    ("sample", sample, "Regenerate the public sample/ directory from the full dataset."),
    ("import-db", db, "Load the dataset into Postgres."),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="monologue", description="Late-night monologue dataset tools.")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument(
        "--data-dir",
        default=str(default_data_dir()),
        help=f"Directory holding one subfolder per source (default: ${DATA_DIR_ENV} or ./data).",
    )
    verbosity = parser.add_mutually_exclusive_group()
    verbosity.add_argument("-v", "--verbose", action="store_true", help="Show debug output.")
    verbosity.add_argument("-q", "--quiet", action="store_true", help="Only show warnings and errors.")
    commands = parser.add_subparsers(dest="command", required=True)

    crawl = commands.add_parser("crawl", help="Fetch new days from one source.")
    crawl_sources = crawl.add_subparsers(dest="source", required=True)
    for module, help_text in CRAWLERS:
        sub = crawl_sources.add_parser(module.SOURCE, help=help_text)
        module.add_arguments(sub)
        sub.set_defaults(func=module.run)

    for name, module, help_text in COMMANDS:
        sub = commands.add_parser(name, help=help_text)
        module.add_arguments(sub)
        sub.set_defaults(func=module.run)
    return parser


def configure_logging(args: argparse.Namespace) -> None:
    level = logging.DEBUG if args.verbose else logging.WARNING if args.quiet else logging.INFO
    logging.basicConfig(level=level, format="%(message)s")
    if not args.verbose:
        logging.getLogger("urllib3").setLevel(logging.WARNING)


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    configure_logging(args)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
