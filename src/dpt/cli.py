import argparse
import sys
from pathlib import Path

import duckdb

from .config import PipelineError, load_config
from .ingest import ingest
from .reporting import publish


def main(argv=None):
    parser = argparse.ArgumentParser(prog="dpt", description="Validate data deliveries and publish local reports.")
    commands = parser.add_subparsers(dest="command", required=True)
    loader = commands.add_parser("ingest", help="Import and validate a CSV or Parquet delivery.")
    loader.add_argument("--config", required=True, type=Path)
    loader.add_argument("--source", required=True, type=Path)
    loader.add_argument("--delivery", required=True)
    loader.add_argument("--work-dir", required=True, type=Path)
    reporter = commands.add_parser("report", help="Publish a completed report package.")
    reporter.add_argument("--dataset", required=True)
    reporter.add_argument("--work-dir", required=True, type=Path)
    reporter.add_argument("--output", required=True, type=Path)
    args = parser.parse_args(argv)
    try:
        if args.command == "ingest":
            result = ingest(load_config(args.config), args.source, args.delivery, args.work_dir)
        else:
            result = publish(args.dataset, args.work_dir, args.output)
    except PipelineError as error:
        print(f"Error: {error}", file=sys.stderr)
        return 1
    except (OSError, duckdb.Error):
        print("Error: Work directory or database is unavailable. Check access and disk space.", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Interrupted.", file=sys.stderr)
        return 130
    print(result)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
