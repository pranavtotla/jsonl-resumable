"""Command-line interface for jsonl-resumable."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import NoReturn

from .index import JsonlIndex


def cmd_info(args: argparse.Namespace) -> int:
    """Handle the 'info' subcommand."""
    try:
        index = JsonlIndex(args.file)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    index_path = Path(args.file).with_suffix(".idx")
    index_exists = index_path.exists()

    if args.json:
        info = {
            "file": str(Path(args.file).resolve()),
            "lines": index.total_lines,
            "size_bytes": index.file_size,
            "index_exists": index_exists,
        }
        print(json.dumps(info, indent=2))
    else:
        print(f"File: {Path(args.file).resolve()}")
        print(f"Lines: {index.total_lines:,}")
        print(f"Size: {_format_size(index.file_size)}")
        print(f"Index: {'exists' if index_exists else 'will be created'}")

    return 0


def cmd_read(args: argparse.Namespace) -> int:
    """Handle the 'read' subcommand."""
    try:
        index = JsonlIndex(args.file)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    errors = False
    for line_num in args.lines:
        try:
            data = index.read_json(line_num)
            if args.pretty:
                print(json.dumps(data, indent=2))
            else:
                print(json.dumps(data))
        except IndexError:
            print(
                f"Error: Line {line_num} out of range (0-{index.total_lines - 1})",
                file=sys.stderr,
            )
            errors = True
        except json.JSONDecodeError as e:
            print(f"Error: Invalid JSON at line {line_num}: {e}", file=sys.stderr)
            errors = True

    return 1 if errors else 0


def cmd_sample(args: argparse.Namespace) -> int:
    """Handle the 'sample' subcommand."""
    try:
        index = JsonlIndex(args.file)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    if index.total_lines == 0:
        if args.pretty:
            print("[]")
        return 0

    try:
        records = index.sample(args.n, seed=args.seed)
    except json.JSONDecodeError as e:
        print(f"Error: Invalid JSON in file: {e}", file=sys.stderr)
        return 1

    if args.pretty:
        print(json.dumps(records, indent=2))
    else:
        for record in records:
            print(json.dumps(record))

    return 0


def _format_size(size_bytes: int) -> str:
    """Format byte size in human-readable form."""
    size: float = float(size_bytes)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if size < 1024:
            if unit == "B":
                return f"{int(size)} {unit}"
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} PB"


def create_parser() -> argparse.ArgumentParser:
    """Create the argument parser with all subcommands."""
    parser = argparse.ArgumentParser(
        prog="jsonl-index",
        description="O(1) random access and sampling for JSONL files",
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # info subcommand
    info_parser = subparsers.add_parser(
        "info",
        help="Show file information",
        description="Display line count, file size, and index status",
    )
    info_parser.add_argument("file", help="Path to JSONL file")
    info_parser.add_argument(
        "--json", action="store_true", help="Output as JSON for scripting"
    )
    info_parser.set_defaults(func=cmd_info)

    # read subcommand
    read_parser = subparsers.add_parser(
        "read",
        help="Read specific line(s)",
        description="Read and print one or more lines by line number",
    )
    read_parser.add_argument("file", help="Path to JSONL file")
    read_parser.add_argument(
        "lines", type=int, nargs="+", help="Line number(s) to read (0-indexed)"
    )
    read_parser.add_argument(
        "--pretty", action="store_true", help="Pretty-print JSON output"
    )
    read_parser.set_defaults(func=cmd_read)

    # sample subcommand
    sample_parser = subparsers.add_parser(
        "sample",
        help="Random sample of records",
        description="Get a random sample of n records from the file",
    )
    sample_parser.add_argument("file", help="Path to JSONL file")
    sample_parser.add_argument("n", type=int, help="Number of records to sample")
    sample_parser.add_argument(
        "--seed", type=int, help="Random seed for reproducibility"
    )
    sample_parser.add_argument(
        "--pretty", action="store_true", help="Output as JSON array (indented)"
    )
    sample_parser.set_defaults(func=cmd_sample)

    return parser


def main(argv: list[str] | None = None) -> NoReturn:
    """Main entry point for CLI."""
    parser = create_parser()
    args = parser.parse_args(argv)
    sys.exit(args.func(args))


if __name__ == "__main__":
    main()
