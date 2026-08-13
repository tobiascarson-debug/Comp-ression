"""CLI for Codex compressor"""
import argparse
import sys
from pathlib import Path
from tools.codex.codex.engine import CodexEngine


def parse_args(argv):
    p = argparse.ArgumentParser(prog="codex")
    sub = p.add_subparsers(dest="cmd")

    compress = sub.add_parser("compress", help="Compress a directory into a codex store")
    compress.add_argument("--input", required=True, help="Input directory")
    compress.add_argument("--store", required=True, help="Output codex store directory")

    listp = sub.add_parser("list", help="List files in the codex")
    listp.add_argument("--store", required=True, help="Codex store directory")

    extract = sub.add_parser("extract", help="Extract a file from the codex")
    extract.add_argument("--store", required=True, help="Codex store directory")
    extract.add_argument("--file", required=True, help="Path of the original file to extract (relative to input)")
    extract.add_argument("--dest", required=True, help="Destination file or directory")

    inspectp = sub.add_parser("inspect", help="Inspect codex database")
    inspectp.add_argument("--store", required=True, help="Codex store directory")

    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv or sys.argv[1:])
    if not args.cmd:
        print("Use --help for usage")
        return 1

    engine = CodexEngine(Path(args.store))

    if args.cmd == "compress":
        engine.compress_directory(Path(args.input))
    elif args.cmd == "list":
        files = engine.list_files()
        for f in files:
            print(f)
    elif args.cmd == "extract":
        engine.extract_file(args.file, Path(args.dest))
    elif args.cmd == "inspect":
        engine.inspect()
    else:
        print("Unknown command")
        return 2

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
