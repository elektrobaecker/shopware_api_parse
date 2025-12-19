from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path
from typing import Optional

from .bmecat import extract_input, iter_bmecat_products, write_products_ndjson
from .config import Settings
from .importer import import_products
from .logging_utils import configure_logging

DEFAULT_WORKDIR = Path("./work")
DEFAULT_CONFIG = Path("config.yml")


def bmecat_extract_cmd(args: argparse.Namespace) -> None:
    configure_logging()
    input_path = Path(args.input)
    workdir = Path(args.workdir)
    result = extract_input(input_path, workdir)
    print(result)


def bmecat_parse_cmd(args: argparse.Namespace) -> None:
    configure_logging()
    settings = Settings.load(Path(args.config))
    input_path = Path(args.input or Path(args.workdir) / "input.xml")
    output_path = Path(args.output or Path(args.workdir) / "products.ndjson")
    products = iter_bmecat_products(input_path, settings.mapping)
    write_products_ndjson(products, output_path)
    print(output_path)


def shopware_import_cmd(args: argparse.Namespace) -> None:
    configure_logging()
    settings = Settings.load(Path(args.config))
    ndjson_path = Path(args.ndjson or Path(args.workdir) / "products.ndjson")
    stats = import_products(ndjson_path, settings, batch_size=args.batch_size)
    print(f"Processed={stats.products_processed} uploaded={stats.products_uploaded} errors={stats.errors}")
    if stats.errors:
        sys.exit(1)


def run_all_cmd(args: argparse.Namespace) -> None:
    configure_logging()
    settings = Settings.load(Path(args.config))
    workdir = Path(args.workdir)
    input_path = Path(args.input)
    xml_path = extract_input(input_path, workdir)
    products_path = workdir / "products.ndjson"
    products = iter_bmecat_products(xml_path, settings.mapping)
    write_products_ndjson(products, products_path)
    stats = import_products(products_path, settings, batch_size=args.batch_size)
    print(f"Processed={stats.products_processed} uploaded={stats.products_uploaded} errors={stats.errors}")
    if stats.errors:
        sys.exit(1)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="BMEcat importer for Shopware 6")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), help="Path to config.yml")
    parser.add_argument("--workdir", default=str(DEFAULT_WORKDIR), help="Working directory")

    subparsers = parser.add_subparsers(dest="command", required=True)

    extract = subparsers.add_parser("bmecat_extract", help="Extract XML from ZIP or copy XML to workdir")
    extract.add_argument("input", help="Path to input ZIP or XML")
    extract.set_defaults(func=bmecat_extract_cmd)

    parse = subparsers.add_parser("bmecat_parse", help="Parse BMEcat XML to NDJSON")
    parse.add_argument("--input", help="Path to XML (default workdir/input.xml)")
    parse.add_argument("--output", help="Path to NDJSON output (default workdir/products.ndjson)")
    parse.set_defaults(func=bmecat_parse_cmd)

    imp = subparsers.add_parser("shopware_import", help="Import NDJSON into Shopware using sync API")
    imp.add_argument("--ndjson", help="Path to NDJSON (default workdir/products.ndjson)")
    imp.add_argument("--batch-size", type=int, default=250, help="Batch size for sync")
    imp.set_defaults(func=shopware_import_cmd)

    run_all = subparsers.add_parser("run_all", help="Run extract, parse and import")
    run_all.add_argument("input", help="Path to input ZIP or XML")
    run_all.add_argument("--batch-size", type=int, default=250, help="Batch size for sync")
    run_all.set_defaults(func=run_all_cmd)

    return parser


def main(argv: Optional[list[str]] = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)


if __name__ == "__main__":
    main()