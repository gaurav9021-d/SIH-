"""Command-line entry points for SIH26162 ingestion."""

from __future__ import annotations

import argparse
import logging
from datetime import date
from pathlib import Path
from typing import Sequence

from ingestion.core.db import Database
from ingestion.core.errors import ConfigurationError, IngestionError
from ingestion.core.logging import configure_logging, get_logger, log_event, redact
from ingestion.core.settings import load_config, load_secrets
from ingestion.firms.backfill import run_firms_backfill

LOGGER = get_logger("cli")
PROJECT_ROOT = Path(__file__).resolve().parents[1]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ingestion",
        description="Resilient SIH26162 data ingestion",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    backfill = commands.add_parser("backfill", help="run a resumable historical backfill")
    backfill.add_argument("source", choices=["firms"])
    backfill.add_argument("--start", type=_calendar_date, required=True)
    backfill.add_argument(
        "--end",
        type=_calendar_date,
        required=True,
        help="inclusive calendar end date",
    )
    backfill.add_argument("--aoi", default=None)
    backfill.add_argument(
        "--products",
        default=None,
        help="comma-separated explicit FIRMS products; default routes each sensor family",
    )
    backfill.add_argument("--config", default=None)
    backfill.add_argument(
        "--dry-run",
        action="store_true",
        help="print deterministic preferred-product chunks without network or database access",
    )

    migrate = commands.add_parser("migrate", help="upgrade the ingestion schema")
    migrate.add_argument("--config", default=None)

    check = commands.add_parser("check", help="validate configuration and PostGIS access")
    check.add_argument("--config", default=None)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "backfill":
            products = (
                [value for value in args.products.split(",") if value.strip()]
                if args.products
                else None
            )
            return run_firms_backfill(
                start=args.start,
                end_inclusive=args.end,
                aoi_name=args.aoi,
                products=products,
                config_path=args.config,
                dry_run=args.dry_run,
            )
        if args.command == "migrate":
            return _migrate()
        if args.command == "check":
            return _check(args.config)
    except KeyboardInterrupt:
        configure_logging()
        log_event(LOGGER, logging.WARNING, "ingestion_interrupted")
        return 130
    except ConfigurationError as exc:
        configure_logging()
        log_event(LOGGER, logging.ERROR, "configuration_error", error=str(exc))
        return 2
    except IngestionError as exc:
        configure_logging()
        log_event(
            LOGGER,
            logging.ERROR,
            "ingestion_error",
            error=f"{type(exc).__name__}: {redact(str(exc))}",
        )
        return 1
    except Exception as exc:
        configure_logging()
        LOGGER.error(
            "unexpected_ingestion_error",
            exc_info=True,
            extra={
                "event": "unexpected_ingestion_error",
                "fields": {"error": f"{type(exc).__name__}: {redact(str(exc))}"},
            },
        )
        return 1
    return 2


def _migrate() -> int:
    secrets = load_secrets()
    database_url = secrets.require_database_url()
    map_key = (
        secrets.firms_map_key.get_secret_value()
        if secrets.firms_map_key is not None
        else None
    )
    configure_logging(map_key, database_url)

    from alembic import command
    from alembic.config import Config

    alembic_config = Config(str(PROJECT_ROOT / "alembic.ini"))
    alembic_config.set_main_option(
        "script_location", str(PROJECT_ROOT / "ingestion" / "migrations")
    )
    alembic_config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    command.upgrade(alembic_config, "head")
    log_event(LOGGER, logging.INFO, "migrations_complete", revision="head")
    return 0


def _check(config_path: str | None) -> int:
    config = load_config(config_path)
    secrets = load_secrets()
    map_key = secrets.require_firms_key()
    database_url = secrets.require_database_url()
    configure_logging(map_key, database_url)
    database = Database(database_url, config.database, application_name="ingestion:check")
    try:
        status = database.check(
            max_concurrent_backfills=config.firms.max_concurrent_backfills
        )
    finally:
        database.close()
    log_event(
        LOGGER,
        logging.INFO,
        "ingestion_check_complete",
        aoi=config.default_aoi,
        firms_key="present",
        **status,
    )
    return 0 if status["schema_ready"] else 2


def _calendar_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


if __name__ == "__main__":
    raise SystemExit(main())
