"""Command-line entrypoints for the clustering engine."""

from __future__ import annotations

import argparse
import json
import logging
from dataclasses import replace
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any, Sequence

from ingestion.core.db import Database
from ingestion.core.errors import ConfigurationError, IngestionError
from ingestion.core.logging import configure_logging, get_logger, log_event, redact

from clustering.core.params import ClusterParams, validate
from clustering.core.stats import SizeHistogram
from clustering.engine import RunSummary, run_from_store
from clustering.settings import load_config, load_secrets
from clustering.stress import ScenarioResult, run_all

LOGGER = get_logger("clustering.cli")
PROJECT_ROOT = Path(__file__).resolve().parents[1]

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_CONFIG = 2


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m clustering",
        description="Causal diameter-bounded spatiotemporal clustering of FIRMS detections",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    for name, help_text in (
        ("run", "cluster a date range and persist the result"),
        ("preview", "cluster a date range and report without persisting"),
    ):
        sub = commands.add_parser(name, help=help_text)
        sub.add_argument("--start", type=_calendar_date, required=True)
        sub.add_argument(
            "--end", type=_calendar_date, required=True, help="inclusive calendar date"
        )
        sub.add_argument("--aoi", default=None)
        sub.add_argument("--eval-time", type=_timestamp, default=None)
        sub.add_argument("--products", default=None)
        sub.add_argument("--eps-m", type=float, default=None)
        sub.add_argument("--min-samples", type=int, default=None)
        sub.add_argument("--inactivity-days", type=float, default=None)
        sub.add_argument("--max-diameter-m", type=float, default=None)
        sub.add_argument("--config", default=None)
        sub.add_argument("--json", action="store_true")

    stress = commands.add_parser("stress", help="validate parameters against extremes")
    stress.add_argument("--scenario", default=None)
    stress.add_argument("--config", default=None)
    stress.add_argument("--json", action="store_true")
    stress.add_argument(
        "--persist",
        action="store_true",
        help="persist store-backed scenario results (off by default)",
    )

    migrate = commands.add_parser("migrate", help="upgrade the clustering schema")
    migrate.add_argument("--config", default=None)

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command in {"run", "preview"}:
            return _run(args, persist=args.command == "run")
        if args.command == "stress":
            return _stress(args)
        if args.command == "migrate":
            return _migrate()
    except KeyboardInterrupt:
        configure_logging()
        log_event(LOGGER, logging.WARNING, "clustering_interrupted")
        return 130
    except ConfigurationError as exc:
        configure_logging()
        log_event(LOGGER, logging.ERROR, "configuration_error", error=str(exc))
        return EXIT_CONFIG
    except IngestionError as exc:
        configure_logging()
        log_event(
            LOGGER,
            logging.ERROR,
            "clustering_error",
            error=f"{type(exc).__name__}: {redact(str(exc))}",
        )
        return EXIT_FAILED
    except Exception as exc:  # pragma: no cover - defensive
        configure_logging()
        LOGGER.error(
            "unexpected_clustering_error",
            exc_info=True,
            extra={
                "event": "unexpected_clustering_error",
                "fields": {"error": f"{type(exc).__name__}: {redact(str(exc))}"},
            },
        )
        return EXIT_FAILED
    return EXIT_CONFIG


def _effective_params(config_params: ClusterParams, args: Any) -> ClusterParams:
    overrides: dict[str, Any] = {}
    if args.eps_m is not None:
        overrides["eps_m"] = args.eps_m
    if args.min_samples is not None:
        overrides["min_samples"] = args.min_samples
    if args.inactivity_days is not None:
        overrides["inactivity_days"] = args.inactivity_days
        if args.__dict__.get("max_linkage_gap_days") is None:
            overrides["max_linkage_gap_days"] = args.inactivity_days
    if args.max_diameter_m is not None:
        overrides["max_cluster_diameter_m"] = args.max_diameter_m
    if not overrides:
        return config_params
    return validate(replace(config_params, **overrides))


def _run(args: Any, *, persist: bool) -> int:
    config = load_config(args.config)
    params = _effective_params(config.params, args)
    aoi = config.get_aoi(args.aoi)
    products = (
        [p.strip() for p in args.products.split(",") if p.strip()]
        if args.products
        else None
    )

    secrets = load_secrets()
    database_url = secrets.require_database_url()
    configure_logging(database_url)

    # Fail fast on startup connectivity. The pooled engine's retry budget exists
    # for a transient blip mid-run, not for waiting out a database that is simply
    # not running.
    if not _database_reachable(database_url):
        raise ConfigurationError(
            "Cannot reach the database. Start it with "
            "`docker compose --env-file .env -f docker/docker-compose.yml up -d postgis` "
            "and check DATABASE_URL."
        )

    database = Database(
        database_url, config.database, application_name="ingestion:clustering"
    )
    try:
        summary = run_from_store(
            database,
            params,
            aoi=aoi,
            start=args.start,
            end_inclusive=args.end,
            evaluation_time=args.eval_time,
            products=products,
            batch_size=config.runtime.batch_size,
            flush_every=config.runtime.flush_every,
            persist=persist,
        )
    finally:
        database.close()

    if args.json:
        print(json.dumps(summary.as_dict(), indent=2, sort_keys=True))
    else:
        for line in render_summary(summary, persisted=persist):
            print(line)
    return EXIT_OK


def _stress(args: Any) -> int:
    config = load_config(args.config)
    secrets = load_secrets()
    database: Database | None = None
    database_url = (
        secrets.database_url.get_secret_value()
        if secrets.database_url is not None
        else None
    )
    configure_logging(database_url)

    # Probe with a short timeout first. Without this, an unreachable database
    # makes the harness sit through the full retry budget before the fixture
    # scenarios, which need no database at all, ever get to report.
    if database_url is not None and _database_reachable(
        secrets.require_database_url()
    ):
        database = Database(
            secrets.require_database_url(),
            config.database,
            application_name="ingestion:clustering-stress",
        )
    elif database_url is not None:
        log_event(
            LOGGER,
            logging.INFO,
            "database_unreachable",
            note="store-backed scenarios will be skipped; fixtures still run",
        )

    try:
        results = run_all(
            config,
            database=database,
            only=args.scenario,
            persist=args.persist,
        )
    finally:
        if database is not None:
            database.close()

    if args.json:
        print(json.dumps([r.as_dict() for r in results], indent=2, sort_keys=True))
    else:
        for line in render_scenarios(results):
            print(line)

    return EXIT_FAILED if any(r.verdict == "failed" for r in results) else EXIT_OK


def _database_reachable(database_url: str, timeout_s: int = 10) -> bool:
    """Cheap liveness probe that bypasses the pooled engine's retry budget.

    The timeout has to clear a remote connection, not just a local socket: a
    Supabase pooler handshake measured about 3 s, so a 3 s budget sat exactly on
    the boundary and intermittently reported a healthy database as unreachable.
    Ten seconds keeps the fail-fast benefit while leaving real headroom.
    """
    from sqlalchemy import create_engine, text

    engine = create_engine(
        database_url, connect_args={"connect_timeout": timeout_s}, pool_pre_ping=False
    )
    try:
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return True
    except Exception:
        return False
    finally:
        engine.dispose()


def _migrate() -> int:
    secrets = load_secrets()
    database_url = secrets.require_database_url()
    configure_logging(database_url)

    from alembic import command
    from alembic.config import Config

    alembic_config = Config(str(PROJECT_ROOT / "alembic.ini"))
    alembic_config.set_main_option(
        "script_location", str(PROJECT_ROOT / "clustering" / "migrations")
    )
    alembic_config.set_main_option("sqlalchemy.url", database_url.replace("%", "%%"))
    command.upgrade(alembic_config, "head")
    log_event(LOGGER, logging.INFO, "clustering_migrations_complete", revision="head")
    return EXIT_OK


def render_summary(summary: RunSummary, *, persisted: bool) -> list[str]:
    params = summary.params
    sizes = summary.size_distribution()
    extents = summary.extent_distribution()
    limit = float(params["max_cluster_diameter_m"])
    headroom = limit - summary.max_extent_m

    lines = [
        "Clustering run",
        f"  run id            {summary.run_id}",
        f"  aoi               {summary.aoi}",
        f"  window            [{summary.window_start.isoformat()}, "
        f"{summary.window_end.isoformat()})  half-open, UTC",
        f"  evaluation time   {summary.evaluation_time.isoformat()}",
        f"  settled horizon   {summary.settled_horizon.isoformat()}",
        f"  persisted         {'yes' if persisted else 'no (preview)'}",
        "",
        "Parameters",
        f"  eps_m             {params['eps_m']}  (ceiling 750.0)",
        f"  min_samples       {params['min_samples']}",
        f"  inactivity_days   {params['inactivity_days']}",
        f"  linkage_gap_days  {params['max_linkage_gap_days']}",
        f"  max_diameter_m    {limit}",
        f"  reconciliation    {params['reconcile_spatial_m']} m / "
        f"{params['reconcile_temporal_minutes']} min"
        f"{'' if params['reconcile_enabled'] else '  (disabled)'}",
        f"  algorithm         {params['algorithm_name']} {params['algorithm_version']}",
        f"  fingerprint       {params['params_fingerprint']}",
        "",
        "Input",
        f"  detections read   {summary.detections_read}",
        f"  observations      {summary.observations}  (after reconciliation)",
        "",
        "Clusters",
        f"  total             {summary.cluster_count}",
        f"  open / closed     {summary.clusters_open} / {summary.clusters_closed}",
        f"  noise             {summary.noise_count} "
        f"({summary.noise_fraction * 100:.1f}% of observations)",
        f"  pending           {summary.pending_count}  (inside the linkage window)",
        "",
        "Size distribution (observations per cluster)",
        f"  min {sizes.minimum:.0f}  median {sizes.median:.0f}  p90 {sizes.p90:.0f}  "
        f"p99 {sizes.p99:.0f}  max {sizes.maximum:.0f}  mean {sizes.mean:.1f}",
    ]
    lines.extend(SizeHistogram.of(summary.sizes).render())
    lines.extend(
        [
            "",
            "Extent distribution (metres)",
            f"  median {extents.median:.0f}  p90 {extents.p90:.0f}  "
            f"p99 {extents.p99:.0f}  max {extents.maximum:.0f}",
            f"  max observed {summary.max_extent_m:.1f} m against a {limit:.0f} m "
            f"limit  (headroom {headroom:.1f} m)",
            "",
            f"  duration {summary.duration_s:.2f} s",
        ]
    )
    return lines


def render_scenarios(results: list[ScenarioResult]) -> list[str]:
    marks = {"passed": "PASS", "failed": "FAIL", "skipped": "SKIP"}
    lines = ["Stress scenarios", ""]
    for result in results:
        lines.append(f"  [{marks[result.verdict]}] {result.name}")
        if result.description:
            lines.append(f"         {result.description}")
        if result.synthetic:
            lines.append("         source: SYNTHETIC fixture, not observed data")
        if result.reason:
            lines.append(f"         reason: {result.reason}")
        if result.summary is not None:
            summary = result.summary
            sizes = summary.size_distribution()
            lines.append(
                f"         detections={summary.detections_read} "
                f"observations={summary.observations} clusters={summary.cluster_count} "
                f"noise={summary.noise_count} pending={summary.pending_count}"
            )
            lines.append(
                f"         sizes min/median/p99/max="
                f"{sizes.minimum:.0f}/{sizes.median:.0f}/{sizes.p99:.0f}/{sizes.maximum:.0f}"
                f"  max_extent={summary.max_extent_m:.1f} m"
                f"  limit={summary.params['max_cluster_diameter_m']:.0f} m"
            )
        for failure in result.failures:
            lines.append(f"         failed: {failure}")
        lines.append("")

    verdicts = [r.verdict for r in results]
    lines.append(
        f"  {verdicts.count('passed')} passed, {verdicts.count('failed')} failed, "
        f"{verdicts.count('skipped')} skipped"
    )
    return lines


def _calendar_date(value: str) -> date:
    try:
        return date.fromisoformat(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected YYYY-MM-DD") from exc


def _timestamp(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise argparse.ArgumentTypeError("expected an ISO 8601 timestamp") from exc
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=UTC)


if __name__ == "__main__":
    raise SystemExit(main())
