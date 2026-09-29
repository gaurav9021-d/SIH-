"""Convenience wrapper for the FIRMS backfill CLI."""

from __future__ import annotations

import sys

from ingestion.__main__ import main


if __name__ == "__main__":
    raise SystemExit(main(["backfill", "firms", *sys.argv[1:]]))
