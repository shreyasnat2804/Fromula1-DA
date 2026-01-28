#!/usr/bin/env python3
"""Generate schema and basic metadata for all CSV files under a data folder."""

from __future__ import annotations

import argparse
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List

import pandas as pd


def summarize_csv(path: Path) -> Dict[str, Any]:
    """Return schema and simple stats for a single CSV file."""
    df = pd.read_csv(path, low_memory=False)

    column_details: List[Dict[str, Any]] = []
    for col in df.columns:
        series = df[col]
        column_details.append(
            {
                "name": col,
                "dtype": str(series.dtype),
                "non_null": int(series.notna().sum()),
                "nulls": int(series.isna().sum()),
                "unique": int(series.nunique(dropna=True)),
                "examples": series.dropna().head(3).tolist(),
            }
        )

    return {
        "file": str(path),
        "rows": int(len(df)),
        "columns": len(df.columns),
        "memory_bytes": int(df.memory_usage(deep=True).sum()),
        "column_details": column_details,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--data-dir",
        default="formula1",
        help="Directory containing CSV files (default: formula1)",
    )
    parser.add_argument(
        "--out",
        default="schema_metadata.json",
        help="Where to write the JSON report (default: schema_metadata.json)",
    )
    parser.add_argument(
        "--pretty",
        action="store_true",
        help="Pretty-print JSON with indentation",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir).expanduser().resolve()
    csv_files = sorted(p for p in data_dir.rglob("*.csv") if p.is_file())

    results: List[Dict[str, Any]] = []
    for csv_path in csv_files:
        try:
            results.append(summarize_csv(csv_path))
        except Exception as exc:  # pragma: no cover - defensive logging
            results.append({"file": str(csv_path), "error": str(exc)})

    payload = {
        "data_dir": str(data_dir),
        "total_files": len(csv_files),
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "files": results,
    }

    out_path = Path(args.out)
    out_path.write_text(
        json.dumps(payload, indent=2 if args.pretty else None),
        encoding="utf-8",
    )
    print(f"Wrote schema for {len(csv_files)} files to {out_path}")


if __name__ == "__main__":
    main()
