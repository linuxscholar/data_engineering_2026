#!/usr/bin/env python3
"""
Ingest NYC Yellow Taxi CSV data into Postgres in chunks (Pandas -> SQLAlchemy).

Example:
  python ingest_yellow_taxi.py \
    --url "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz" \
    --db-url "postgresql+psycopg://root:root@localhost:5432/ny_taxi" \
    --table yellow_taxi_data \
    --read-chunksize 100000 \
    --write-chunksize 2000 \
    --expected-chunks 13 \
    --drop-first

Requirements:
  pip install pandas sqlalchemy "psycopg[binary,pool]" tqdm
"""

from __future__ import annotations

import argparse
import sys
import time
from typing import Dict, List, Optional

import pandas as pd
from sqlalchemy import create_engine, text
from tqdm import tqdm


# ---- Schema (matches the Zoomcamp example) ----
DTYPE: Dict[str, str] = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "congestion_surcharge": "float64",
}

PARSE_DATES: List[str] = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
]


def parse_args(argv: Optional[List[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Ingest NYC taxi CSV into Postgres.")
    p.add_argument(
        "--url",
        required=True,
        help="CSV URL or local path (supports .csv.gz).",
    )
    p.add_argument(
        "--db-url",
        required=True,
        help="SQLAlchemy database URL, e.g. postgresql+psycopg://user:pass@host:5432/db",
    )
    p.add_argument("--table", default="yellow_taxi_data", help="Destination table name.")
    p.add_argument("--schema", default="public", help="Destination schema name.")
    p.add_argument("--read-chunksize", type=int, default=100_000, help="Rows per read chunk.")
    p.add_argument(
        "--write-chunksize",
        type=int,
        default=2000,
        help="Rows per INSERT batch when method='multi'. Keep small to avoid 65k parameter limit.",
    )
    p.add_argument(
        "--method",
        default="multi",
        choices=["multi", "none"],
        help="Pandas to_sql method. 'multi' is faster but needs safe write-chunksize. 'none' uses executemany.",
    )
    p.add_argument(
        "--expected-chunks",
        type=int,
        default=None,
        help="Optional: total chunks for tqdm (nice progress bar).",
    )
    p.add_argument(
        "--drop-first",
        action="store_true",
        help="Drop the destination table before loading (fresh start).",
    )
    p.add_argument(
        "--sample-rows",
        type=int,
        default=0,
        help="Optional: read a small sample (n rows) first to validate parsing; 0 disables.",
    )
    return p.parse_args(argv)


def main(argv: Optional[List[str]] = None) -> int:
    args = parse_args(argv)

    method = None if args.method == "none" else "multi"
    full_table = f"{args.schema}.{args.table}"

    print(f"[config] source: {args.url}")
    print(f"[config] db:     {args.db_url}")
    print(f"[config] table:  {full_table}")
    print(f"[config] read_chunksize={args.read_chunksize} write_chunksize={args.write_chunksize} method={args.method}")

    engine = create_engine(args.db_url)

    if args.sample_rows and args.sample_rows > 0:
        print(f"[sample] reading {args.sample_rows} rows to validate schema...")
        df_sample = pd.read_csv(
            args.url,
            nrows=args.sample_rows,
            dtype=DTYPE,
            parse_dates=PARSE_DATES,
        )
        print("[sample] ok. columns:", list(df_sample.columns))
        print("[sample] dtypes:\n", df_sample.dtypes)

    if args.drop_first:
        print(f"[db] dropping table if exists: {full_table}")
        with engine.begin() as conn:
            conn.execute(text(f"DROP TABLE IF EXISTS {full_table};"))

    print("[ingest] creating chunk iterator...")
    df_iter = pd.read_csv(
        args.url,
        chunksize=args.read_chunksize,
        dtype=DTYPE,
        parse_dates=PARSE_DATES,
    )

    rows_loaded = 0
    chunks_loaded = 0
    first = True

    t0 = time.time()
    for df_chunk in tqdm(df_iter, total=args.expected_chunks):
        if df_chunk is None or df_chunk.empty:
            continue

        chunks_loaded += 1
        rows_loaded += len(df_chunk)

        df_chunk.to_sql(
            name=args.table,
            con=engine,
            schema=args.schema,
            if_exists="replace" if first else "append",
            index=False,
            method=method,
            chunksize=args.write_chunksize if method == "multi" else None,
        )
        first = False

    elapsed = time.time() - t0
    print(f"[ingest] done. chunks={chunks_loaded} rows={rows_loaded} time={elapsed:.1f}s")

    # Verify count
    with engine.connect() as conn:
        count = conn.execute(text(f"SELECT COUNT(*) FROM {full_table};")).scalar()
    print(f"[verify] rows in {full_table}: {count}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
