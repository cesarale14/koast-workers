#!/usr/bin/env python3
"""
Pricing performance reconciler.

Runs nightly at 02:30 UTC (after market_sync at 02:00, before the 10:00
validator). Catches two gap cases the Channex booking_new webhook
misses:

  1. Webhook delivery failed (Channex retried-and-gave-up, or our route
     500'd on a transient DB error) — the booking landed in `bookings`
     via a subsequent revision poll but pricing_performance never got
     the outcome backfilled.

  2. iCal-sourced bookings that never triggered a Channex webhook at
     all — they're in `bookings` with platform='airbnb_ical' (or
     similar) but have no pricing_performance trace.

Logic:
  - Scan bookings created in the last 48h.
  - For each booking, walk the stay dates [check_in .. check_out-1].
  - For each date, find a matching pricing_performance row with
    booked=false. If present, backfill (booked=true, actual_rate,
    booked_at). If no row exists at all for that date, skip — the host
    never applied a recommendation for that date so there's nothing to
    track.
  - Per-night rate = total_price / nights if total_price present.

Usage:
  python3 pricing_performance_reconciler.py [--dry-run]

Systemd:
  See koast-pricing-performance-reconciler.service + .timer alongside
  pricing_validator in /etc/systemd/system/.

VERIFY manually:
  sudo systemctl start koast-pricing-performance-reconciler
  journalctl -u koast-pricing-performance-reconciler.service -n 50
  # Then in DB:
  #   SELECT id, property_id, date, booked, actual_rate, booked_at
  #     FROM pricing_performance
  #     WHERE booked_at > now() - interval '5 minutes';
"""

import os
import sys
import logging
import argparse
from datetime import datetime, timedelta, timezone

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [pricing-perf-reconciler] %(levelname)s %(message)s",
)
log = logging.getLogger(__name__)


def main(dry_run: bool = False) -> None:
    log.info(f"=== Pricing performance reconciler starting (dry_run={dry_run}) ===")
    conn = psycopg2.connect(os.environ["DATABASE_URL"])
    conn.autocommit = False
    cur = conn.cursor(cursor_factory=RealDictCursor)

    window_start = datetime.now(timezone.utc) - timedelta(hours=48)
    cur.execute(
        """
        SELECT id, property_id, check_in, check_out, total_price, platform
        FROM bookings
        WHERE created_at >= %s
          AND status IN ('confirmed', 'pending')
          AND check_in IS NOT NULL AND check_out IS NOT NULL
        """,
        (window_start,),
    )
    bookings = cur.fetchall()
    log.info(f"Found {len(bookings)} bookings in last 48h")

    backfilled = 0
    skipped_no_perf_row = 0
    skipped_already_booked = 0
    skipped_no_rate = 0
    for b in bookings:
        check_in = b["check_in"]
        check_out = b["check_out"]
        if not isinstance(check_in, (datetime, type(check_in))):
            continue
        nights = (check_out - check_in).days
        if nights <= 0:
            continue
        total = float(b["total_price"]) if b["total_price"] is not None else None
        nightly = round(total / nights, 2) if total and total > 0 else None
        if nightly is None or nightly <= 0:
            skipped_no_rate += 1
            log.info(f"  booking {b['id']}: no valid rate ({total=}, {nights=}) — skip")
            continue

        dates = [check_in + timedelta(days=i) for i in range(nights)]
        date_strs = [d.isoformat() for d in dates]

        # Query existing performance rows for these dates.
        cur.execute(
            """
            SELECT id, date, booked, actual_rate
            FROM pricing_performance
            WHERE property_id = %s AND date = ANY(%s)
            """,
            (b["property_id"], date_strs),
        )
        perf_rows = cur.fetchall()
        perf_by_date = {r["date"].isoformat() if hasattr(r["date"], "isoformat") else str(r["date"]): r for r in perf_rows}

        for ds in date_strs:
            row = perf_by_date.get(ds)
            if row is None:
                skipped_no_perf_row += 1
                continue
            if row["booked"]:
                skipped_already_booked += 1
                continue
            if dry_run:
                log.info(
                    f"  DRY-RUN would backfill perf row {row['id']} "
                    f"(prop={b['property_id']} date={ds} rate=${nightly})"
                )
                backfilled += 1
                continue
            cur.execute(
                """
                UPDATE pricing_performance
                SET booked = true,
                    actual_rate = %s,
                    booked_at = now()
                WHERE id = %s
                """,
                (nightly, row["id"]),
            )
            backfilled += 1

    if not dry_run:
        conn.commit()

    log.info(
        f"=== Reconciler complete: backfilled={backfilled} "
        f"skip_no_perf_row={skipped_no_perf_row} "
        f"skip_already_booked={skipped_already_booked} "
        f"skip_no_rate={skipped_no_rate} "
        f"(dry_run={dry_run}) ==="
    )
    cur.close()
    conn.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true", help="Scan but do not update.")
    args = parser.parse_args()
    try:
        main(dry_run=args.dry_run)
    except Exception as e:
        log.exception("reconciler failed")
        sys.exit(1)
