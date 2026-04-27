#!/usr/bin/env python3
"""
Koast pricing engine validator.

Runs daily at 6 AM ET via systemd timer. For every Channex-connected
property, this worker:

  1. Triggers the 9-signal pricing engine via the existing
     /api/pricing/calculate/{property_id} endpoint — reuses the signal
     math rather than re-implementing it in Python. The endpoint writes
     suggested_rate + factors back to calendar_rates.
  2. Fetches the LIVE Airbnb rate for the next 60 days from Channex by
     looking up the property's Airbnb rate plan id (persisted in
     property_channels.settings.rate_plan_id by the per-channel rate
     editor's auto-discovery) and hitting /restrictions.
  3. Writes one pricing_recommendations row per (property, date) capturing:
       - current_rate  = live Airbnb rate right now
       - suggested_rate = Koast's 9-signal output
       - reason_signals = the factors JSON from calendar_rates
  4. Prints a per-property comparison summary: how many dates Koast
     suggests higher vs lower and by how much on average. This is the
     evidence that the engine works (or doesn't).

The validator purposely does NOT push rates to any channel — it's a
read-only observability feedback loop.
"""

import os
import sys
import time
import logging
from datetime import datetime, timedelta

import httpx
import psycopg2
from psycopg2.extras import RealDictCursor, Json
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [pricing-validator] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/staycommand/pricing-validator.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

DATABASE_URL = os.environ["DATABASE_URL"]
CHANNEX_URL = os.environ.get("CHANNEX_API_URL", "https://app.channex.io/api/v1")
CHANNEX_KEY = os.environ["CHANNEX_API_KEY"]
API_URL = os.environ.get("STAYCOMMAND_API_URL", "https://staycommand.vercel.app")
SERVICE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

FORECAST_DAYS = 60


def get_db():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)


def channex_headers():
    return {"Content-Type": "application/json", "user-api-key": CHANNEX_KEY}


def fetch_airbnb_live_rates(client: httpx.Client, channex_property_id: str,
                            rate_plan_id: str, date_from: str, date_to: str):
    """
    Returns a dict { "YYYY-MM-DD": rate_float_dollars } for the given
    rate plan using Channex's bucketed restrictions endpoint. Zero and
    null rates are skipped (Channex returns "0.00" for dates with no
    pushed rate — that's not a real price).
    """
    url = (
        f"{CHANNEX_URL}/restrictions"
        f"?filter[property_id]={channex_property_id}"
        f"&filter[rate_plan_id]={rate_plan_id}"
        f"&filter[date][gte]={date_from}"
        f"&filter[date][lte]={date_to}"
        f"&filter[restrictions]=rate"
    )
    resp = client.get(url, headers=channex_headers(), timeout=30)
    resp.raise_for_status()
    data = resp.json()
    bucket = (data.get("data") or {}).get(rate_plan_id) or {}
    out = {}
    for date_str, entry in bucket.items():
        try:
            rate = float(entry.get("rate") or 0)
        except (TypeError, ValueError):
            continue
        if rate > 0:
            out[date_str] = rate
    return out


def trigger_pricing_calculate(client: httpx.Client, property_id: str):
    """
    POST to /api/pricing/calculate/{id} so the 9-signal engine runs and
    writes suggested_rate + factors into calendar_rates. Returns the
    API response dict on success.
    """
    url = f"{API_URL}/api/pricing/calculate/{property_id}"
    resp = client.post(
        url,
        headers={"Content-Type": "application/json", "x-service-key": SERVICE_KEY},
        json={"days": FORECAST_DAYS, "pricing_mode": "review"},
        timeout=180,
    )
    resp.raise_for_status()
    return resp.json()


def read_engine_output(cur, property_id: str, date_from: str, date_to: str):
    """
    After the pricing engine has run, read back the suggested_rate +
    factors from calendar_rates for the base (channel_code IS NULL) rows
    in the forecast window.
    """
    cur.execute(
        """
        SELECT date::text AS date, suggested_rate, factors
        FROM calendar_rates
        WHERE property_id = %s
          AND channel_code IS NULL
          AND date >= %s AND date <= %s
          AND suggested_rate IS NOT NULL
        """,
        (property_id, date_from, date_to),
    )
    return {row["date"]: row for row in cur.fetchall()}


def log_recommendations(cur, property_id: str,
                        engine_rows: dict, live_rates: dict) -> int:
    """
    Upsert one pricing_recommendations row per date we have data for.
    engine_rows is { date: { suggested_rate, factors } }, live_rates is
    { date: current_rate }. A date must appear in BOTH to be logged —
    otherwise we don't have a complete comparison.

    Dedup contract: the pricing_recs_unique_pending_per_date partial
    unique index (migration 20260419000000) allows at most one pending
    row per (property_id, date). We use ON CONFLICT WHERE status='pending'
    inference to update the existing pending row on re-runs instead of
    creating a duplicate.
    """
    upserted = 0
    for date_str, live_rate in live_rates.items():
        engine_row = engine_rows.get(date_str)
        if not engine_row or engine_row["suggested_rate"] is None:
            continue
        cur.execute(
            """
            INSERT INTO pricing_recommendations
              (property_id, date, current_rate, suggested_rate, reason_signals, status)
            VALUES (%s, %s, %s, %s, %s, 'pending')
            ON CONFLICT (property_id, date) WHERE status = 'pending'
            DO UPDATE SET
              current_rate   = EXCLUDED.current_rate,
              suggested_rate = EXCLUDED.suggested_rate,
              reason_signals = EXCLUDED.reason_signals,
              created_at     = now()
            """,
            (
                property_id,
                date_str,
                live_rate,
                float(engine_row["suggested_rate"]),
                Json(engine_row["factors"]) if engine_row["factors"] else None,
            ),
        )
        upserted += 1
    return upserted


def summarize(cur, property_id: str, property_name: str):
    """
    Read back this run's recommendations for a property and print the
    per-property comparison: how many dates higher/lower, average delta.
    """
    cur.execute(
        """
        SELECT current_rate, suggested_rate, delta_abs, delta_pct
        FROM pricing_recommendations
        WHERE property_id = %s
          AND created_at >= NOW() - INTERVAL '1 hour'
        """,
        (property_id,),
    )
    rows = cur.fetchall()
    if not rows:
        log.info(f"  [{property_name}] no comparable dates")
        return

    total = len(rows)
    higher = sum(1 for r in rows if r["delta_abs"] is not None and float(r["delta_abs"]) > 0.5)
    lower = sum(1 for r in rows if r["delta_abs"] is not None and float(r["delta_abs"]) < -0.5)
    equal = total - higher - lower
    non_null_delta = [float(r["delta_abs"]) for r in rows if r["delta_abs"] is not None]
    non_null_pct = [float(r["delta_pct"]) for r in rows if r["delta_pct"] is not None]
    avg_delta = sum(non_null_delta) / len(non_null_delta) if non_null_delta else 0.0
    avg_pct = sum(non_null_pct) / len(non_null_pct) if non_null_pct else 0.0

    log.info(
        f"  [{property_name}] {total} dates | "
        f"Koast higher on {higher} ({higher*100//total}%), "
        f"lower on {lower} ({lower*100//total}%), "
        f"equal on {equal} | "
        f"avg delta ${avg_delta:+.2f} ({avg_pct:+.2f}%)"
    )


def main():
    log.info("=== Pricing validator starting ===")
    run_started = datetime.utcnow()

    # Forecast window: next FORECAST_DAYS days starting tomorrow (today is
    # typically already booked/locked on BDC so comparing makes less sense)
    today = datetime.utcnow().date()
    date_from = (today + timedelta(days=1)).isoformat()
    date_to = (today + timedelta(days=FORECAST_DAYS)).isoformat()
    log.info(f"Forecast window: {date_from} -> {date_to}")

    client = httpx.Client(timeout=60)

    with get_db() as conn:
        with conn.cursor() as cur:
            # Find every property that has both:
            #   - a Channex property id
            #   - an Airbnb (ABB) property_channels row with rate_plan_id in settings
            cur.execute(
                """
                SELECT p.id, p.name, p.channex_property_id,
                       pc.settings->>'rate_plan_id' AS airbnb_rate_plan_id
                FROM properties p
                JOIN property_channels pc ON pc.property_id = p.id
                WHERE p.channex_property_id IS NOT NULL
                  AND pc.channel_code = 'ABB'
                  AND pc.settings->>'rate_plan_id' IS NOT NULL
                ORDER BY p.name
                """
            )
            targets = cur.fetchall()
            log.info(f"Tracking {len(targets)} properties")

            if not targets:
                log.info("No eligible properties — exiting")
                client.close()
                return

            for i, target in enumerate(targets):
                prop_id = target["id"]
                prop_name = target["name"]
                channex_prop_id = target["channex_property_id"]
                rate_plan_id = target["airbnb_rate_plan_id"]

                log.info(f"[{i+1}/{len(targets)}] {prop_name} ({prop_id})")

                try:
                    log.info("  Running pricing engine...")
                    calc = trigger_pricing_calculate(client, prop_id)
                    rate_range = calc.get("rate_range", {}) or {}
                    log.info(
                        f"    calculated {calc.get('dates_calculated')} dates, "
                        f"range ${rate_range.get('min')}-${rate_range.get('max')}"
                    )
                except Exception as e:
                    log.error(f"    engine call failed: {e}")
                    continue

                try:
                    log.info(f"  Fetching live Airbnb rates from rate plan {rate_plan_id[:8]}...")
                    live_rates = fetch_airbnb_live_rates(
                        client, channex_prop_id, rate_plan_id, date_from, date_to
                    )
                    log.info(f"    got {len(live_rates)} live rates from Channex")
                except Exception as e:
                    log.error(f"    Channex fetch failed: {e}")
                    continue

                try:
                    engine_rows = read_engine_output(cur, prop_id, date_from, date_to)
                    log.info(f"    engine wrote {len(engine_rows)} suggested rates to calendar_rates")
                    inserted = log_recommendations(cur, prop_id, engine_rows, live_rates)
                    conn.commit()
                    log.info(f"    logged {inserted} pricing_recommendations rows")
                except Exception as e:
                    log.error(f"    recommendation write failed: {e}")
                    conn.rollback()
                    continue

                summarize(cur, prop_id, prop_name)

                if i < len(targets) - 1:
                    time.sleep(3)

    client.close()
    elapsed = (datetime.utcnow() - run_started).total_seconds()
    log.info(f"=== Pricing validator complete in {elapsed:.1f}s ===")


if __name__ == "__main__":
    main()
