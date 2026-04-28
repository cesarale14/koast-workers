#!/usr/bin/env python3
"""Pricing worker — runs every 6 hours, calculates rates for all properties."""

import os
import sys
import time
import logging
import httpx
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [pricing] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/koast/pricing.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
API_URL = os.environ.get("KOAST_API_URL", "https://app.koasthq.com")
SERVICE_HEADERS = {"x-service-key": SUPABASE_KEY}


def main():
    log.info("=== Pricing worker starting ===")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Fetch all properties
    res = supabase.table("properties").select("id, name").execute()
    properties = res.data or []
    log.info(f"Found {len(properties)} properties")

    if not properties:
        log.info("No properties to process")
        return

    client = httpx.Client(timeout=120)
    success = 0
    errors = 0

    for i, prop in enumerate(properties):
        prop_id = prop["id"]
        prop_name = prop["name"]
        log.info(f"[{i+1}/{len(properties)}] Processing: {prop_name} ({prop_id})")

        try:
            # Calculate rates
            resp = client.post(
                f"{API_URL}/api/pricing/calculate/{prop_id}",
                json={"days": 90, "pricing_mode": "review"},
                headers=SERVICE_HEADERS,
            )
            resp.raise_for_status()
            data = resp.json()

            rate_range = data.get("rate_range", {})
            log.info(
                f"  Calculated: {data.get('dates_calculated', 0)} dates, "
                f"range ${rate_range.get('min', 0)}-${rate_range.get('max', 0)}, "
                f"avg ${rate_range.get('avg', 0)}"
            )

            # TODO: Check pricing_mode and auto-push if 'auto'
            # For now, just calculate — push requires manual approval
            success += 1

        except Exception as e:
            log.error(f"  Error: {e}")
            errors += 1

        # Stagger between properties
        if i < len(properties) - 1:
            time.sleep(5)

    log.info(f"=== Pricing worker complete: {success} success, {errors} errors ===")
    client.close()


if __name__ == "__main__":
    main()
