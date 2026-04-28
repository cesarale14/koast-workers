#!/usr/bin/env python3
"""Market sync worker — runs daily at 2 AM, refreshes market data from AirROI."""

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
    format="%(asctime)s [market] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/koast/market.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
API_URL = os.environ.get("KOAST_API_URL", "https://app.koasthq.com")
SERVICE_HEADERS = {"x-service-key": SUPABASE_KEY}


def main():
    log.info("=== Market sync starting ===")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

    # Fetch properties with lat/lng
    res = supabase.table("properties").select("id, name, latitude, longitude").execute()
    properties = [p for p in (res.data or []) if p.get("latitude") and p.get("longitude")]
    log.info(f"Found {len(properties)} properties with location data")

    if not properties:
        log.info("No properties to process")
        return

    client = httpx.Client(timeout=120)
    total_cost = 0.0
    success = 0
    errors = 0

    for i, prop in enumerate(properties):
        prop_id = prop["id"]
        prop_name = prop["name"]
        log.info(f"[{i+1}/{len(properties)}] Refreshing: {prop_name} ({prop_id})")

        try:
            resp = client.post(f"{API_URL}/api/market/refresh/{prop_id}", headers=SERVICE_HEADERS)
            resp.raise_for_status()
            data = resp.json()

            snapshot = data.get("snapshot", {})
            comp_set = data.get("compSet", {})
            api_usage = data.get("api_usage", {})

            log.info(
                f"  Market: ADR=${snapshot.get('market_adr', 0)}, "
                f"Occ={snapshot.get('market_occupancy', 0)}%, "
                f"Demand={snapshot.get('market_demand_score', 0)}/100"
            )
            if comp_set:
                log.info(f"  Comps: {comp_set.get('total_comps', 0)} found")

            cost = api_usage.get("cost", 0)
            total_cost += cost
            success += 1

        except Exception as e:
            log.error(f"  Error: {e}")
            errors += 1

        # Stagger between properties
        if i < len(properties) - 1:
            time.sleep(5)

    log.info(
        f"=== Market sync complete: {success} success, {errors} errors, "
        f"API cost: ${total_cost:.2f} ==="
    )
    client.close()

    # Sync events from Ticketmaster
    sync_events(properties)


def sync_events(properties):
    """Sync local events from Ticketmaster for each property."""
    import os

    tm_key = os.environ.get("TICKETMASTER_API_KEY")
    if not tm_key:
        log.info("TICKETMASTER_API_KEY not set, skipping event sync")
        return

    from datetime import date, timedelta

    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    start = date.today().isoformat()
    end = (date.today() + timedelta(days=90)).isoformat()

    log.info("=== Event sync starting ===")
    total_events = 0

    for prop in properties:
        pid = prop["id"]
        lat = prop.get("latitude")
        lng = prop.get("longitude")
        if not lat or not lng:
            continue

        try:
            url = (
                f"https://app.ticketmaster.com/discovery/v2/events.json"
                f"?apikey={tm_key}&latlong={lat},{lng}&radius=15&unit=miles"
                f"&startDateTime={start}T00:00:00Z&endDateTime={end}T23:59:59Z"
                f"&size=200&sort=date,asc"
            )
            resp = httpx.get(url, timeout=30)
            if resp.status_code != 200:
                log.warning(f"  Ticketmaster error for {prop['name']}: {resp.status_code}")
                continue

            data = resp.json()
            events = data.get("_embedded", {}).get("events", [])

            # Delete old events for this property in range
            supabase.table("local_events").delete().eq("property_id", pid).gte("event_date", start).lte("event_date", end).execute()

            # Insert new events
            rows = []
            for e in events:
                segment = ""
                if e.get("classifications"):
                    segment = e["classifications"][0].get("segment", {}).get("name", "").lower()

                impact = 0.2
                attendance = 3000
                event_type = "other"
                name = e.get("name", "").lower()

                if "sport" in segment or "football" in name or "hockey" in name:
                    event_type = "sports"
                    impact = 0.5
                    attendance = 15000
                elif "music" in segment:
                    event_type = "music"
                    impact = 0.4
                    attendance = 8000
                    if "festival" in name:
                        event_type = "festival"
                        impact = 0.7
                        attendance = 30000
                elif "conference" in name or "convention" in name:
                    event_type = "conference"
                    impact = 0.5
                    attendance = 10000

                venue = (e.get("_embedded", {}).get("venues", [{}])[0] or {}).get("name")

                rows.append({
                    "property_id": pid,
                    "event_name": e.get("name", "Unknown"),
                    "event_date": e.get("dates", {}).get("start", {}).get("localDate"),
                    "venue_name": venue,
                    "event_type": event_type,
                    "estimated_attendance": attendance,
                    "demand_impact": impact,
                    "source": "ticketmaster",
                    "raw_data": {"segment": segment},
                })

            if rows:
                # Batch insert
                for i in range(0, len(rows), 50):
                    supabase.table("local_events").insert(rows[i:i+50]).execute()

            total_events += len(rows)
            log.info(f"  {prop['name']}: {len(rows)} events synced")

        except Exception as e:
            log.error(f"  Event sync error for {prop['name']}: {e}")

    log.info(f"=== Event sync complete: {total_events} total events ===")


if __name__ == "__main__":
    main()
