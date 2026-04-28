#!/usr/bin/env python3
"""Booking sync worker — runs every 15 min, polls Channex booking revisions feed.

Works alongside webhooks as a safety net:
- Webhooks fire instantly for new/modified/cancelled bookings
- This poller catches anything webhooks missed (network issues, downtime, etc.)
- Deduplicates via revision_id in channex_webhook_log
"""

import os
import sys
import json
import logging
from datetime import datetime, date, timedelta
import httpx
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [bookings] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/staycommand/bookings.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
CHANNEX_URL = os.environ.get("CHANNEX_API_URL", "https://app.channex.io/api/v1")
CHANNEX_KEY = os.environ["CHANNEX_API_KEY"]


# ==================== Channex API helpers ====================

def channex_headers():
    return {"Content-Type": "application/json", "user-api-key": CHANNEX_KEY}


def channex_get(client: httpx.Client, endpoint: str):
    resp = client.get(f"{CHANNEX_URL}{endpoint}", headers=channex_headers())
    resp.raise_for_status()
    return resp.json()


def channex_post(client: httpx.Client, endpoint: str, body=None):
    resp = client.post(f"{CHANNEX_URL}{endpoint}", headers=channex_headers(), json=body or {})
    resp.raise_for_status()
    return resp.json()


def channex_put(client: httpx.Client, endpoint: str, body=None):
    resp = client.put(f"{CHANNEX_URL}{endpoint}", headers=channex_headers(), json=body or {})
    resp.raise_for_status()
    return resp.json()


# ==================== Availability helpers ====================

def build_avail_range(channex_property_id: str, room_type_id: str,
                      check_in: str, check_out: str, availability: int):
    """Build per-day availability entries for a date range."""
    values = []
    ci = datetime.strptime(check_in, "%Y-%m-%d")
    co = datetime.strptime(check_out, "%Y-%m-%d")
    d = ci
    while d < co:
        ds = d.strftime("%Y-%m-%d")
        values.append({
            "property_id": channex_property_id,
            "room_type_id": room_type_id,
            "date_from": ds,
            "date_to": ds,
            "availability": availability,
        })
        d += timedelta(days=1)
    return values


def update_availability(client: httpx.Client, channex_property_id: str,
                        action: str, old_ci: str, old_co: str,
                        new_ci: str, new_co: str):
    """Update Channex availability for all room types based on booking action."""
    try:
        room_types = channex_get(client, f"/room_types?filter[property_id]={channex_property_id}")
        rt_ids = [rt["id"] for rt in room_types.get("data", [])]
        if not rt_ids:
            log.warning(f"  No room types for property {channex_property_id}")
            return False

        avail_values = []
        for rt_id in rt_ids:
            if action == "cancelled" and old_ci and old_co:
                avail_values.extend(build_avail_range(channex_property_id, rt_id, old_ci, old_co, 1))
            elif action == "modified" and old_ci and old_co:
                avail_values.extend(build_avail_range(channex_property_id, rt_id, old_ci, old_co, 1))
                avail_values.extend(build_avail_range(channex_property_id, rt_id, new_ci, new_co, 0))
            elif action == "created":
                avail_values.extend(build_avail_range(channex_property_id, rt_id, new_ci, new_co, 0))

        if avail_values:
            channex_post(client, "/availability", {"values": avail_values})
            log.info(f"  Availability updated: {len(avail_values)} entries across {len(rt_ids)} room types")
            return True
    except Exception as e:
        log.warning(f"  Availability update failed: {e}")
    return False


# ==================== Main polling logic ====================

def main():
    log.info("=== Booking revision poller starting ===")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    client = httpx.Client(timeout=30)

    # Get sync state
    sync_state = supabase.table("channex_sync_state").select("*").eq("id", "default").execute()
    last_revision_id = None
    if sync_state.data:
        last_revision_id = sync_state.data[0].get("last_revision_id")
    log.info(f"Last processed revision: {last_revision_id or 'none (first run)'}")

    # Get all Channex-connected properties
    props = supabase.table("properties").select("id, channex_property_id").not_.is_("channex_property_id", "null").execute()
    channex_props = {p["channex_property_id"]: p["id"] for p in (props.data or [])}
    log.info(f"Tracking {len(channex_props)} Channex-connected properties")

    # Pull unacknowledged revisions from the feed
    try:
        feed = channex_get(client, "/booking_revisions/feed")
    except Exception as e:
        log.error(f"Failed to fetch revision feed: {e}")
        client.close()
        return

    revisions = feed.get("data", [])
    log.info(f"Found {len(revisions)} unacknowledged revisions")

    if not revisions:
        # Update poll timestamp even if nothing to process
        supabase.table("channex_sync_state").update({
            "last_polled_at": datetime.utcnow().isoformat(),
        }).eq("id", "default").execute()
        log.info("No revisions to process")
        client.close()

        # Still run secondary tasks
        sync_ical_feeds()
        record_outcomes(supabase)
        return

    processed = 0
    skipped_dup = 0
    errors = 0
    latest_rev_id = last_revision_id

    for rev in revisions:
        rev_id = rev["id"]
        attrs = rev.get("attributes", {})
        rels = rev.get("relationships", {})

        # Extract booking_id and property_id from relationships or attributes
        booking_id = None
        channex_property_id = None

        if isinstance(rels, dict):
            booking_rel = rels.get("booking", {})
            if isinstance(booking_rel, dict):
                booking_data = booking_rel.get("data", {})
                if isinstance(booking_data, dict):
                    booking_id = booking_data.get("id")
            prop_rel = rels.get("property", {})
            if isinstance(prop_rel, dict):
                prop_data = prop_rel.get("data", {})
                if isinstance(prop_data, dict):
                    channex_property_id = prop_data.get("id")

        # Fallback to attributes
        booking_id = booking_id or attrs.get("booking_id")
        channex_property_id = channex_property_id or attrs.get("property_id")
        status = attrs.get("status", "new")

        log.info(f"Revision {rev_id}: booking={booking_id}, property={channex_property_id}, status={status}")

        # Dedup: check if this revision was already processed via webhook
        try:
            existing_log = supabase.table("channex_webhook_log").select("id").eq(
                "revision_id", rev_id
            ).limit(1).execute()
            if existing_log.data:
                log.info(f"  Already processed (via webhook), acknowledging only")
                try:
                    channex_post(client, f"/booking_revisions/{rev_id}/ack")
                except Exception:
                    pass
                skipped_dup += 1
                latest_rev_id = rev_id
                continue
        except Exception:
            pass  # Table might not have the column yet, proceed anyway

        if not booking_id:
            log.warning(f"  No booking_id in revision, acknowledging and skipping")
            try:
                channex_post(client, f"/booking_revisions/{rev_id}/ack")
            except Exception:
                pass
            latest_rev_id = rev_id
            continue

        try:
            # Fetch full booking details from Channex
            booking = channex_get(client, f"/bookings/{booking_id}")
            ba = booking.get("data", {}).get("attributes", {})

            # Resolve property — try from revision, fall back to booking
            if not channex_property_id:
                channex_property_id = (
                    booking.get("data", {})
                    .get("relationships", {})
                    .get("property", {})
                    .get("data", {})
                    .get("id")
                )

            property_id = channex_props.get(channex_property_id)
            if not property_id:
                log.warning(f"  Property {channex_property_id} not in Supabase, acknowledging and skipping")
                channex_post(client, f"/booking_revisions/{rev_id}/ack")
                latest_rev_id = rev_id
                continue

            # Check for self-originated bookings (prevent loop)
            ota_code = ba.get("ota_reservation_code", "")
            if ota_code.startswith("SC-") and ba.get("ota_name") == "Offline":
                log.info(f"  Self-originated booking (ota_code={ota_code}), skipping")
                channex_post(client, f"/booking_revisions/{rev_id}/ack")
                try:
                    supabase.table("channex_webhook_log").insert({
                        "event_type": "revision_poll",
                        "booking_id": booking_id,
                        "revision_id": rev_id,
                        "channex_property_id": channex_property_id,
                        "action_taken": "skipped_self",
                        "ack_sent": True,
                        "ack_response": "self-originated",
                    }).execute()
                except Exception:
                    pass
                latest_rev_id = rev_id
                continue

            # Build guest name
            customer = ba.get("customer", {}) or {}
            guest_name = f"{customer.get('name', '')} {customer.get('surname', '')}".strip() or None

            # Map OTA to platform
            ota = (ba.get("ota_name") or "").lower()
            if "airbnb" in ota:
                platform = "airbnb"
            elif "vrbo" in ota or "homeaway" in ota:
                platform = "vrbo"
            elif "booking" in ota:
                platform = "booking_com"
            else:
                platform = "direct"

            # Determine action
            if status == "cancelled" or ba.get("status") == "cancelled":
                action = "cancelled"
                db_status = "cancelled"
            elif status == "modified" or ba.get("status") == "modified":
                action = "modified"
                db_status = "confirmed"
            else:
                action = "created"
                db_status = "confirmed"

            # Check for existing booking (needed for old dates on modify/cancel)
            existing = supabase.table("bookings").select("id, check_in, check_out").eq(
                "channex_booking_id", booking_id
            ).limit(1).execute()
            existing_booking = existing.data[0] if existing.data else None
            old_ci = existing_booking["check_in"] if existing_booking else None
            old_co = existing_booking["check_out"] if existing_booking else None

            booking_data = {
                "property_id": property_id,
                "platform": platform,
                "channex_booking_id": booking_id,
                "guest_name": guest_name,
                "guest_first_name": (customer.get("name") or "").strip() or None,
                "guest_last_name": (customer.get("surname") or "").strip() or None,
                "guest_email": customer.get("mail"),
                "guest_phone": customer.get("phone"),
                "check_in": ba.get("arrival_date"),
                "check_out": ba.get("departure_date"),
                "total_price": float(ba["amount"]) if ba.get("amount") else None,
                "currency": ba.get("currency", "USD"),
                "status": db_status,
                "platform_booking_id": ba.get("ota_reservation_code"),
                # Session 6.3 — separate column from platform_booking_id so
                # the read-side join is unambiguous and an iCal-source row
                # alongside (with platform_booking_id = ical UID) doesn't
                # collide. Reviews join on this.
                "ota_reservation_code": ba.get("ota_reservation_code"),
                "revision_number": ba.get("revision"),
                "source": "channex",
                "notes": ba.get("notes"),
            }

            # Upsert booking
            if existing_booking:
                supabase.table("bookings").update(booking_data).eq("id", existing_booking["id"]).execute()
                log.info(f"  Updated booking {booking_id} ({action})")
            else:
                supabase.table("bookings").insert(booking_data).execute()
                log.info(f"  Inserted booking {booking_id} ({action})")

            # Update Channex availability
            avail_updated = update_availability(
                client, channex_property_id, action,
                old_ci, old_co,
                ba.get("arrival_date"), ba.get("departure_date"),
            )

            # Acknowledge revision
            ack_sent = False
            ack_response = ""
            try:
                channex_post(client, f"/booking_revisions/{rev_id}/ack")
                ack_sent = True
                ack_response = "OK"
                log.info(f"  Acknowledged revision {rev_id}")
            except Exception as e:
                ack_response = str(e)
                log.warning(f"  Failed to acknowledge {rev_id}: {e}")

            # Log to channex_webhook_log (with revision_id for dedup)
            try:
                supabase.table("channex_webhook_log").insert({
                    "event_type": "revision_poll",
                    "booking_id": booking_id,
                    "revision_id": rev_id,
                    "channex_property_id": channex_property_id,
                    "guest_name": guest_name,
                    "check_in": ba.get("arrival_date"),
                    "check_out": ba.get("departure_date"),
                    "action_taken": action,
                    "ack_sent": ack_sent,
                    "ack_response": ack_response,
                }).execute()
            except Exception as e:
                log.debug(f"  Log insert error: {e}")

            processed += 1
            latest_rev_id = rev_id

        except Exception as e:
            log.error(f"  Error processing revision {rev_id}: {e}")
            errors += 1

    # Update sync state
    update_data = {"last_polled_at": datetime.utcnow().isoformat()}
    if latest_rev_id:
        update_data["last_revision_id"] = latest_rev_id
        update_data["revisions_processed"] = (
            (sync_state.data[0].get("revisions_processed") or 0) + processed
            if sync_state.data else processed
        )
    supabase.table("channex_sync_state").update(update_data).eq("id", "default").execute()

    log.info(f"=== Revision poll complete: {processed} processed, {skipped_dup} skipped (already via webhook), {errors} errors ===")

    # Secondary tasks
    sync_ical_feeds()
    record_outcomes(supabase)
    client.close()


# ==================== iCal sync ====================

def sync_ical_feeds():
    """Sync all active iCal feeds."""
    from db import get_connection
    from ical_parser import parse_ical
    import httpx as _httpx

    log.info("=== iCal sync starting ===")

    try:
        conn = get_connection()
        cur = conn.cursor()

        # Get all active feeds. Include the property's Channex id so we
        # can push availability=0 back to Channex for any new iCal
        # booking — this is the critical path that prevents overbookings
        # on properties that only have some channels connected via OAuth
        # and others via iCal (e.g., Modern House: Airbnb via Channex,
        # Booking.com via iCal while MFA is blocking OAuth).
        cur.execute(
            "SELECT f.id, f.property_id, f.platform, f.feed_url, p.name, "
            "p.channex_property_id "
            "FROM ical_feeds f JOIN properties p ON p.id = f.property_id "
            "WHERE f.is_active = true"
        )
        feeds = cur.fetchall()
        log.info(f"Found {len(feeds)} active iCal feeds")

        total_new = 0
        client = _httpx.Client(timeout=30)

        for feed in feeds:
            try:
                resp = client.get(feed["feed_url"], headers={"User-Agent": "StayCommand/1.0"})
                if resp.status_code != 200:
                    log.warning(f"  {feed['name']} ({feed['platform']}): HTTP {resp.status_code}")
                    cur.execute("UPDATE ical_feeds SET last_error = %s WHERE id = %s",
                                (f"HTTP {resp.status_code}", feed["id"]))
                    conn.commit()
                    continue

                entries = parse_ical(resp.text, feed["feed_url"])
                new_count = 0
                newly_inserted_dates = []  # list of (check_in, check_out) to push to Channex
                feed_uids = set()

                for entry in entries:
                    feed_uids.add(entry.uid)
                    if entry.is_blocked:
                        continue  # Skip explicit "blocked" entries (not real reservations)

                    # Check if booking exists
                    cur.execute(
                        "SELECT id FROM bookings WHERE property_id = %s AND platform_booking_id = %s",
                        (feed["property_id"], entry.uid)
                    )
                    existing = cur.fetchone()

                    if existing:
                        cur.execute(
                            "UPDATE bookings SET check_in = %s, check_out = %s, guest_name = %s WHERE id = %s",
                            (entry.check_in, entry.check_out, entry.guest_name, existing["id"])
                        )
                    else:
                        # Cross-source dedup: check if a row with no UID
                        # already exists for the same property+platform+dates
                        # (e.g. Channex webhook inserted it first without a
                        # platform_booking_id). Promote that row by stamping
                        # the iCal UID into it instead of creating a duplicate.
                        cur.execute(
                            "SELECT id FROM bookings WHERE property_id = %s AND check_in = %s "
                            "AND check_out = %s AND platform = %s AND status != 'cancelled' "
                            "AND platform_booking_id IS NULL",
                            (feed["property_id"], entry.check_in, entry.check_out, entry.platform)
                        )
                        dup = cur.fetchone()
                        if dup:
                            cur.execute(
                                "UPDATE bookings SET platform_booking_id = %s, guest_name = %s WHERE id = %s",
                                (entry.uid, entry.guest_name, dup["id"])
                            )
                            log.info(f"    Promoted Channex-sourced row for "
                                     f"{feed['property_id']} {entry.check_in}-{entry.check_out} with iCal UID")
                            continue

                        # RDX-3 — iCal feeds expose an email-UID UID
                        # (e.g. ...@airbnb.com), never an HM-code. Stamp
                        # ota_reservation_code as NULL explicitly so reviews-
                        # sync's HM-code join can't accidentally match this
                        # row via column-default oversight.
                        cur.execute(
                            "INSERT INTO bookings (property_id, platform, platform_booking_id, guest_name, "
                            "check_in, check_out, status, ota_reservation_code) VALUES (%s, %s, %s, %s, %s, %s, 'confirmed', NULL)",
                            (feed["property_id"], entry.platform, entry.uid, entry.guest_name,
                             entry.check_in, entry.check_out)
                        )
                        new_count += 1
                        newly_inserted_dates.append((entry.check_in, entry.check_out))

                conn.commit()

                # --- Cancel rows whose UID disappeared from the feed ---
                # Restricted to source='ical' rows. Channex-direct bookings
                # carry their HM-code in `platform_booking_id` too, but
                # post-OAuth Airbnb stops including OAuth-connected bookings
                # in its iCal feed export — without the source filter, every
                # iCal tick re-cancelled every Channex-direct booking
                # because its HM-code wasn't in `feed_uids`. (Diagnosed
                # 2026-04-28; Margot Castillo's HM9JXBCTHB was the worked
                # example: Channex revision_poll + booking_new webhook
                # stamped status='confirmed', then the next iCal tick 5 min
                # later flipped it to cancelled.)
                cur.execute(
                    "SELECT id, platform_booking_id, check_in, check_out, channex_booking_id "
                    "FROM bookings WHERE property_id = %s AND platform = %s "
                    "AND source = 'ical' "
                    "AND status = 'confirmed' AND platform_booking_id IS NOT NULL",
                    (feed["property_id"], feed["platform"])
                )
                cancellation_dates = []
                for row in cur.fetchall():
                    if row["platform_booking_id"] in feed_uids:
                        continue
                    cur.execute(
                        "UPDATE bookings SET status = 'cancelled' WHERE id = %s",
                        (row["id"],)
                    )
                    cancellation_dates.append((row["check_in"].isoformat() if hasattr(row["check_in"], "isoformat") else row["check_in"],
                                                row["check_out"].isoformat() if hasattr(row["check_out"], "isoformat") else row["check_out"]))
                conn.commit()

                # --- CRITICAL: push availability back to Channex ---
                # For every newly inserted iCal booking, push availability=0
                # for the booked dates so every OTHER channel (Airbnb, Vrbo)
                # that Channex manages for this property blocks those dates
                # automatically. Same in reverse for cancellations.
                # Without this, iCal bookings land in Moora but never block
                # the sibling OTAs → guaranteed overbookings.
                channex_property_id = feed.get("channex_property_id")
                if channex_property_id and (newly_inserted_dates or cancellation_dates):
                    try:
                        for ci, co in newly_inserted_dates:
                            update_availability(
                                client, channex_property_id, "created",
                                old_ci=None, old_co=None,
                                new_ci=ci if isinstance(ci, str) else ci.isoformat(),
                                new_co=co if isinstance(co, str) else co.isoformat(),
                            )
                        for ci, co in cancellation_dates:
                            update_availability(
                                client, channex_property_id, "cancelled",
                                old_ci=ci, old_co=co,
                                new_ci=None, new_co=None,
                            )
                        log.info(f"  Pushed {len(newly_inserted_dates)} new + "
                                 f"{len(cancellation_dates)} cancelled availability updates to Channex for {feed['name']}")
                    except Exception as e:
                        log.warning(f"  Channex availability push failed for {feed['name']}: {e}")

                # Update feed status
                cur.execute(
                    "UPDATE ical_feeds SET last_synced = NOW(), last_error = NULL, "
                    "sync_count = sync_count + 1 WHERE id = %s",
                    (feed["id"],)
                )
                conn.commit()

                bookings_count = len([e for e in entries if not e.is_blocked])
                blocked_count = len([e for e in entries if e.is_blocked])
                total_new += new_count
                log.info(f"  {feed['name']} ({feed['platform']}): {bookings_count} bookings, "
                         f"{blocked_count} blocked, {new_count} new")

            except Exception as e:
                log.error(f"  Feed error ({feed['name']}): {e}")
                cur.execute("UPDATE ical_feeds SET last_error = %s WHERE id = %s", (str(e), feed["id"]))
                conn.commit()

        client.close()
        cur.close()
        conn.close()
        log.info(f"=== iCal sync complete: {total_new} new bookings ===")

    except Exception as e:
        log.error(f"iCal sync error: {e}")


# ==================== Pricing outcomes ====================

def record_outcomes(supabase):
    """Record pricing outcomes for dates that have passed."""
    log.info("=== Recording pricing outcomes ===")
    yesterday = (date.today() - timedelta(days=1)).isoformat()
    week_ago = (date.today() - timedelta(days=7)).isoformat()

    # Get all properties
    props = supabase.table("properties").select("id").execute().data or []
    outcomes_recorded = 0

    for prop in props:
        pid = prop["id"]

        # Get calendar_rates for the past 7 days
        rates = (
            supabase.table("calendar_rates")
            .select("date, applied_rate, suggested_rate, rate_source, factors")
            .eq("property_id", pid)
            .gte("date", week_ago)
            .lte("date", yesterday)
            .execute()
        ).data or []

        # Get bookings spanning these dates
        bookings = (
            supabase.table("bookings")
            .select("id, check_in, check_out, total_price, created_at")
            .eq("property_id", pid)
            .gte("check_out", week_ago)
            .lte("check_in", yesterday)
            .neq("status", "cancelled")
            .execute()
        ).data or []

        # Get market snapshot
        snap = (
            supabase.table("market_snapshots")
            .select("market_adr, market_occupancy, market_demand_score")
            .eq("property_id", pid)
            .order("snapshot_date", desc=True)
            .limit(1)
            .execute()
        ).data or []
        market = snap[0] if snap else {}

        # Get comp median
        comps = (
            supabase.table("market_comps")
            .select("comp_adr")
            .eq("property_id", pid)
            .execute()
        ).data or []
        comp_adrs = sorted([c["comp_adr"] for c in comps if c.get("comp_adr")])
        comp_median = comp_adrs[len(comp_adrs) // 2] if comp_adrs else None

        for rate in rates:
            rate_date = rate["date"]

            # Check if already recorded
            existing = (
                supabase.table("pricing_outcomes")
                .select("id")
                .eq("property_id", pid)
                .eq("date", rate_date)
                .limit(1)
                .execute()
            ).data or []
            if existing:
                continue

            # Check if this date was booked
            was_booked = False
            booking_id = None
            actual_revenue = None
            booked_at = None
            days_before = None

            for b in bookings:
                if b["check_in"] <= rate_date < b["check_out"]:
                    was_booked = True
                    booking_id = b["id"]
                    nights = (date.fromisoformat(b["check_out"]) - date.fromisoformat(b["check_in"])).days
                    actual_revenue = float(b["total_price"]) / nights if b.get("total_price") and nights > 0 else None
                    booked_at = b.get("created_at")
                    if booked_at:
                        days_before = (date.fromisoformat(rate_date) - date.fromisoformat(booked_at[:10])).days
                    break

            suggested = float(rate.get("suggested_rate") or 0)
            applied = float(rate.get("applied_rate") or 0)
            rev_vs = (actual_revenue - suggested) if actual_revenue and suggested else None

            outcome = {
                "property_id": pid,
                "date": rate_date,
                "suggested_rate": suggested or None,
                "applied_rate": applied or None,
                "rate_source": rate.get("rate_source"),
                "was_booked": was_booked,
                "booking_id": booking_id,
                "actual_revenue": actual_revenue,
                "booked_at": booked_at,
                "days_before_checkin": days_before,
                "market_adr": market.get("market_adr"),
                "market_occupancy": market.get("market_occupancy"),
                "demand_score": market.get("market_demand_score"),
                "comp_median_adr": comp_median,
                "signals": rate.get("factors"),
                "revenue_vs_suggested": rev_vs,
            }

            try:
                supabase.table("pricing_outcomes").insert(outcome).execute()
                outcomes_recorded += 1
            except Exception as e:
                log.debug(f"  Outcome insert error for {pid}/{rate_date}: {e}")

    log.info(f"  Recorded {outcomes_recorded} pricing outcomes")


if __name__ == "__main__":
    main()
