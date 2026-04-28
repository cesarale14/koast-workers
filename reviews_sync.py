#!/usr/bin/env python3
"""Reviews sync worker — runs every 20 min, polls Channex /reviews per property.

Channex's /reviews endpoint has no incremental delta and silently caps
pagination at ~10 entries per page (channex-expert known-quirks #6),
so each run pulls the first page per property and dedupes against the
existing guest_reviews rows by channex_review_id.

Session 6.6. Mirrors src/app/api/reviews/sync/route.ts upsert and
booking-resolution shape; the route stays as the manual-trigger path
behind the "Refresh now" UI button.
"""

import os
import sys
import logging
from datetime import datetime
import httpx
from dotenv import load_dotenv
from supabase import create_client

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [reviews] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/koast/reviews.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
CHANNEX_URL = os.environ.get("CHANNEX_API_URL", "https://app.channex.io/api/v1")
CHANNEX_KEY = os.environ["CHANNEX_API_KEY"]


# ==================== Channex helpers ====================

def channex_headers():
    return {"Content-Type": "application/json", "user-api-key": CHANNEX_KEY}


def fetch_reviews(client: httpx.Client, channex_property_id: str):
    """Pull all reviews Channex will return for a property.

    Channex caps pagination at ~10 per page regardless of page[limit]
    and ignores page[number] beyond the first batch. The dedup-by-id
    loop terminates as soon as a page adds zero new entries — same
    pattern the route uses (src/app/api/reviews/sync/route.ts).
    """
    seen = set()
    out = []
    page = 1
    while page <= 50:
        url = (
            f"{CHANNEX_URL}/reviews?filter[property_id]={channex_property_id}"
            f"&page[limit]=100&page[number]={page}"
        )
        resp = client.get(url, headers=channex_headers())
        resp.raise_for_status()
        batch = (resp.json() or {}).get("data", [])
        if not batch:
            break
        before = len(seen)
        for entry in batch:
            attrs = entry.get("attributes") or {}
            rid = attrs.get("id") or entry.get("id")
            if rid and rid not in seen:
                seen.add(rid)
                # Mirrors src/lib/channex/client.ts getReviews — flatten
                # to attributes-only so the rest of the worker reads
                # the same shape the route does.
                out.append(attrs)
        if len(seen) == before:
            break
        page += 1
    return out


# ==================== Upsert helpers ====================

def to_five_star(score):
    """Channex 0-10 → guest_reviews.incoming_rating numeric(2,1) 0-5.

    Mirrors src/app/api/reviews/sync/route.ts toFiveStar — divide by 2,
    round to one decimal, return None on missing/non-numeric input.
    """
    if score is None:
        return None
    try:
        s = float(score)
    except (TypeError, ValueError):
        return None
    return round((s / 2) + 1e-9, 1)


def resolve_guest_name(rv):
    # Mirrors src/app/api/reviews/sync/route.ts — trust whatever Channex
    # gives us; route's display-name logic happens at read time via
    # resolveDisplayGuestName once a booking is joined.
    name = rv.get("guest_name")
    if isinstance(name, str) and name.strip():
        return name.strip()
    return None


def build_row(rv, property_id, local_booking_id):
    raw = rv.get("raw_content") or {}
    public_text = raw.get("public_review") or rv.get("content")
    private_text = raw.get("private_feedback")
    rating5 = to_five_star(rv.get("overall_score"))
    incoming_at = rv.get("received_at") or rv.get("inserted_at")
    # Session 6.7 — Channex /reviews carries `attributes.is_hidden`.
    # True for pre-disclosure reviews (14-day mutual-disclosure window
    # open: rating=0 sentinel, content=null). Mirrors
    # src/lib/reviews/sync.ts. Required to gate is_low_rating so the
    # rating=0 sentinel doesn't trip the "Bad review" tag.
    is_hidden = rv.get("is_hidden") is True
    return {
        "channex_review_id": rv.get("id"),
        "booking_id": local_booking_id,
        "property_id": property_id,
        "direction": "incoming",
        "guest_name": resolve_guest_name(rv),
        "ota_reservation_code": rv.get("ota_reservation_id"),
        "incoming_text": public_text,
        "private_feedback": private_text,
        "incoming_rating": rating5,
        "incoming_date": incoming_at,
        "subratings": rv.get("scores"),
        "expired_at": rv.get("expired_at"),
        "is_hidden": is_hidden,
    }, rating5, is_hidden


def sync_property(supabase, client, prop):
    """Sync one property. Returns (new, updated, skipped_no_match).

    Raises on Channex/HTTP failure so the caller knows not to stamp
    reviews_last_synced_at. Per-row upsert errors are logged-and-
    continued so a single bad row doesn't abort the run.
    """
    pid = prop["id"]
    cpid = prop["channex_property_id"]
    name = prop.get("name") or pid

    reviews = fetch_reviews(client, cpid)
    log.info(f"  {name}: fetched {len(reviews)} reviews")

    # Preload bookings for ota_reservation_id → booking_id resolution.
    # RDX-3 — primary key is ota_reservation_code; platform_booking_id stays
    # as fallback during transition. Mirrors src/lib/reviews/sync.ts.
    bookings = (
        supabase.table("bookings")
        .select("id, ota_reservation_code, platform_booking_id")
        .eq("property_id", pid)
        .execute()
    ).data or []
    booking_by_ota = {}
    for b in bookings:
        orc = b.get("ota_reservation_code")
        if orc:
            booking_by_ota[orc] = b["id"]
    for b in bookings:
        pbid = b.get("platform_booking_id")
        if pbid and pbid not in booking_by_ota:
            booking_by_ota[pbid] = b["id"]

    # Preload existing rows by channex_review_id to count new vs updated
    # (Supabase upsert doesn't expose inserted-vs-updated cleanly).
    channex_ids = [r.get("id") for r in reviews if r.get("id")]
    existing = []
    if channex_ids:
        existing = (
            supabase.table("guest_reviews")
            # RDX-DIAG-FIX — preload response state so the no-downgrade
            # rule below can gate updates against Koast-originated true.
            .select("id, channex_review_id, response_sent, response_final, published_at")
            .in_("channex_review_id", channex_ids)
            .execute()
        ).data or []
    existing_set = {r["channex_review_id"] for r in existing}
    existing_by_id = {r["channex_review_id"]: r for r in existing}

    new_count = 0
    updated_count = 0
    skipped_no_match = 0

    for rv in reviews:
        rid = rv.get("id")
        if not rid:
            continue
        ota_res = rv.get("ota_reservation_id")
        local_booking_id = booking_by_ota.get(ota_res) if ota_res else None
        if not local_booking_id:
            skipped_no_match += 1
            log.warning(
                f"    booking_id unresolved channex_review_id={rid} "
                f"ota_reservation_id={ota_res}"
            )

        row, rating5, is_hidden = build_row(rv, pid, local_booking_id)
        is_new = rid not in existing_set

        # RDX-4 — algorithmic low-rating flag, written every iteration.
        # is_bad_review is no longer touched by sync; host marks live on
        # is_flagged_by_host. Mirrors src/lib/reviews/sync.ts.
        # Session 6.7 — gate on is_hidden so rating=0 pre-disclosure
        # reviews don't trip the "Bad review" tag.
        row["is_low_rating"] = (not is_hidden) and rating5 is not None and rating5 < 4
        if is_new:
            row["status"] = "published" if rv.get("is_replied") else "pending"

        try:
            supabase.table("guest_reviews").upsert(
                row, on_conflict="channex_review_id"
            ).execute()
        except Exception as e:
            log.warning(f"    upsert failed for {rid}: {e}")
            continue

        if is_new:
            new_count += 1
        else:
            updated_count += 1

        # RDX-DIAG-FIX — re-evaluate response state on every iteration.
        # Mirrors src/lib/reviews/sync.ts (canonical). No-downgrade rule:
        # only flip false→true; never touch already-true rows.
        if rv.get("is_replied") is True:
            existing_row = existing_by_id.get(rid) or {}
            reply_obj = rv.get("reply") or {}
            reply_text = reply_obj.get("reply") if isinstance(reply_obj, dict) else None
            if not (isinstance(reply_text, str) and reply_text):
                reply_text = None

            patch = {}
            if existing_row.get("response_sent") is not True:
                patch["response_sent"] = True
                patch["status"] = "published"
                if not existing_row.get("published_at"):
                    # Channex doesn't expose a dedicated reply timestamp.
                    # updated_at is the best approximation.
                    patch["published_at"] = rv.get("updated_at") or datetime.utcnow().isoformat()
            if not existing_row.get("response_final") and reply_text:
                patch["response_final"] = reply_text

            if patch:
                try:
                    supabase.table("guest_reviews").update(patch).eq(
                        "channex_review_id", rid
                    ).execute()
                except Exception as e:
                    log.warning(f"    response-state patch failed for {rid}: {e}")

    log.info(
        f"  {name}: {new_count} new, {updated_count} updated, "
        f"{skipped_no_match} skipped (no booking match)"
    )
    return new_count, updated_count, skipped_no_match


# ==================== Main ====================

def main():
    log.info("=== Reviews sync starting ===")
    supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
    client = httpx.Client(timeout=30)

    props = (
        supabase.table("properties")
        .select("id, name, channex_property_id")
        .not_.is_("channex_property_id", "null")
        .execute()
    ).data or []
    log.info(f"Tracking {len(props)} Channex-connected properties")

    if not props:
        log.info("=== Reviews sync complete: no properties ===")
        client.close()
        return

    total_new = 0
    total_updated = 0
    total_skipped = 0
    failed = 0

    for prop in props:
        try:
            n, u, s = sync_property(supabase, client, prop)
            total_new += n
            total_updated += u
            total_skipped += s
            # Stamp on success only (per Session 6.6 brief).
            try:
                supabase.table("properties").update(
                    {"reviews_last_synced_at": datetime.utcnow().isoformat()}
                ).eq("id", prop["id"]).execute()
            except Exception as e:
                log.warning(
                    f"  {prop.get('name')}: stamp failed: {e} "
                    "(sync succeeded, last_synced_at not updated)"
                )
        except Exception as e:
            failed += 1
            log.error(f"  {prop.get('name')}: sync failed: {e}")

    client.close()
    log.info(
        f"=== Reviews sync complete: {total_new} new, {total_updated} "
        f"updated, {total_skipped} skipped, {failed} property failures ==="
    )


if __name__ == "__main__":
    main()
