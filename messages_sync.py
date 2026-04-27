#!/usr/bin/env python3
"""Messages sync worker — runs every 60 min as webhook fallback.

MSG-S1 Phase G. Mirrors reviews_sync.py shape but with these
differences:
  - 60-min cadence (vs reviews 20-min). Webhooks carry realtime
    load; this is reconciliation only.
  - Two endpoints per property: /message_threads (list) then
    /message_threads/:id/messages per thread (paginated).
  - Channel-asymmetric booking link per MESSAGING_DESIGN §3:
    BDC threads carry relationships.booking; AirBNB derive via
    ota_message_thread_id → bookings.platform_booking_id.

Same dedup-by-id pagination loop as reviews — page[limit] is
advisory (probe-confirmed: page[limit]=5 returned 10 on
/message_threads/:id/messages 2026-04-26).

NOT systemd-enabled in slice 1 commit. Run once manually under
supervision before enabling the timer.
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
    format="%(asctime)s [messages] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/staycommand/messages.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]
CHANNEX_URL = os.environ.get("CHANNEX_API_URL", "https://app.channex.io/api/v1")
CHANNEX_KEY = os.environ["CHANNEX_API_KEY"]


def channex_headers():
    return {"Content-Type": "application/json", "user-api-key": CHANNEX_KEY}


def channel_code_from_provider(provider):
    if not provider:
        return "unknown"
    p = provider.lower()
    if p == "airbnb":
        return "abb"
    if p in ("bookingcom", "booking_com", "booking.com"):
        return "bdc"
    return "unknown"


def platform_for_code(code):
    if code == "abb":
        return "airbnb"
    if code == "bdc":
        return "booking_com"
    return code


def derive_booking_link(thread):
    """Channel-asymmetric per MESSAGING_DESIGN §3.

    BDC: relationships.booking.data.id present
    AirBNB: only attributes.ota_message_thread_id
    """
    a = thread.get("attributes", {}) or {}
    rels = thread.get("relationships", {}) or {}
    ota = a.get("ota_message_thread_id")
    cbid = (rels.get("booking") or {}).get("data", {}).get("id") if rels.get("booking") else None
    return cbid, ota


def fetch_threads(client, cpid):
    """Pull all threads for a property — dedup-by-id loop."""
    seen = set()
    out = []
    page = 1
    while page <= 50:
        url = f"{CHANNEX_URL}/message_threads?filter[property_id]={cpid}&page[limit]=100&page[number]={page}"
        r = client.get(url, headers=channex_headers())
        r.raise_for_status()
        batch = (r.json() or {}).get("data", [])
        if not batch:
            break
        before = len(seen)
        for t in batch:
            tid = t.get("id")
            if tid and tid not in seen:
                seen.add(tid)
                out.append(t)
        if len(seen) == before:
            break
        page += 1
    return out


def fetch_messages(client, thread_id):
    """Pull all messages in a thread — dedup-by-id loop."""
    seen = set()
    out = []
    page = 1
    while page <= 50:
        url = f"{CHANNEX_URL}/message_threads/{thread_id}/messages?page[limit]=100&page[number]={page}"
        r = client.get(url, headers=channex_headers())
        r.raise_for_status()
        batch = (r.json() or {}).get("data", [])
        if not batch:
            break
        before = len(seen)
        for m in batch:
            mid = m.get("id")
            if mid and mid not in seen:
                seen.add(mid)
                out.append(m)
        if len(seen) == before:
            break
        page += 1
    return out


def resolve_local_booking_id(supabase, property_id, channex_booking_id, ota_thread_id):
    """Same logic as src/lib/webhooks/messaging.ts resolveLocalBookingIdForThread."""
    if channex_booking_id:
        rows = (
            supabase.table("bookings")
            .select("id")
            .eq("property_id", property_id)
            .eq("channex_booking_id", channex_booking_id)
            .limit(1)
            .execute()
        ).data or []
        if rows:
            return rows[0]["id"]
    if ota_thread_id:
        rows = (
            supabase.table("bookings")
            .select("id")
            .eq("property_id", property_id)
            .eq("ota_reservation_code", ota_thread_id)
            .limit(1)
            .execute()
        ).data or []
        if rows:
            return rows[0]["id"]
    return None


def upsert_thread(supabase, thread, property_id):
    """Mirror src/lib/webhooks/messaging.ts buildThreadRowFromChannex."""
    a = thread.get("attributes", {}) or {}
    cbid, ota = derive_booking_link(thread)
    rels = thread.get("relationships", {}) or {}
    channel_id = (rels.get("channel") or {}).get("data", {}).get("id") if rels.get("channel") else None
    provider = a.get("provider") or "Unknown"
    booking_id = resolve_local_booking_id(supabase, property_id, cbid, ota)

    row = {
        "channex_thread_id": thread["id"],
        "property_id": property_id,
        "booking_id": booking_id,
        "channex_channel_id": channel_id,
        "channex_booking_id": cbid,
        "ota_message_thread_id": ota,
        "channel_code": channel_code_from_provider(provider),
        "provider_raw": provider,
        "title": a.get("title"),
        # Channex returns last_message as an object {message, sender, ...}
        # not a string. Extract the .message text. (Same fix as
        # src/lib/channex/messages.ts:lastMessagePreview.)
        "last_message_preview": (
            a["last_message"].get("message") if isinstance(a.get("last_message"), dict)
            else a.get("last_message")
        ),
        "last_message_received_at": a.get("last_message_received_at"),
        "message_count": a.get("message_count") or 0,
        "is_closed": bool(a.get("is_closed")),
        "meta": a.get("meta"),
        "channex_inserted_at": a.get("inserted_at"),
        "channex_updated_at": a.get("updated_at"),
        "updated_at": datetime.utcnow().isoformat(),
    }
    res = (
        supabase.table("message_threads")
        .upsert(row, on_conflict="channex_thread_id")
        .execute()
    )
    # Fetch local id back (upsert may not return it on the same call shape)
    local = (
        supabase.table("message_threads")
        .select("id")
        .eq("channex_thread_id", thread["id"])
        .limit(1)
        .execute()
    ).data
    return local[0]["id"] if local else None


def upsert_messages(supabase, msgs, local_thread_id, property_id, platform):
    """Mirror src/lib/messages/sync.ts upsertMessages."""
    if not msgs:
        return 0, 0
    ids = [m.get("id") for m in msgs if m.get("id")]
    existing = (
        supabase.table("messages")
        .select("channex_message_id")
        .in_("channex_message_id", ids)
        .execute()
    ).data or []
    existing_set = {r["channex_message_id"] for r in existing}

    new_count = 0
    upd_count = 0
    for m in msgs:
        a = m.get("attributes", {}) or {}
        sender = a.get("sender") or "guest"
        direction = "inbound" if sender == "guest" else "outbound"
        is_new = m["id"] not in existing_set
        try:
            supabase.table("messages").upsert(
                {
                    "channex_message_id": m["id"],
                    "thread_id": local_thread_id,
                    "property_id": property_id,
                    "platform": platform,
                    "direction": direction,
                    "sender": sender,
                    "sender_name": "Guest" if sender == "guest" else "Host",
                    "content": a.get("message") or "",
                    "attachments": a.get("attachments") or [],
                    "channex_meta": a.get("meta"),
                    "channex_inserted_at": a.get("inserted_at"),
                    "channex_updated_at": a.get("updated_at"),
                },
                on_conflict="channex_message_id",
            ).execute()
        except Exception as e:
            log.warning(f"    msg upsert failed {m['id']}: {e}")
            continue
        if is_new:
            new_count += 1
        else:
            upd_count += 1
    return new_count, upd_count


def refresh_aggregates(supabase, local_thread_id):
    rows = (
        supabase.table("messages")
        .select("channex_inserted_at, sender, read_at")
        .eq("thread_id", local_thread_id)
        .order("channex_inserted_at", desc=True)
        .execute()
    ).data or []
    newest = rows[0]["channex_inserted_at"] if rows else None
    msg_count = len(rows)
    unread = sum(1 for r in rows if r.get("sender") == "guest" and not r.get("read_at"))
    supabase.table("message_threads").update(
        {
            "last_message_received_at": newest,
            "message_count": msg_count,
            "unread_count": unread,
            "updated_at": datetime.utcnow().isoformat(),
        }
    ).eq("id", local_thread_id).execute()


def sync_property(supabase, client, prop):
    pid = prop["id"]
    cpid = prop["channex_property_id"]
    name = prop.get("name") or pid

    threads = fetch_threads(client, cpid)
    log.info(f"  {name}: fetched {len(threads)} threads")

    t_new = t_upd = m_new = m_upd = 0
    for t in threads:
        try:
            local_thread_id = upsert_thread(supabase, t, pid)
            if not local_thread_id:
                log.warning(f"    upsert returned no id for {t['id']}, skipping messages")
                continue
            # Quick check whether the thread already had this message_count
            # in our DB — proxy for "new vs existing thread."
            t_new += 1  # we don't bother distinguishing on the worker path
            provider = (t.get("attributes") or {}).get("provider")
            platform = platform_for_code(channel_code_from_provider(provider))

            msgs = fetch_messages(client, t["id"])
            n, u = upsert_messages(supabase, msgs, local_thread_id, pid, platform)
            m_new += n
            m_upd += u
            refresh_aggregates(supabase, local_thread_id)
        except Exception as e:
            log.error(f"    thread {t.get('id')}: {e}")

    log.info(f"  {name}: {len(threads)} threads, {m_new} new msgs, {m_upd} updated msgs")
    return len(threads), m_new, m_upd


def main():
    log.info("=== Messages sync starting ===")
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
        log.info("=== Messages sync complete: no properties ===")
        client.close()
        return

    total_threads = total_new = total_upd = 0
    failed = 0
    for p in props:
        try:
            t, n, u = sync_property(supabase, client, p)
            total_threads += t
            total_new += n
            total_upd += u
            try:
                supabase.table("properties").update(
                    {"messages_last_synced_at": datetime.utcnow().isoformat()}
                ).eq("id", p["id"]).execute()
            except Exception as e:
                log.warning(f"  {p.get('name')}: stamp failed: {e}")
        except Exception as e:
            failed += 1
            log.error(f"  {p.get('name')}: sync failed: {e}")

    client.close()
    log.info(
        f"=== Messages sync complete: {total_threads} threads, {total_new} new msgs, "
        f"{total_upd} updated msgs, {failed} property failures ==="
    )


if __name__ == "__main__":
    main()
