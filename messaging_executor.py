#!/usr/bin/env python3
"""Messaging template executor — Session 8a slice 1.

Time-anchored, draft-and-queue. Hourly cadence via systemd timer.

Per-run algorithm:
  1. Compute the lookback window: now() - 7 days. Don't fire ancient
     bookings.
  2. Join active message_templates × active bookings; compute
     target_fire_at = booking.<anchor> + trigger_days_offset days
     + trigger_time, where <anchor> ∈ {check_in, check_out, created_at}
     per the trigger_type vocabulary in default-templates.ts.
  3. Filter to candidates whose target_fire_at IS in [now()-7d, now()].
  4. For each candidate: INSERT INTO message_automation_firings
     (template_id, booking_id) ON CONFLICT DO NOTHING RETURNING id.
     Skip if no row (already fired).
  5. If insert succeeded: render the template body (regex
     substitution against booking + property + property_details
     + resolved guest name), insert into messages with
     direction='outgoing', sender='property', draft_status=
     'draft_pending_approval', sent_at=NULL, three-stage timestamps
     NULL.
  6. UPDATE the firings row with draft_message_id = inserted message id.
  7. Per-row try/except — one bad template doesn't poison the run.

Variable substitution: regex {var} on the existing default-templates
vocabulary. Missing variable renders as empty string + WARN log.

Out of scope (8b/8c):
  - Event-driven triggers (Channex webhook → fire). Slice 2.
  - Conditional triggers (skip-if-review-already-left). Slice 3.
  - AI personalization beyond {var} substitution.

NOT systemd-enabled in this commit. Manual run + log inspection is
the supervised first-run gate per references/tech-debt.md.
"""

import logging
import os
import re
import sys
from datetime import datetime, timedelta, timezone

from dotenv import load_dotenv
from supabase import create_client

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [executor] %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("/var/log/staycommand/messaging-executor.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger(__name__)

SUPABASE_URL = os.environ["SUPABASE_URL"]
SUPABASE_KEY = os.environ["SUPABASE_SERVICE_ROLE_KEY"]

LOOKBACK_DAYS = 7
VAR_RE = re.compile(r"\{([a-z_]+)\}")

# trigger_type → which booking date column anchors the fire time.
ANCHOR_COLUMN = {
    "on_booking": "created_at",
    "before_checkin": "check_in",
    "on_checkin": "check_in",
    "after_checkin": "check_in",
    "before_checkout": "check_out",
    "on_checkout": "check_out",
    "after_checkout": "check_out",
}


# ─────────────────────────────────────────────────────────────────
# Guest name resolver — mirrors src/lib/messages/guest-name.ts.
# Keep in sync; the TS file is canonical.
# ─────────────────────────────────────────────────────────────────

PLATFORM_LABEL = {
    "airbnb": "Airbnb Guest",
    "abb": "Airbnb Guest",
    "booking_com": "Booking.com Guest",
    "bookingcom": "Booking.com Guest",
    "bdc": "Booking.com Guest",
    "vrbo": "Vrbo Guest",
    "direct": "Guest",
}


def resolve_guest_name(booking_guest_name, thread_title, platform):
    fb = (booking_guest_name or "").strip()
    if fb and fb.lower() != "guest":
        return fb
    ft = (thread_title or "").strip()
    if ft and not re.match(r"^conversation\b", ft, re.I) and not re.match(r"^(airbnb|booking)", ft, re.I):
        return ft
    key = (platform or "").strip().lower()
    return PLATFORM_LABEL.get(key, "Guest")


# ─────────────────────────────────────────────────────────────────
# Variable substitution
# ─────────────────────────────────────────────────────────────────

def render(body, variables):
    """Replace {var} tokens. Missing vars render as empty + WARN."""
    missing = []

    def sub(m):
        key = m.group(1)
        if key in variables and variables[key] is not None:
            return str(variables[key])
        missing.append(key)
        return ""

    out = VAR_RE.sub(sub, body)
    return out, missing


def build_variables(booking, prop, details, guest_name):
    return {
        "guest_name": guest_name,
        "property_name": prop.get("name") or "",
        "check_in": booking.get("check_in") or "",
        "check_out": booking.get("check_out") or "",
        "checkin_time": (details or {}).get("checkin_time") or "15:00",
        "checkout_time": (details or {}).get("checkout_time") or "11:00",
        "wifi_network": (details or {}).get("wifi_network") or "",
        "wifi_password": (details or {}).get("wifi_password") or "",
        "door_code": (details or {}).get("door_code") or "",
        "parking_instructions": (details or {}).get("parking_instructions") or "",
        "house_rules": (details or {}).get("house_rules") or "",
        "special_instructions": (details or {}).get("special_instructions") or "",
    }


# ─────────────────────────────────────────────────────────────────
# Fire-time computation
# ─────────────────────────────────────────────────────────────────

def parse_anchor(value):
    """Parse a date or timestamp from Supabase into a tz-aware datetime."""
    if not value:
        return None
    s = str(value)
    # Date-only column (check_in / check_out)
    if len(s) == 10 and s[4] == "-":
        return datetime(int(s[:4]), int(s[5:7]), int(s[8:10]), tzinfo=timezone.utc)
    # Full timestamptz
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    return datetime.fromisoformat(s)


def parse_time_of_day(value):
    """`'14:00:00'` → (14, 0, 0). Defaults to midnight."""
    if not value:
        return (0, 0, 0)
    parts = str(value).split(":")
    h = int(parts[0]) if len(parts) > 0 else 0
    m = int(parts[1]) if len(parts) > 1 else 0
    return (h, m, 0)


def compute_fire_at(template, booking):
    anchor_col = ANCHOR_COLUMN.get(template["trigger_type"])
    if not anchor_col:
        return None
    anchor = parse_anchor(booking.get(anchor_col))
    if not anchor:
        return None
    offset_days = template.get("trigger_days_offset") or 0
    h, m, _ = parse_time_of_day(template.get("trigger_time"))
    return (anchor + timedelta(days=offset_days)).replace(hour=h, minute=m, second=0, microsecond=0)


# ─────────────────────────────────────────────────────────────────
# Main run
# ─────────────────────────────────────────────────────────────────

def run():
    sb = create_client(SUPABASE_URL, SUPABASE_KEY)
    now = datetime.now(timezone.utc)
    lookback = now - timedelta(days=LOOKBACK_DAYS)

    log.info("starting run window=[%s, %s]", lookback.isoformat(), now.isoformat())

    # 1. Active templates with a known trigger anchor.
    templates = sb.table("message_templates").select(
        "id, property_id, trigger_type, trigger_days_offset, trigger_time, body"
    ).eq("is_active", True).execute().data or []
    templates = [t for t in templates if t.get("trigger_type") in ANCHOR_COLUMN]

    log.info("loaded %d active templates with known triggers", len(templates))

    if not templates:
        log.info("no templates to evaluate; exiting clean")
        return

    # 2. Active bookings within the window. Pull a wider net than
    #    strictly needed; we filter per template in step 3.
    bookings = sb.table("bookings").select(
        "id, property_id, guest_name, check_in, check_out, platform, created_at"
    ).gte("check_out", (lookback - timedelta(days=30)).date().isoformat()) \
     .lte("check_in", (now + timedelta(days=30)).date().isoformat()) \
     .neq("status", "cancelled").execute().data or []

    log.info("loaded %d candidate bookings", len(bookings))

    bookings_by_property = {}
    for b in bookings:
        bookings_by_property.setdefault(b["property_id"], []).append(b)

    # Property + details lookups, lazily cached.
    prop_cache = {}
    details_cache = {}
    thread_cache = {}  # booking_id → (thread_id, thread_title)

    fired = 0
    skipped = 0
    errors = 0

    for tmpl in templates:
        for booking in bookings_by_property.get(tmpl["property_id"], []):
            try:
                fire_at = compute_fire_at(tmpl, booking)
                if not fire_at or fire_at < lookback or fire_at > now:
                    continue

                # 3. Idempotency gate — INSERT firings ON CONFLICT DO NOTHING.
                ins = sb.table("message_automation_firings").upsert(
                    {"template_id": tmpl["id"], "booking_id": booking["id"]},
                    on_conflict="template_id,booking_id",
                    ignore_duplicates=True,
                ).execute()
                if not ins.data:
                    skipped += 1
                    continue
                firing_id = ins.data[0]["id"]

                # 4. Resolve thread for this booking.
                if booking["id"] not in thread_cache:
                    th = sb.table("message_threads").select(
                        "id, title"
                    ).eq("booking_id", booking["id"]).limit(1).execute().data or []
                    thread_cache[booking["id"]] = (
                        th[0]["id"] if th else None,
                        th[0].get("title") if th else None,
                    )
                thread_id, thread_title = thread_cache[booking["id"]]
                if not thread_id:
                    log.warning(
                        "no thread for booking_id=%s template_id=%s — firing recorded but no draft created",
                        booking["id"], tmpl["id"],
                    )
                    continue

                # 5. Render template body.
                if tmpl["property_id"] not in prop_cache:
                    pr = sb.table("properties").select("id, name").eq("id", tmpl["property_id"]).limit(1).execute().data or []
                    prop_cache[tmpl["property_id"]] = pr[0] if pr else {}
                prop = prop_cache[tmpl["property_id"]]

                if tmpl["property_id"] not in details_cache:
                    dt = sb.table("property_details").select(
                        "wifi_network, wifi_password, door_code, checkin_time, "
                        "checkout_time, parking_instructions, house_rules, special_instructions"
                    ).eq("property_id", tmpl["property_id"]).limit(1).execute().data or []
                    details_cache[tmpl["property_id"]] = dt[0] if dt else {}
                details = details_cache[tmpl["property_id"]]

                guest = resolve_guest_name(booking.get("guest_name"), thread_title, booking.get("platform"))
                variables = build_variables(booking, prop, details, guest)
                rendered, missing = render(tmpl["body"], variables)
                if missing:
                    log.warning("template_id=%s missing vars: %s", tmpl["id"], missing)

                # 6. Insert draft message.
                msg = sb.table("messages").insert({
                    "thread_id": thread_id,
                    "property_id": tmpl["property_id"],
                    "booking_id": booking["id"],
                    "platform": booking.get("platform") or "unknown",
                    # Existing direction CHECK constraint allows only
                    # 'inbound' | 'outbound'. Match it.
                    "direction": "outbound",
                    "sender": "property",
                    "sender_name": "Host",
                    "content": rendered,
                    "ai_draft": rendered,
                    "draft_status": "draft_pending_approval",
                }).execute().data
                draft_message_id = msg[0]["id"] if msg else None

                # 7. Backfill firings row with draft_message_id.
                if draft_message_id:
                    sb.table("message_automation_firings").update(
                        {"draft_message_id": draft_message_id}
                    ).eq("id", firing_id).execute()

                fired += 1
                log.info(
                    "fired template_id=%s booking_id=%s firing_id=%s draft_message_id=%s "
                    "target_fire_at=%s rendered_length=%d",
                    tmpl["id"], booking["id"], firing_id, draft_message_id,
                    fire_at.isoformat(), len(rendered),
                )

            except Exception as exc:
                errors += 1
                log.exception(
                    "row failed template_id=%s booking_id=%s: %s",
                    tmpl.get("id"), booking.get("id"), exc,
                )

    log.info("run complete fired=%d skipped=%d errors=%d", fired, skipped, errors)


if __name__ == "__main__":
    try:
        run()
    except Exception as exc:
        log.exception("fatal: %s", exc)
        sys.exit(1)
