"""Simple iCal (.ics) parser for StayCommand workers."""

import re
from datetime import datetime
from typing import List, Optional
from dataclasses import dataclass


BLOCKED_SUMMARIES = [
    "not available", "blocked", "airbnb (not available)",
    "reserved", "unavailable", "closed", "block",
]


@dataclass
class ICalBooking:
    uid: str
    guest_name: Optional[str]
    check_in: str  # YYYY-MM-DD
    check_out: str
    platform: str
    is_blocked: bool
    description: Optional[str] = None


def detect_platform(url: str) -> str:
    lower = url.lower()
    if "airbnb.com" in lower:
        return "airbnb"
    if "vrbo.com" in lower or "homeaway.com" in lower:
        return "vrbo"
    if "booking.com" in lower:
        return "booking_com"
    return "direct"


def parse_date(value: str) -> str:
    """Parse iCal date value to YYYY-MM-DD."""
    digits = re.sub(r"[^\d]", "", value)[:8]
    if len(digits) == 8:
        return f"{digits[:4]}-{digits[4:6]}-{digits[6:8]}"
    return value


def unfold(text: str) -> str:
    """RFC 5545 line unfolding."""
    return re.sub(r"\r?\n[ \t]", "", text)


def parse_ical(text: str, url: str = "") -> List[ICalBooking]:
    """Parse iCal text into booking entries."""
    text = unfold(text)
    platform = detect_platform(url)
    bookings = []

    blocks = text.split("BEGIN:VEVENT")
    for block in blocks[1:]:
        block = block.split("END:VEVENT")[0]
        lines = block.split("\n")

        uid = ""
        summary = ""
        dtstart = ""
        dtend = ""
        description = ""

        for line in lines:
            line = line.strip()
            if line.startswith("UID:"):
                uid = line[4:].strip()
            elif line.startswith("SUMMARY:"):
                summary = line[8:].strip()
            elif line.startswith("DTSTART"):
                val = line.split(":", 1)[-1].strip()
                dtstart = parse_date(val)
            elif line.startswith("DTEND"):
                val = line.split(":", 1)[-1].strip()
                dtend = parse_date(val)
            elif line.startswith("DESCRIPTION:"):
                description = line[12:].strip()

        if not dtstart or not dtend:
            continue
        if not uid:
            uid = f"{platform}-{dtstart}-{dtend}-{summary}"

        lower_summary = summary.lower()
        # "Reserved" on Airbnb = real booking (privacy-masked)
        is_airbnb_booking = (platform == "airbnb" and lower_summary == "reserved")
        # Booking.com iCal feeds use summaries like "CLOSED - Not available
        # for check-in" for actual reservations — the feed never includes
        # guest details, just the blocked dates. Treat every BDC iCal entry
        # as a real booking so we can push availability=0 back through
        # Channex to block the dates on other channels (mainly Airbnb).
        # Same logic for Vrbo which also uses "Blocked"/"Reserved" summaries.
        is_bdc_booking = platform == "booking_com"
        is_vrbo_booking = platform == "vrbo" and (
            "reserved" in lower_summary or "blocked" in lower_summary
        )
        is_platform_booking = is_airbnb_booking or is_bdc_booking or is_vrbo_booking

        is_blocked = not is_platform_booking and (
            any(b in lower_summary for b in BLOCKED_SUMMARIES)
            or lower_summary in ("", "closed - not available")
        )

        if is_bdc_booking:
            guest_name = "Booking.com Guest"
        elif is_airbnb_booking:
            guest_name = "Airbnb Guest"
        elif is_vrbo_booking:
            guest_name = "Vrbo Guest"
        elif is_blocked:
            guest_name = None
        else:
            guest_name = summary or None

        bookings.append(ICalBooking(
            uid=uid,
            guest_name=guest_name,
            check_in=dtstart,
            check_out=dtend,
            platform=platform,
            is_blocked=is_blocked,
            description=description or None,
        ))

    return bookings
