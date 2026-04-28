"""Direct PostgreSQL database access for Koast workers."""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv(os.path.join(os.path.dirname(__file__), ".env"))


def get_connection():
    """Get a new database connection."""
    return psycopg2.connect(os.environ["DATABASE_URL"], cursor_factory=RealDictCursor)


def get_active_properties():
    """Get all properties with channex_property_id set."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT id, name, latitude, longitude, channex_property_id "
                "FROM properties WHERE channex_property_id IS NOT NULL"
            )
            return cur.fetchall()


def get_all_properties():
    """Get all properties."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT id, name, latitude, longitude, channex_property_id FROM properties")
            return cur.fetchall()


def upsert_booking(booking_data):
    """Upsert a booking by channex_booking_id."""
    with get_connection() as conn:
        with conn.cursor() as cur:
            # Check if exists
            cur.execute(
                "SELECT id FROM bookings WHERE channex_booking_id = %s",
                (booking_data["channex_booking_id"],),
            )
            existing = cur.fetchone()

            if existing:
                cols = ", ".join(f"{k} = %({k})s" for k in booking_data if k != "channex_booking_id")
                cur.execute(
                    f"UPDATE bookings SET {cols} WHERE id = %(existing_id)s",
                    {**booking_data, "existing_id": existing["id"]},
                )
                conn.commit()
                return existing["id"], "updated"
            else:
                cols = ", ".join(booking_data.keys())
                vals = ", ".join(f"%({k})s" for k in booking_data)
                cur.execute(
                    f"INSERT INTO bookings ({cols}) VALUES ({vals}) RETURNING id",
                    booking_data,
                )
                conn.commit()
                row = cur.fetchone()
                return row["id"] if row else None, "inserted"
