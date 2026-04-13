import os
import psycopg2
from psycopg2.extras import RealDictCursor

RIDES_DB_URL = "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(
    user=os.getenv("POSTGRES_USER", "postgres"),
    password=os.getenv("POSTGRES_PASSWORD", "postgres"),
    host=os.getenv("POSTGRES_HOST", "localhost"),
    port=os.getenv("POSTGRES_PORT", "5432"),
    dbname=os.getenv("POSTGRES_DB", "ride_booking_pg_db"),
)

TRIPS_DB_URL = "postgresql://{user}:{password}@{host}:{port}/{dbname}".format(
    user=os.getenv("TRIPS_POSTGRES_USER", "postgres"),
    password=os.getenv("TRIPS_POSTGRES_PASSWORD", "postgres"),
    host=os.getenv("TRIPS_POSTGRES_HOST", "localhost"),
    port=os.getenv("TRIPS_POSTGRES_PORT", "5432"),
    dbname=os.getenv("TRIPS_POSTGRES_DB", "trips_pg_db"),
)


def get_rides_connection():
    return psycopg2.connect(RIDES_DB_URL, cursor_factory=RealDictCursor)


def get_trips_connection():
    return psycopg2.connect(TRIPS_DB_URL, cursor_factory=RealDictCursor)


def init_db():
    conn = get_rides_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS rides (
                    id UUID PRIMARY KEY,
                    rider_name VARCHAR(255) NOT NULL,
                    pickup_location VARCHAR(255) NOT NULL,
                    dropoff_location VARCHAR(255) NOT NULL,
                    status VARCHAR(20) NOT NULL DEFAULT 'booked',
                    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                    cancelled_at TIMESTAMP
                );
            """)
        conn.commit()
    finally:
        conn.close()

    conn = get_trips_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS trips (
                    id UUID PRIMARY KEY,
                    trip_name VARCHAR(255) NOT NULL,
                    origin VARCHAR(255) NOT NULL,
                    destination VARCHAR(255) NOT NULL,
                    trip_date DATE NOT NULL,
                    created_at TIMESTAMP NOT NULL DEFAULT NOW()
                );
            """)
        conn.commit()
    finally:
        conn.close()
