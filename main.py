import uuid
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from db import get_connection, init_db

app = FastAPI(title="Ride Booking API")


class BookRideRequest(BaseModel):
    rider_name: str
    pickup_location: str
    dropoff_location: str


class RideResponse(BaseModel):
    id: str
    rider_name: str
    pickup_location: str
    dropoff_location: str
    status: str
    created_at: datetime
    cancelled_at: Optional[datetime] = None


@app.get("/health")
def health():
    return {"status": "ok"}


@app.on_event("startup")
def startup():
    init_db()


@app.post("/rides", response_model=RideResponse, status_code=201)
def book_ride(request: BookRideRequest):
    ride_id = str(uuid.uuid4())
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO rides (id, rider_name, pickup_location, dropoff_location, status)
                VALUES (%s, %s, %s, %s, 'booked')
                RETURNING id, rider_name, pickup_location, dropoff_location, status, created_at, cancelled_at
                """,
                (ride_id, request.rider_name, request.pickup_location, request.dropoff_location),
            )
            ride = cur.fetchone()
        conn.commit()
        return dict(ride)
    finally:
        conn.close()


@app.patch("/rides/{ride_id}/cancel", response_model=RideResponse)
def cancel_ride(ride_id: str):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT status FROM rides WHERE id = %s", (ride_id,))
            ride = cur.fetchone()
            if not ride:
                raise HTTPException(status_code=404, detail="Ride not found")
            if ride["status"] == "cancelled":
                raise HTTPException(status_code=400, detail="Ride is already cancelled")

            cur.execute(
                """
                UPDATE rides SET status = 'cancelled', cancelled_at = NOW()
                WHERE id = %s
                RETURNING id, rider_name, pickup_location, dropoff_location, status, created_at, cancelled_at
                """,
                (ride_id,),
            )
            updated_ride = cur.fetchone()
        conn.commit()
        return dict(updated_ride)
    finally:
        conn.close()


@app.get("/rides", response_model=List[RideResponse])
def list_rides(status: Optional[str] = None):
    conn = get_connection()
    try:
        with conn.cursor() as cur:
            if status and status in ("booked", "cancelled"):
                cur.execute(
                    "SELECT * FROM rides WHERE status = %s ORDER BY created_at DESC",
                    (status,),
                )
            else:
                cur.execute("SELECT * FROM rides ORDER BY created_at DESC")
            rides = cur.fetchall()
        return [dict(r) for r in rides]
    finally:
        conn.close()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
