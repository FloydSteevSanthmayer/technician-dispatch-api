"""
Async FastAPI Technician Dispatch Service (app.py)

This file is a production-ready async implementation using asyncpg and httpx.
"""

import os
import math
import asyncio
import logging
from typing import List, Optional
from datetime import datetime

import asyncpg
import httpx
from tenacity import retry, wait_exponential, stop_after_attempt
from fastapi import FastAPI, HTTPException, Path
from pydantic import BaseModel

# Configuration from environment
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "technician_short")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD")

ORS_API_KEY = os.getenv("OPENROUTE_API_KEY")
ORS_BASE_URL = os.getenv("OPENROUTE_BASE_URL", "https://api.openrouteservice.org/v2/directions/driving-car")

if not DB_PASSWORD:
    raise RuntimeError("DB_PASSWORD is required")
if not ORS_API_KEY:
    raise RuntimeError("OPENROUTE_API_KEY is required")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class AssignmentModel(BaseModel):
    id: Optional[int]
    cust_id: int
    tech_id: int
    distance_km: float
    assigned_at: datetime

def haversine_km(lat1, lon1, lat2, lon2) -> float:
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi/2)**2 + math.cos(phi1)*math.cos(phi2)*math.sin(dlambda/2)**2
    return 2 * R * math.asin(math.sqrt(a))

app = FastAPI(title="Technician Dispatch API (async)")

async def create_db_pool():
    pool = await asyncpg.create_pool(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASSWORD, database=DB_NAME, min_size=1, max_size=10
    )
    return pool

async def init_db(pool):
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS public.customers (
                customerid SERIAL PRIMARY KEY,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS public.technicians (
                technicianid SERIAL PRIMARY KEY,
                latitude DOUBLE PRECISION NOT NULL,
                longitude DOUBLE PRECISION NOT NULL,
                is_active BOOLEAN DEFAULT TRUE
            );
        """)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS public.assignments (
                id SERIAL PRIMARY KEY,
                cust_id INTEGER NOT NULL REFERENCES public.customers(customerid),
                tech_id INTEGER NOT NULL REFERENCES public.technicians(technicianid),
                distance_km DOUBLE PRECISION NOT NULL,
                assigned_at TIMESTAMP WITH TIME ZONE DEFAULT now()
            );
        """)

@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(3), reraise=True)
async def get_distance_km(client: httpx.AsyncClient, lat1, lon1, lat2, lon2) -> float:
    params = {"start": f"{lon1},{lat1}", "end": f"{lon2},{lat2}"}
    headers = {"Authorization": ORS_API_KEY, "Accept": "application/json"}
    resp = await client.get(ORS_BASE_URL, params=params, headers=headers, timeout=15.0)
    resp.raise_for_status()
    data = resp.json()
    features = data.get("features") or []
    if not features:
        raise ValueError("ORS returned no features")
    summary = features[0].get("properties", {}).get("summary") or {}
    distance_m = summary.get("distance")
    if distance_m is None:
        raise ValueError("ORS response missing distance")
    return float(distance_m) / 1000.0

@app.on_event("startup")
async def on_startup():
    app.state.db_pool = await create_db_pool()
    await init_db(app.state.db_pool)

@app.on_event("shutdown")
async def on_shutdown():
    pool = getattr(app.state, "db_pool", None)
    if pool:
        await pool.close()

@app.post("/dispatch/{cust_id}", response_model=AssignmentModel)
async def dispatch_one(cust_id: int = Path(...)):
    pool = app.state.db_pool
    async with pool.acquire() as conn:
        customer = await conn.fetchrow("SELECT customerid, latitude, longitude FROM public.customers WHERE customerid = $1", cust_id)
    if not customer:
        raise HTTPException(status_code=404, detail=f"Customer {cust_id} not found")
    cust_lat, cust_lon = customer["latitude"], customer["longitude"]

    async with pool.acquire() as conn:
        tech_rows = await conn.fetch("SELECT technicianid, latitude, longitude FROM public.technicians WHERE is_active = TRUE")
    if not tech_rows:
        raise HTTPException(status_code=503, detail="No technicians available")

    TOP_K = 5
    candidates = []
    for r in tech_rows:
        tech_id, tlat, tlon = r["technicianid"], r["latitude"], r["longitude"]
        est = haversine_km(cust_lat, cust_lon, tlat, tlon)
        candidates.append((est, tech_id, tlat, tlon))
    candidates.sort(key=lambda x: x[0])
    shortlist = candidates[:TOP_K]

    async with httpx.AsyncClient() as client:
        tasks = [asyncio.create_task(get_distance_km(client, cust_lat, cust_lon, tlat, tlon)) for (_, tech_id, tlat, tlon) in shortlist]
        done, pending = await asyncio.wait(tasks, return_when=asyncio.ALL_COMPLETED)
        best_id = None
        best_dist = float("inf")
        for idx, fut in enumerate(tasks):
            tech_id = shortlist[idx][1]
            try:
                d = fut.result()
            except Exception as e:
                continue
            if d < best_dist:
                best_dist = d
                best_id = tech_id

    if best_id is None:
        raise HTTPException(status_code=503, detail="Unable to compute driving distances at this time")

    async with pool.acquire() as conn:
        row = await conn.fetchrow("INSERT INTO public.assignments (cust_id, tech_id, distance_km) VALUES ($1,$2,$3) RETURNING id, assigned_at", cust_id, best_id, best_dist)
    return AssignmentModel(id=row["id"], cust_id=cust_id, tech_id=best_id, distance_km=best_dist, assigned_at=row["assigned_at"])

@app.get("/assignments", response_model=List[AssignmentModel])
async def list_assignments():
    pool = app.state.db_pool
    async with pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, cust_id, tech_id, distance_km, assigned_at FROM public.assignments ORDER BY assigned_at DESC")
    return [AssignmentModel(id=r["id"], cust_id=r["cust_id"], tech_id=r["tech_id"], distance_km=r["distance_km"], assigned_at=r["assigned_at"]) for r in rows]

@app.get("/healthz")
async def healthz():
    pool = getattr(app.state, "db_pool", None)
    db_ok = False
    if pool:
        try:
            async with pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            db_ok = True
        except Exception:
            db_ok = False
    return {"status": "ok" if db_ok else "db_unavailable"}
