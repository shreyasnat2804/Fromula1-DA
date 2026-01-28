import asyncio
import logging
import os
import json
import pandas as pd
from typing import Set
from pathlib import Path
from ingestion.client import OpenF1Client
from ingestion.session_manager import SessionManager
from ingestion.ingest_car_data import fetch_car_data
from ingestion.ingest_laps import fetch_laps
from ingestion.ingest_weather import fetch_weather

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

DATA_DIR = Path("data/raw")
STATE_FILE = DATA_DIR / "ingestion_state.json"

def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_state(state: dict):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, indent=2)

async def process_session(client: OpenF1Client, session_key: int, state: dict):
    session_str = str(session_key)
    if session_str not in state:
        state[session_str] = {"laps": False, "weather": False, "drivers": []}
    
    session_state = state[session_str]
    
    # 1. Fetch Laps (to get driver list)
    laps_path = DATA_DIR / f"laps_{session_key}.parquet"
    laps_df = pd.DataFrame()
    
    if not session_state.get("laps") or not laps_path.exists():
        laps_df = await fetch_laps(client, session_key)
        if not laps_df.empty:
            laps_df.to_parquet(laps_path, index=False)
            session_state["laps"] = True
            save_state(state)
    else:
        logger.info(f"Laps for {session_key} already fetched.")
        laps_df = pd.read_parquet(laps_path)

    # 2. Fetch Weather
    weather_path = DATA_DIR / f"weather_{session_key}.parquet"
    if not session_state.get("weather") or not weather_path.exists():
        weather_df = await fetch_weather(client, session_key)
        if not weather_df.empty:
            weather_df.to_parquet(weather_path, index=False)
            session_state["weather"] = True
            save_state(state)
    else:
         logger.info(f"Weather for {session_key} already fetched.")

    # 3. Fetch Car Data for All Drivers
    if laps_df.empty:
        logger.warning(f"No laps data for {session_key}, cannot determine drivers.")
        return

    drivers = laps_df['driver_number'].unique()
    logged_drivers = set(session_state.get("drivers", []))
    
    tasks = []
    
    async def fetch_and_save_driver(driver):
        if str(driver) in logged_drivers:
             # logger.info(f"Driver {driver} already fetched.") 
             # keeping logs quiet for skipped items
             return
             
        df = await fetch_car_data(client, session_key, driver)
        if not df.empty:
            df.to_parquet(DATA_DIR / f"car_{session_key}_{driver}.parquet", index=False)
            # Update state safely (in main thread context ideally, but we'll modify the set and save at end/periodically)
            # actually, concurrent modification of the dict/set might be risky if we save continuously.
            # We'll return the driver number and update state in the gather.
            return driver
        return None

    # Semaphore is already inside client.fetch, so we can spawn many tasks.
    # But let's batch them slightly or just fire all (asyncio.gather handles it, client limits concurrency).
    logger.info(f"Found {len(drivers)} drivers. Fetching telemetry...")
    
    pending_drivers = [d for d in drivers if str(d) not in logged_drivers]
    
    if not pending_drivers:
        logger.info("All drivers already fetched for this session.")
        return

    # Create tasks
    tasks = [fetch_and_save_driver(d) for d in pending_drivers]
    
    # Run tasks
    results = await asyncio.gather(*tasks)
    
    # Update State
    newly_fetched = [r for r in results if r is not None]
    if newly_fetched:
        session_state["drivers"].extend([int(x) for x in newly_fetched])
        save_state(state)
        logger.info(f"Fetched and saved {len(newly_fetched)} new drivers.")

async def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    state = load_state()
    
    async with OpenF1Client(concurrency_limit=5) as client: # Limit concurrent requests
        session_mgr = SessionManager(client)
        
        # Get Latest Session
        session_key = await session_mgr.get_latest_session_key()
        if not session_key:
            return

        logger.info(f"Processing Session: {session_key}")
        await process_session(client, session_key, state)
        
    logger.info("Ingestion Complete.")

if __name__ == "__main__":
    asyncio.run(main())
