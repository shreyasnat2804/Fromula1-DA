import requests
import pandas as pd
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Constants
API_BASE = "https://api.openf1.org/v1"
YEAR = 2024
SESSION_KEY = 9662  # Example: 2024 Bahrain (or similar valid key)
DRIVER_NUMBER = 55  # Carlos Sainz
LAP_COUNT = 10

def fetch_data(endpoint: str, params: dict) -> pd.DataFrame:
    """Helper to fetch data from OpenF1 API."""
    url = f"{API_BASE}/{endpoint}"
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    data = resp.json()
    return pd.DataFrame(data)

def main():
    logger.info(f"Starting exploration for Driver {DRIVER_NUMBER} in Session {SESSION_KEY}...")

    # 1. Fetch Laps
    logger.info("Fetching laps...")
    laps_df = fetch_data("laps", {"session_key": SESSION_KEY, "driver_number": DRIVER_NUMBER})
    
    if laps_df.empty:
        logger.error("No laps found.")
        return

    # Filter to last N laps
    laps_df['lap_number'] = pd.to_numeric(laps_df['lap_number'])
    last_laps = laps_df.sort_values('lap_number', ascending=False).head(LAP_COUNT)
    
    # Get time range (start of first of these laps -> end of last)
    # OpenF1 laps usually have 'date_start'
    # We want the range to query car_data efficiently (though car_data endpoint accepts session/driver directly)
    # However, fetching ALL car data for a full race is heavy (30MB+). 
    # For a quick script, we can query by time range if the API supports it, or just fetch all and filter.
    # OpenF1 `car_data` supports `date>...` and `date<...`.
    
    start_time = last_laps['date_start'].min()
    # There is no date_end in some laps responses, but let's check columns or just assume we want from start_time onwards.
    # Actually, fetching by session+driver is safest for this boilerplate, 
    # but let's try to be smart if the user wants "initial exploration" to be fast.
    # Let's just fetch all for the driver/session for simplicity as requested, 
    # OR strictly filter. The prompt asks to "pull the last 10 laps".
    # Relying on `date_start` of the Nth-to-last lap.
    
    logger.info(f"Targeting data after {start_time}")

    # 2. Fetch Car Data (Telemetry)
    # Verify rate limit handling isn't strictly needed for a single run script, but good practice.
    # We will just use simple requests here as requested.
    logger.info("Fetching car data (telemetry)...")
    # Using >= start_time to reduce load
    car_params = {
        "session_key": SESSION_KEY, 
        "driver_number": DRIVER_NUMBER, 
        "date>": start_time
    }
    car_df = fetch_data("car_data", car_params)
    
    # 3. Fetch Weather
    logger.info("Fetching weather...")
    # Weather is global, fetch for same time range
    weather_params = {
        "session_key": SESSION_KEY,
        "date>": start_time
    }
    weather_df = fetch_data("weather", weather_params)

    # 4. Processing & Merging
    logger.info("Processing and merging...")
    
    # Standardize Timestamps
    car_df['date'] = pd.to_datetime(car_df['date'], format='ISO8601')
    last_laps['date_start'] = pd.to_datetime(last_laps['date_start'], format='ISO8601')
    weather_df['date'] = pd.to_datetime(weather_df['date'], format='ISO8601')
    
    # Sort for merge_asof
    car_df = car_df.sort_values('date')
    weather_df = weather_df.sort_values('date')
    last_laps = last_laps.sort_values('date_start')
    
    # Merge Laps (to get lap number on telemetry)
    # Rename for merge
    laps_merge = last_laps[['date_start', 'lap_number']].rename(columns={'date_start': 'date'})
    
    merged_df = pd.merge_asof(
        car_df,
        laps_merge,
        on='date',
        direction='backward'
    )
    
    # Filter out data before the first lap start (if any)
    merged_df = merged_df.dropna(subset=['lap_number'])

    # Merge Weather
    # Rename weather date to avoid collision/loss
    weather_merge = weather_df[['date', 'air_temperature', 'track_temperature']].rename(columns={'date': 'weather_date'})
    
    final_df = pd.merge_asof(
        merged_df,
        weather_merge,
        left_on='date',
        right_on='weather_date',
        direction='backward'
    )
    
    logger.info(f"Successfully created dataset with {len(final_df)} rows.")
    print(final_df.head())
    
    # Optional: Save
    final_df.to_csv("exploration_last10.csv", index=False)
    logger.info("Saved to exploration_last10.csv")

if __name__ == "__main__":
    main()
