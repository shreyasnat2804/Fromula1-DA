import logging
import pandas as pd
from ingestion.client import OpenF1Client

logger = logging.getLogger(__name__)

async def fetch_car_data(client: OpenF1Client, session_key: int, driver_number: int = None) -> pd.DataFrame:
    """
    Fetch car data (telemetry) for a specific session.
    Optionally filter by driver_number.
    """
    params = {"session_key": session_key}
    if driver_number:
        params["driver_number"] = driver_number
        
    logger.info(f"Fetching car data for session {session_key}...")
    data = await client.fetch("car_data", params=params)
    
    if not data:
        logger.warning(f"No car data found for session {session_key}")
        return pd.DataFrame()
        
    df = pd.DataFrame(data)
    
    # Standardize timestamp
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], format='ISO8601')
        
    logger.info(f"Fetched {len(df)} car data records for session {session_key}")
    return df
