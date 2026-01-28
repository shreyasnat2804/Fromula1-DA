import logging
import pandas as pd
from ingestion.client import OpenF1Client

logger = logging.getLogger(__name__)

async def fetch_laps(client: OpenF1Client, session_key: int, driver_number: int = None) -> pd.DataFrame:
    """
    Fetch laps data for a specific session.
    """
    params = {"session_key": session_key}
    if driver_number:
        params["driver_number"] = driver_number
        
    logger.info(f"Fetching laps for session {session_key}...")
    data = await client.fetch("laps", params=params)
    
    if not data:
        logger.warning(f"No laps data found for session {session_key}")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    
    # Standardize timestamps
    # Laps often have 'date_start' and 'date_end', maybe just 'date_start' is 'date'
    # Checking docs or typical API response: laps usually have date_start
    if 'date_start' in df.columns:
        df['date_start'] = pd.to_datetime(df['date_start'], format='ISO8601')
        
    logger.info(f"Fetched {len(df)} laps for session {session_key}")
    return df
