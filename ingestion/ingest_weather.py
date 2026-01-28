import logging
import pandas as pd
from ingestion.client import OpenF1Client

logger = logging.getLogger(__name__)

async def fetch_weather(client: OpenF1Client, session_key: int) -> pd.DataFrame:
    """
    Fetch weather data for a specific session.
    Weather is session-global, not driver specific.
    """
    params = {"session_key": session_key}
        
    logger.info(f"Fetching weather for session {session_key}...")
    data = await client.fetch("weather", params=params)
    
    if not data:
        logger.warning(f"No weather data found for session {session_key}")
        return pd.DataFrame()

    df = pd.DataFrame(data)
    
    # Standardize timestamp
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], format='ISO8601')
        
    logger.info(f"Fetched {len(df)} weather records for session {session_key}")
    return df
