import logging
from typing import List, Dict
from ingestion.client import OpenF1Client

logger = logging.getLogger(__name__)

class SessionManager:
    def __init__(self, client: OpenF1Client):
        self.client = client

    async def get_sessions(self, year: int = 2024, type: str = None) -> List[Dict]:
        """
        Fetch sessions for a given year.
        Optionally filter by type (e.g., 'Race', 'Qualifying').
        """
        params = {"year": year}
        data = await self.client.fetch("sessions", params=params)
        
        if type:
            data = [s for s in data if s.get("session_name") == type]
            
        logger.info(f"Found {len(data)} sessions for year {year}")
        return data

    async def get_latest_session_key(self) -> int:
        """
        Get the session key for the latest/current session.
        """
        # We can query with 'latest' implicitly or fetch generic sessions
        # OpenF1 doesn't have a direct 'latest' endpoint that returns a key, 
        # but usage conventionally implies looking at the last one in the list.
        sessions = await self.get_sessions(year=2024) # defaulting to 2024 for now
        if not sessions:
            return None
        
        # sort by date_start explicitly to be sure
        sessions.sort(key=lambda x: x['date_start'])
        return sessions[-1]['session_key']
