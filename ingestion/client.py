import asyncio
import logging
import aiohttp
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

BASE_URL = "https://api.openf1.org/v1"

class OpenF1Client:
    def __init__(self, concurrency_limit: int = 5):
        self.semaphore = asyncio.Semaphore(concurrency_limit)
        self.session = None

    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    @retry(
        retry=retry_if_exception_type(aiohttp.ClientResponseError),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=2, max=10)
    )
    async def fetch(self, endpoint: str, params: dict = None) -> list:
        """
        Generic fetch method with retry logic for rate limits/errors.
        """
        if self.session is None:
             raise RuntimeError("Client session not initialized. Use 'async with' context.")

        url = f"{BASE_URL}/{endpoint}"
        
        async with self.semaphore:
            async with self.session.get(url, params=params) as response:
                if response.status == 429:
                    logger.warning(f"Rate limited on {endpoint}. Retrying...")
                    response.raise_for_status() # Trigger retry
                
                if response.status >= 500:
                     logger.warning(f"Server error {response.status} on {endpoint}. Retrying...")
                     response.raise_for_status()

                response.raise_for_status()
                
                # Check content type, some errors might return text/html instead of json
                if "application/json" not in response.headers.get("Content-Type", ""):
                     text = await response.text()
                     logger.error(f"Unexpected content type: {response.headers.get('Content-Type')}. Body: {text[:100]}")
                     raise aiohttp.ContentTypeError(response.request_info, response.history, message="Expected JSON response")

                return await response.json()

    async def fetch_all_pages(self, endpoint: str, params: dict = None):
         # OpenF1 typically returns all data in one go (streaming-like) or manageable chunks.
         # But if pagination is needed, logic goes here. 
         # Currently OpenF1 docs suggest large queries are just returned.
         return await self.fetch(endpoint, params)
