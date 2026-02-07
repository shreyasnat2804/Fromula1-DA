import requests
import pandas as pd
import logging
import time
import random
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_BASE = "https://api.jolpi.ca/ergast/f1"
DATA_DIR = Path("data/strategy")

def fetch_pitstops(start_year: int, end_year: int):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    all_seasons = []

    for season in range(start_year, end_year + 1):
        logger.info(f"Starting Season {season}...")
        
        # Friendly delay before starting a season
        time.sleep(random.uniform(5.0, 10.0))
        
        # Get schedule
        try:
            schedule_url = f"{API_BASE}/{season}.json"
            resp = requests.get(schedule_url, timeout=20)
            if resp.status_code == 429:
                logger.warning("Hit 429 on schedule. Sleeping 60s...")
                time.sleep(60)
                resp = requests.get(schedule_url, timeout=20)
            
            resp.raise_for_status()
            total_rounds = int(resp.json()['MRData']['RaceTable']['Races'][-1]['round'])
            logger.info(f"Season {season}: {total_rounds} rounds.")
        except Exception as e:
            logger.error(f"Failed to fetch schedule for {season}: {e}")
            continue

        season_stops = []
        
        for r in range(1, total_rounds + 1):
            # HUMANISTIC DELAY: 5 to 15 seconds between requests
            delay = random.uniform(5.0, 15.0)
            logger.info(f"Fetching {season} R{r}... (sleeping {delay:.1f}s)")
            time.sleep(delay)
            
            url = f"{API_BASE}/{season}/{r}/pitstops.json?limit=100"
            
            # Retry loop with long backoff
            for attempt in range(5):
                try:
                    resp = requests.get(url, timeout=20)
                    if resp.status_code == 429:
                        wait = (2 ** attempt) * 10 + random.uniform(0, 5)
                        logger.warning(f"429 at {season} R{r}. Backing off for {wait:.1f}s")
                        time.sleep(wait)
                        continue
                    
                    resp.raise_for_status()
                    race_data = resp.json()['MRData']['RaceTable']['Races']
                    
                    if race_data:
                        race = race_data[0]
                        race_name = race['raceName']
                        date = race['date']
                        
                        if 'PitStops' in race:
                            for stop in race['PitStops']:
                                row = {
                                    'season': season,
                                    'round': r,
                                    'race_name': race_name,
                                    'date': date,
                                    'driver_id': stop['driverId'],
                                    'lap': int(stop['lap']),
                                    'stop_number': int(stop['stop']),
                                    'time': stop['time'],
                                    'duration_str': stop['duration']
                                }
                                # Parse duration
                                if 'milliseconds' in stop:
                                     try: row['duration'] = int(stop['milliseconds']) / 1000.0
                                     except: row['duration'] = None
                                else:
                                    try: 
                                        d = stop['duration']
                                        if ':' in d:
                                            p = d.split(':')
                                            row['duration'] = float(p[0])*60 + float(p[1])
                                        else: row['duration'] = float(d)
                                    except: row['duration'] = None
                                    
                                season_stops.append(row)
                    break # Success
                except Exception as e:
                    logger.error(f"Error {season} R{r}: {e}")
                    time.sleep(5)
            
        # Save per season to be safe
        if season_stops:
            df = pd.DataFrame(season_stops)
            path = DATA_DIR / f"pitstops_{season}.parquet"
            df.to_parquet(path, index=False)
            logger.info(f"Saved {len(df)} stops for {season} to {path}")
            all_seasons.append(df)

if __name__ == "__main__":
    # Part 2: 2020 to 2025
    logger.info("Starting Part 2 Ingestion (2020-2025). This will take a while.")
    fetch_pitstops(2020, 2025)
    logger.info("Part 2 Complete.")
