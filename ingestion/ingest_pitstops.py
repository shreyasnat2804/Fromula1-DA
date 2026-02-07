import requests
import pandas as pd
import logging
import time
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Jolpica / Ergast API Base
API_BASE = "https://api.jolpi.ca/ergast/f1"
DATA_DIR = Path("data/strategy")

def fetch_pitstops(season: int) -> pd.DataFrame:
    """
    Fetch all pit stops for a given season.
    Ergast endpoint: /2023/pitstops.json (limit=30, offset=0)
    We need to paginate heavily as there are ~40-80 stops per race * 20 races = 800-1600 stops.
    It's better to iterate by round to avoid massive offsets and huge requests.
    """
    stops_list = []
    
    # Get schedule first to know how many rounds
    for attempt in range(5):
        try:
            schedule_url = f"{API_BASE}/{season}.json"
            resp = requests.get(schedule_url, timeout=10)
            if resp.status_code == 429:
                sleep_time = (2 ** attempt) + 1
                logger.warning(f"Rate limited on schedule {season}. Sleeping {sleep_time}s...")
                time.sleep(sleep_time)
                continue
            resp.raise_for_status()
            data = resp.json()
            total_rounds = int(data['MRData']['RaceTable']['Races'][-1]['round'])
            break
        except Exception as e:
            logger.error(f"Failed to fetch schedule for {season}: {e}")
            time.sleep(1)
    else:
        return pd.DataFrame()

    logger.info(f"Season {season}: {total_rounds} rounds detected.")

    for r in range(1, total_rounds + 1):
        url = f"{API_BASE}/{season}/{r}/pitstops.json?limit=100"
        
        # Retry loop
        for attempt in range(5):
            try:
                resp = requests.get(url, timeout=10)
                if resp.status_code == 429:
                    # Exponential backoff
                    sleep_time = (2 ** attempt) + 0.5
                    logger.warning(f"Rate limited (429) on {season} R{r}. Sleeping {sleep_time}s...")
                    time.sleep(sleep_time)
                    continue
                
                resp.raise_for_status()
                race_data = resp.json()['MRData']['RaceTable']['Races']
                break # Success
            except requests.exceptions.RequestException as e:
                logger.error(f"Request failed {url}: {e}")
                time.sleep(1)
        else:
            logger.error(f"Failed to fetch {season} round {r} after retries.")
            continue
            
        try:
            if not race_data:
                continue
                
            race = race_data[0] # The race object
            # Race info isn't in the PitStop list directly usually, but in the Race object
            race_name = race['raceName']
            date = race['date']
            circuit_id = race['Circuit']['circuitId'] if 'Circuit' in race else None 
            # Note: Jolpica/Ergast sometimes nests Circuit inside Race, sometimes not for pitstops endpoint depending on query.
            # Let's double check structure. Usually PitStops endpoint is: RaceTable -> Races[0] -> PitStops
            
            if 'PitStops' not in race:
                continue

                row = {
                    'season': season,
                    'round': r,
                    'race_name': race_name,
                    'date': date,
                    'driver_id': stop['driverId'],
                    'lap': int(stop['lap']),
                    'stop_number': int(stop['stop']),
                    'time': stop['time'],
                    'duration_str': stop['duration'] # Keep original string just in case
                }
                
                # Handling duration parsing
                # Usually 'duration' is "21.565" (seconds) or "1:02.123" (minutes:seconds)
                # 'milliseconds' field is safest if available as integer ms.
                
                if 'milliseconds' in stop:
                     try:
                        row['duration'] = int(stop['milliseconds']) / 1000.0
                     except:
                        row['duration'] = None
                else:
                    # Parse string duration "21.565" or "1:02.123"
                    d_str = stop['duration']
                    try:
                        if ':' in d_str:
                            parts = d_str.split(':')
                            row['duration'] = float(parts[0]) * 60 + float(parts[1])
                        else:
                            row['duration'] = float(d_str)
                    except:
                        row['duration'] = None

                stops_list.append(row)
                
            # Be nice to the API
            time.sleep(0.2)
            
        except Exception as e:
            logger.error(f"Error fetching {season} round {r} pitstops: {e}")
            
    return pd.DataFrame(stops_list)

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    all_seasons = []
    
    # Pit stop data is available from 2012 onwards in Ergast
    start_year = 2014 # Align with our scope
    end_year = 2025
    
    for year in range(start_year, end_year + 1):
        logger.info(f"Processing season {year}...")
        df = fetch_pitstops(year)
        if not df.empty:
            all_seasons.append(df)
            
    if all_seasons:
        final_df = pd.concat(all_seasons, ignore_index=True)
        # Parquet
        output_file = DATA_DIR / f"pit_stops_{start_year}_{end_year}.parquet"
        final_df.to_parquet(output_file, index=False)
        logger.info(f"Saved {len(final_df)} rows to {output_file}")
        
        # Calculate Team Efficiency
        # We need a mapping of Driver -> Team (Constructor) for each season.
        # This isn't in the PitSteps endpoint. We can get it from the Results parquet we just made!
        # or fetch separately.
        # For now, let's just save the raw pit stops.
        # We will do the merging in a separate step or script.
    else:
        logger.warning("No data fetched.")

if __name__ == "__main__":
    main()
