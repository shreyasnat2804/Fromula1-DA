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

def fetch_results(season: int) -> pd.DataFrame:
    """
    Fetch all race results for a given season using the Ergast API (via Jolpica).
    Pagination is handled by iterating rounds, or fetching season full list?
    Ergast season endpoint: /2023/results.json usually returns only limited per-page.
    Limit is usually 30. Races have ~20 drivers * 24 races = 480 rows.
    We can fetch limits=1000 to get whole season? No, usually limit applies to 'races'.
    Let's iterate rounds for safety.
    """
    results_list = []
    
    # Get schedule first to know how many rounds
    try:
        schedule_url = f"{API_BASE}/{season}.json"
        resp = requests.get(schedule_url, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        total_rounds = int(data['MRData']['RaceTable']['Races'][-1]['round'])
    except Exception as e:
        logger.error(f"Failed to fetch schedule for {season}: {e}")
        return pd.DataFrame()

    logger.info(f"Season {season}: {total_rounds} rounds detected.")

    for r in range(1, total_rounds + 1):
        try:
            url = f"{API_BASE}/{season}/{r}/results.json"
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            race_data = resp.json()['MRData']['RaceTable']['Races']
            
            if not race_data:
                continue
                
            race = race_data[0] # The race object
            circuit = race['Circuit']
            date = race['date']
            race_name = race['raceName']
            
            for result in race['Results']:
                # Extract relevant fields
                row = {
                    'season': season,
                    'round': r,
                    'race_name': race_name,
                    'date': date,
                    'circuit_id': circuit['circuitId'],
                    'driver_id': result['Driver']['driverId'],
                    'driver_code': result['Driver'].get('code'),
                    'constructor_id': result['Constructor']['constructorId'],
                    'grid_position': int(result['grid']),
                    'finish_position_text': result['positionText'], # R, D, or number
                    'finish_position': int(result['position']) if result['position'].isdigit() else None,
                    'points': float(result['points']),
                    'status': result['status']
                }
                
                # Calculate Delta
                if row['finish_position'] and row['grid_position'] > 0:
                     row['position_change'] = row['grid_position'] - row['finish_position']
                else:
                     row['position_change'] = None
                     
                results_list.append(row)
                
            # Be nice to the API
            time.sleep(0.2)
            
        except Exception as e:
            logger.error(f"Error fetching {season} round {r}: {e}")
            
    return pd.DataFrame(results_list)

def main():
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    
    all_seasons = []
    
    # 2014 to 2024 (or later)
    # Adjust range as needed.
    start_year = 2014
    end_year = 2025 # Include 2025 if new season started? Assuming current date is Jan 2026, 2025 is complete.
    
    for year in range(start_year, end_year + 1):
        logger.info(f"Processing season {year}...")
        df = fetch_results(year)
        if not df.empty:
            all_seasons.append(df)
            
    if all_seasons:
        final_df = pd.concat(all_seasons, ignore_index=True)
        # Parquet for efficiency
        output_file = DATA_DIR / f"race_results_{start_year}_{end_year}.parquet"
        final_df.to_parquet(output_file, index=False)
        logger.info(f"Saved {len(final_df)} rows to {output_file}")
        
        # Calculate Grid Conversion Probability
        logger.info("Calculating Grid Conversion Metrics...")
        analyze_grid_conversion(final_df)
    else:
        logger.warning("No data fetched.")

def analyze_grid_conversion(df: pd.DataFrame):
    """
    Calculate gain/loss probability per grid slot.
    """
    # Filter valid finishes and grid starts
    valid_df = df.dropna(subset=['position_change', 'grid_position'])
    valid_df = valid_df[valid_df['grid_position'] > 0] # Pit starts usually 0 or handled elsewhere
    
    stats = valid_df.groupby('grid_position')['position_change'].agg(
        count='count',
        mean_change='mean',
        prob_gain=lambda x: (x > 0).mean(),
        prob_loss=lambda x: (x < 0).mean()
    ).reset_index()
    
    # Print formatted
    logger.info("Grid Conversion Stats (2014-Present):")
    print(stats.to_string(index=False))
    
    # Save stats
    stats.to_csv(DATA_DIR / "grid_conversion_stats.csv", index=False)

if __name__ == "__main__":
    main()
