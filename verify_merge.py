import pandas as pd
import logging
from ingestion.processor import merge_data

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = "data/raw"
SESSION_KEY = 9662
DRIVER_NUMBER = 55 # Matches what we saw in the logs

def main():
    logger.info("Loading parquet files...")
    try:
        car_df = pd.read_parquet(f"{DATA_DIR}/car_{SESSION_KEY}_{DRIVER_NUMBER}.parquet")
        laps_df = pd.read_parquet(f"{DATA_DIR}/laps_{SESSION_KEY}.parquet")
        weather_df = pd.read_parquet(f"{DATA_DIR}/weather_{SESSION_KEY}.parquet")
    except FileNotFoundError as e:
        logger.error(f"Could not load data: {e}")
        return

    logger.info("Merging data...")
    merged_df = merge_data(car_df, laps_df, weather_df)
    
    if not merged_df.empty:
        logger.info("Merge successful!")
        logger.info(f"Merged Shape: {merged_df.shape}")
        logger.info("Head of merged data:")
        print(merged_df[['date', 'rpm', 'speed', 'lap_number', 'air_temperature']].head().to_markdown())
    else:
        logger.error("Merged DataFrame is empty.")

if __name__ == "__main__":
    main()
