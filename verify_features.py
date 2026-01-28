import pandas as pd
import logging
from ingestion.processor import process_features

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_DIR = "data/raw"
SESSION_KEY = 9662
DRIVER_NUMBER = 55

def main():
    logger.info("Loading parquet files...")
    try:
        car_df = pd.read_parquet(f"{DATA_DIR}/car_{SESSION_KEY}_{DRIVER_NUMBER}.parquet")
        laps_df = pd.read_parquet(f"{DATA_DIR}/laps_{SESSION_KEY}.parquet")
        weather_df = pd.read_parquet(f"{DATA_DIR}/weather_{SESSION_KEY}.parquet")
    except FileNotFoundError as e:
        logger.error(f"Could not load data: {e}")
        return

    logger.info("Processing features (Merge + Smooth + Eng)...")
    df = process_features(car_df, laps_df, weather_df)
    
    if not df.empty:
        logger.info(f"Processed DataFrame Shape: {df.shape}")
        
        # Check Smoothing
        if 'speed_smooth' in df.columns:
            logger.info("Smoothing verified: 'speed_smooth' column present.")
            # Simple check: variance of smooth should be <= variance of raw (roughly)
            raw_var = df['speed'].var()
            smooth_var = df['speed_smooth'].var()
            logger.info(f"Speed Var: {raw_var:.2f}, Smooth Var: {smooth_var:.2f}")
            
        # Check Tire Age
        if 'tire_age_adj' in df.columns:
            logger.info("Tire Age verified: 'tire_age_adj' column present.")
            # Print sample
            sample = df[['lap_number', 'stnt', 'tire_age_adj']].dropna().head(5)
            logger.info(f"Sample Tire Age:\n{sample.to_string()}")
            
        # Check Interval Delta
        if 'interval_delta' in df.columns:
             logger.info("Interval Delta verified: 'interval_delta' column present.")
        else:
             logger.info("Interval Delta NOT present (likely missing upstream data).")
             
    else:
        logger.error("Processed DataFrame is empty.")

if __name__ == "__main__":
    main()
