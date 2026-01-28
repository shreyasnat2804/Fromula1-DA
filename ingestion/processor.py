import pandas as pd
import logging
from ingestion.cleaning import smooth_telemetry, calculate_tire_age_adjusted, calculate_interval_delta

logger = logging.getLogger(__name__)

def merge_data(car_df: pd.DataFrame, laps_df: pd.DataFrame, weather_df: pd.DataFrame) -> pd.DataFrame:
    """
    Merges car data with laps and weather data using merge_asof.
    Assumes all DataFrames have a 'date' column in datetime64[ns].
    """
    if car_df.empty:
        logger.warning("Car dataframe is empty. Cannot merge.")
        return pd.DataFrame()

    # Ensure timestamps are sorted (required for merge_asof)
    car_df = car_df.sort_values('date')
    
    if not laps_df.empty:
        laps_merge = laps_df.sort_values('date_start').rename(columns={'date_start': 'date'})
        
        # Select columns to merge
        # Ensure we keep 'stnt' (stint) for tire age calc
        cols_to_merge = ['date', 'lap_number', 'is_pit_out_lap']
        cols_to_merge = [c for c in cols_to_merge if c in laps_merge.columns]
        laps_merge = laps_merge[cols_to_merge]
        
        merged_df = pd.merge_asof(
            car_df, 
            laps_merge, 
            on='date', 
            direction='backward'
        )
    else:
        merged_df = car_df
        
    if not weather_df.empty:
        weather_df = weather_df.sort_values('date')
        w_cols = ['date', 'air_temperature', 'track_temperature', 'humidity', 'rainfall']
        w_cols = [c for c in w_cols if c in weather_df.columns]
        weather_merge = weather_df[w_cols]
        
        merged_df = pd.merge_asof(
            merged_df,
            weather_merge,
            on='date',
            direction='backward'
        )
        
    return merged_df

def interpolate_missing(df: pd.DataFrame) -> pd.DataFrame:
    """
    Interpolate missing values for continuous variables.
    """
    continuous_cols = ['speed', 'rpm', 'throttle', 'brake', 'drs'] 
    categorical_cols = ['gear'] 
    
    continuous_cols = [c for c in continuous_cols if c in df.columns]
    categorical_cols = [c for c in categorical_cols if c in df.columns]
    
    if continuous_cols:
        df[continuous_cols] = df[continuous_cols].interpolate(method='linear', limit_direction='both')
        
    if categorical_cols:
         df[categorical_cols] = df[categorical_cols].ffill()
         
    return df

def process_features(car_df: pd.DataFrame, laps_df: pd.DataFrame, weather_df: pd.DataFrame) -> pd.DataFrame:
    """
    Orchestrate the full Feature Engineering pipeline:
    Merge -> Interpolate -> Smooth -> Create Features
    """
    # 1. Merge
    df = merge_data(car_df, laps_df, weather_df)
    
    if df.empty:
        return df

    # 2. Interpolate
    df = interpolate_missing(df)
    
    # 3. Smooth Telemetry
    df = smooth_telemetry(df)
    
    # 4. Feature Creation
    df = calculate_tire_age_adjusted(df)
    df = calculate_interval_delta(df) # logic might be placeholder if 'gap_to_leader' missing, but we run it
    
    return df
