import pandas as pd
import numpy as np
from scipy.signal import savgol_filter
import logging

logger = logging.getLogger(__name__)

def smooth_telemetry(df: pd.DataFrame, window_length: int = 11, polyorder: int = 3) -> pd.DataFrame:
    """
    Apply Savitzky-Golay filter to smooth telemetry data.
    """
    # Columns to smooth
    cols = ['speed', 'rpm', 'throttle', 'brake']
    
    # Filter to cols that exist
    target_cols = [c for c in cols if c in df.columns]
    
    if not target_cols:
        return df
        
    df_clean = df.copy()
    
    for col in target_cols:
        try:
            # savgol_filter requires window_length <= len(x)
            if len(df_clean) > window_length:
                df_clean[f'{col}_smooth'] = savgol_filter(df_clean[col], window_length, polyorder)
            else:
                df_clean[f'{col}_smooth'] = df_clean[col]
        except Exception as e:
            logger.warning(f"Failed to smooth {col}: {e}")
            df_clean[f'{col}_smooth'] = df_clean[col]
            
    return df_clean

def calculate_tire_age_adjusted(df: pd.DataFrame, track_temp: float = 30.0) -> pd.DataFrame:
    """
    Calculate relative tire age adjusted by track temperature.
    Logic: age = laps_since_pit * (1 + 0.05 * (track_temp - 30))
    This assumes df has 'lap_number' and we can infer stints.
    
    Note: Real implementation needs strict stint identification (pit_in/out).
    For now, we will use 'stnt' column (stint number) if available from laps joined.
    """
    if 'stnt' not in df.columns:
        if 'is_pit_out_lap' in df.columns:
            # Infer stint: Increment stint counter every time is_pit_out_lap is true
            # Assuming df is sorted by date/lap
            df['stnt'] = df['is_pit_out_lap'].fillna(0).astype(int).cumsum()
        else:
            logger.warning("Missing 'stnt' and 'is_pit_out_lap' for tire age calc.")
            return df
        
    # Calculate laps within the current stint
    # Group by stint and calculate cumulative count or min lap of stint
    # Fix: ensure lap_number is numeric
    if 'lap_number' in df.columns:
         df['laps_since_pit'] = df.groupby('stnt')['lap_number'].transform(lambda x: x - x.min())
    else:
         return df
    
    # Adjustment factor
    # If track_temp is a column (dynamic), use it, otherwise constant
    if 'track_temperature' in df.columns:
        temp = df['track_temperature']
    else:
        temp = track_temp
        
    df['tire_age_adj'] = df['laps_since_pit'] * (1 + 0.05 * (temp - 30.0))
    
    return df

def calculate_interval_delta(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate interval delta from rolling average.
    Requires 'gap_to_leader' or similar. 
    OpenF1 'intervals' endpoint gives 'gap_to_leader'.
    We want Delta to Car Ahead usually, or Delta to predicted.
    
    Let's assume we merged 'gap_to_leader'.
    """
    if 'gap_to_leader' not in df.columns:
        return df
        
    # Example logic: Delta to historical avg of this driver on this lap? 
    # Or simplified: Rate of change of gap?
    # User request: "Interval Delta (real-time gap to car ahead vs. historical average)"
    
    # We might need 'interval' (gap to car ahead) column found in intervals endpoint data
    if 'interval' in df.columns:
        # Rolling average of gap (last 3 laps approx? or last 60 seconds?)
        # Since data is time-series, rolling 4Hz * 60s = 240 samples
        df['interval_avg_60s'] = df['interval'].rolling(window=240, min_periods=1).mean()
        df['interval_delta'] = df['interval'] - df['interval_avg_60s']
        
    return df
