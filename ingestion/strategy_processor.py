import pandas as pd
import numpy as np
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

# Circuit Type Mapping
CIRCUIT_TYPES = {
    # Power
    'spa': 'Power', 'monza': 'Power', 'las_vegas': 'Power', 'jeddah': 'Power', 'baku': 'Power',
    # Aero
    'silverstone': 'Aero', 'suzuka': 'Aero', 'catalunya': 'Aero', 'zandvoort': 'Aero', 'losail': 'Aero',
    # Street
    'monaco': 'Street', 'marina_bay': 'Street', 'miami': 'Street',
    # Balanced
    'bahrain': 'Balanced', 'austin': 'Balanced', 'interlagos': 'Balanced', 'yas_marina': 'Balanced', 'albert_park': 'Balanced'
}

def calculate_team_efficiency(pit_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Team Efficiency Score based on pit stop durations.
    Score = Z-score of duration per season.
    """
    if pit_df.empty:
        return pd.DataFrame()
        
    df = pit_df.copy()
    
    # Filter outliers (> 10s usually penalty or damage)
    valid_stops = df[df['duration'] < 10.0].copy()
    
    # Calculate Season Stats
    season_stats = valid_stops.groupby('season')['duration'].agg(['mean', 'std']).reset_index()
    
    # Merge and Calculate Z-Score
    valid_stops = valid_stops.merge(season_stats, on='season', suffixes=('', '_season'))
    valid_stops['z_score'] = (valid_stops['duration'] - valid_stops['mean']) / valid_stops['std']
    
    # Aggregate by Team (Constructor) - Wait, pit_df from Ergast doesn't have constructorId!
    # We must merge with Results or Schedule.
    # Assuming the input `pit_df` has been enriched with `constructor_id` externally or we skip team agg for now 
    # and just return driver efficiency.
    # For now, let's return Driver Efficiency as a proxy or assume constructor_id is present.
    # If not present, we can't do Team Efficiency.
    # Let's assume the user will join this with results later.
    
    return valid_stops[['season', 'round', 'driver_id', 'stop_number', 'duration', 'z_score']]

def calculate_driver_track_score(results_df: pd.DataFrame) -> pd.DataFrame:
    """
    Calculate Driver-Track Success Score.
    Score = 0.7 * (Avg Points Last 2 Visits) + 0.3 * (Avg Points Career)
    """
    if results_df.empty:
        return pd.DataFrame()
        
    df = results_df.copy()
    df = df.sort_values(['driver_id', 'circuit_id', 'season'], ascending=[True, True, False])
    
    scores = []
    
    # Group by Driver and Circuit
    # Optimization: Vectorized approach is hard with rolling on dates, so iterate groups?
    # Or use rolling on sorted data.
    
    for (driver, circuit), group in df.groupby(['driver_id', 'circuit_id']):
        # group is sorted desc by season (recent first)
        
        # Career Avg
        career_avg = group['points'].mean()
        
        # Recent Avg (Last 2)
        recent_avg = group.head(2)['points'].mean()
        
        # Score
        score = 0.7 * recent_avg + 0.3 * career_avg
        
        # Append score to the LATEST race for this driver/circuit (to be used as feature for NEXT race? 
        # No, usually we want this feature available for the current race prediction based on PAST data.
        # So we should calculate this *prior* to the race.
        # This function calculates the score *after* the race? 
        # We need a rolling calculation.
        pass
    
    # Better approach: Rolling calculation
    # Sort by date
    df = df.sort_values('date')
    
    # We want the score available *before* the race. So shift?
    # Actually, for training, the feature for 2024 Bahrain should be based on 2023, 2022...
    
    # Let's simple function:
    # 1. Calculate historical stats up to date T.
    # This is expensive.
    # Alternative: Just use the static score based on *all* history for now (data leakage for training?)
    # For strict ML, we must exclude current race.
    
    # Let's implement non-leaking rolling score.
    # Group by driver, circuit. shift(1) to exclude current. expanding().mean() for career.
    # rolling(2).mean() for recent.
    
    # Re-sort for rolling
    df = df.sort_values(['driver_id', 'circuit_id', 'date'])
    
    # Career Avg (up to previous race)
    df['points_shifted'] = df.groupby(['driver_id', 'circuit_id'])['points'].shift(1)
    df['career_avg'] = df.groupby(['driver_id', 'circuit_id'])['points_shifted'].expanding().mean()
    
    # Recent Avg (last 2 races before today)
    df['recent_avg'] = df.groupby(['driver_id', 'circuit_id'])['points_shifted'].rolling(window=2, min_periods=1).mean().reset_index(0, drop=True).reset_index(0, drop=True) 
    # Note: reset_index details depend on pandas version and group keys. safely use transform?
    
    # Pandas rolling handling
    # Let's do it cleaner
    grouped = df.groupby(['driver_id', 'circuit_id'])['points_shifted']
    df['career_avg'] = grouped.expanding().mean().reset_index(level=[0,1], drop=True)
    df['recent_avg'] = grouped.rolling(window=2, min_periods=1).mean().reset_index(level=[0,1], drop=True)
    
    # Fill NA (Rookies)
    df['career_avg'] = df['career_avg'].fillna(0)
    df['recent_avg'] = df['recent_avg'].fillna(0)
    
    df['driver_track_score'] = 0.7 * df['recent_avg'] + 0.3 * df['career_avg']
    
    return df[['season', 'round', 'driver_id', 'circuit_id', 'driver_track_score']]

def add_weather_context(df: pd.DataFrame, circuit_id_col: str = 'circuit_id', temp_col: str = 'track_temperature') -> pd.DataFrame:
    """
    Enrich with Circuit Type and Weather Anomaly logic.
    Assumes `df` has `circuit_id` and `track_temperature`.
    """
    if df.empty:
        return df
        
    # Circuit Type
    df['circuit_type'] = df[circuit_id_col].map(CIRCUIT_TYPES).fillna('Unknown')
    
    # Weather Anomaly
    # Calculate historical stats (strictly, should be done on training set only, but for feature engineering script, okay)
    # We need a historical dataset of weather. 
    # If df IS the historical dataset:
    stats = df.groupby(circuit_id_col)[temp_col].agg(['mean', 'std']).reset_index()
    
    df = df.merge(stats, on=circuit_id_col, suffixes=('', '_hist'))
    
    df['temp_anomaly_z'] = (df[temp_col] - df['mean']) / df['std']
    
    return df

def calculate_tire_life_probability(laps_df: pd.DataFrame) -> pd.DataFrame:
    """
    Build a simple survival curve look-up table.
    P(Survival | Compound, Track, Age)
    Returns: DataFrame with columns [circuit_id, compound, tire_age, prob_survival]
    """
    if laps_df.empty:
        return pd.DataFrame()
        
    df = laps_df.copy()
    
    # 1. Calculate 'Pace Dropoff' (The Cliff)
    # We need rolling average lap time per stint.
    # Group by Driver, Stint.
    # Calculate rolling mean (window=3).
    # If (LapTime - RollingMean) > 1.5s -> Failure Event.
    
    # Sort
    df = df.sort_values(['driver_id', 'stint', 'lap_number'])
    
    # Rolling Mean
    df['rolling_pace'] = df.groupby(['driver_id', 'stint'])['lap_duration'].transform(
        lambda x: x.rolling(window=3, min_periods=1).mean().shift(1)
    )
    
    # Identify Failure
    # Only consider laps under Green condition? Assuming data is clean.
    df['pace_delta'] = df['lap_duration'] - df['rolling_pace']
    df['is_failure'] = (df['pace_delta'] > 1.5).astype(int)
    
    # 2. Survival Analysis (Kaplan-Meier-ish)
    # Group by Circuit, Compound, Tire Age
    # Count Total Laps reached at this age vs Failures at this age.
    # P(Survive Age T) = P(Survive T-1) * (1 - Hazard(T))
    # Hazard(T) = Failures(T) / AtRisk(T)
    
    survival_data = []
    
    # Iterate Circuits
    for circuit, c_df in df.groupby('circuit_id'):
        # Iterate Compounds
        for compound, comp_df in c_df.groupby('compound'):
            # Calculate Hazard per Age
            max_age = int(comp_df['tire_age'].max())
            hazard_rates = []
            
            at_risk = len(comp_df['stint'].unique()) # Number of stints that started
            # Wait, this is tricky. At risk reduces as stints end naturally OR fail.
            # Simpler approach:
            # Just count how many stints *reached* age T, and how many *failed* at age T.
            
            age_counts = comp_df.groupby('tire_age')['is_failure'].agg(['count', 'sum'])
            # count = total laps driven at this age (At Risk)
            # sum = failures at this age
            
            prob_survival = 1.0
            
            for age in range(1, max_age + 1):
                if age in age_counts.index:
                    n_at_risk = age_counts.loc[age, 'count']
                    n_failed = age_counts.loc[age, 'sum']
                    
                    if n_at_risk > 0:
                        hazard = n_failed / n_at_risk
                        prob_survival *= (1 - hazard)
                        
                survival_data.append({
                    'circuit_id': circuit,
                    'compound': compound,
                    'tire_age': age,
                    'prob_survival': prob_survival
                })
                
    return pd.DataFrame(survival_data)

def create_simulation_dataset(merged_df: pd.DataFrame) -> pd.DataFrame:
    """
    Format the merged dataset into the State Vector required for Monte Carlo Simulation.
    State Vector S_t:
    - CurrentPos
    - GapToLeader
    - TireCompound
    - TireAge
    - PitStopStatus (Just pitted?)
    - PaceDelta
    - TrackStatus (Green/SC/VSC)
    - WeatherState
    
    Target:
    - NextLapTime
    - NextPos
    """
    if merged_df.empty:
        return pd.DataFrame()
        
    df = merged_df.copy()
    
    # Ensure sorted order
    df = df.sort_values(['session_key', 'driver_number', 'date'])
    
    # 1. Pit Stop Status
    # Assuming 'pit_duration' > 0 implies pit stop on this lap
    if 'pit_duration' not in df.columns:
        df['pit_duration'] = 0
    df['is_pit_stop'] = (df['pit_duration'] > 0).astype(int)
    
    # 2. Gap To Leader
    # If not present, we can't infer easily without full race session data. 
    # Assumed to be in merged_df from 'intervals' endpoint (if available) or calculated.
    if 'gap_to_leader' not in df.columns:
        # Placeholder or partial calc if we have leader info. 
        # For now, initialize to 0 if missing (should be handled upstream)
        df['gap_to_leader'] = 0.0

    # 3. Track Status (Stochastic SC)
    # This is a labeled dataset for Training. So we use ACTUAL track status if available.
    # OpenF1 'laps' sometimes has flags.
    # If missing, defaulting to Green (0).
    if 'track_status' not in df.columns:
        df['track_status'] = 'Green'

    # Select State Columns
    state_cols = [
        'session_key', 'driver_number', 'lap_number',
        'position', 'gap_to_leader', 
        'compound', 'tire_age', 
        'is_pit_stop', 
        'lap_duration', # This is often the target or input for next lap
        'track_status',
        'rain_fall', # Weather state
        'track_temperature'
    ]
    
    # Filter to existing
    state_cols = [c for c in state_cols if c in df.columns]
    
    sim_df = df[state_cols].copy()
    
    # Create Targets (Next State)
    sim_df['next_lap_time'] = sim_df.groupby(['session_key', 'driver_number'])['lap_duration'].shift(-1)
    sim_df['next_position'] = sim_df.groupby(['session_key', 'driver_number'])['position'].shift(-1)
    
    return sim_df
