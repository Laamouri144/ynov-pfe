import pandas as pd
import numpy as np
import os

# Configuration
INPUT_FILE = r'c:\Users\laamo\ynov-pfe\scripts\Airline_Delay_Cause_Cpt.csv'
OUTPUT_FILE = r'c:\Users\laamo\ynov-pfe\data\Nifi_Templates_1500.csv'

def generate_templates():
    print(f"Reading source: {INPUT_FILE}")
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print(f"Error: Source file not found at {INPUT_FILE}")
        return

    print("Generating expanded templates...")
    
    # Ensure critical columns exist and clean data
    required_cols = ['year', 'month', 'carrier', 'carrier_name', 'airport', 'airport_name', 
                     'arr_flights', 'arr_del15', 'arr_delay', 
                     'carrier_delay', 'weather_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay',
                     'weather_ct', 'security_ct'] # needed for validators
    
    # Fill missing cols with 0 if needed (robustness)
    for col in required_cols:
        if col not in df.columns and col not in ['year', 'month']: # year/month might be there
            df[col] = 0

    # Clean
    df = df.dropna(subset=['carrier', 'airport'])
    
    # We need ~1500 templates. 
    # Strategy: Take observed valid routes, and create varied templates for them.
    
    templates = []
    
    # If original DF is small, we replicate. If large, we sample.
    # The source has 300k+ rows, so we can just sample distinct routes and varied conditions.
    
    # 1. Get unique route/month combinations to preserve seasonality basics
    # But we want to "expand" to ensure we have enough diversity.
    
    # Let's take a sample of 2000 rows to be safe, ensuring good mix
    sample_df = df.sample(n=min(2000, len(df)), random_state=42).copy()
    
    # Add a template_id
    sample_df['template_id'] = range(1, len(sample_df) + 1)
    
    # 2. Add some variation to the "base" values to make them average templates rather than exact records
    # This isn't strictly necessary as the NiFi UpdateRecord will apply random variation,
    # but it helps to have clean "base" numbers.
    
    # Ensure they are numeric
    cols_to_numeric = ['arr_flights', 'arr_del15', 'arr_delay', 'carrier_delay', 'weather_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay']
    for col in cols_to_numeric:
        sample_df[col] = pd.to_numeric(sample_df[col], errors='coerce').fillna(0)
    
    # Write to output
    os.makedirs(os.path.dirname(OUTPUT_FILE), exist_ok=True)
    
    print(f"Writing {len(sample_df)} templates to {OUTPUT_FILE}...")
    sample_df.to_csv(OUTPUT_FILE, index=False)
    print("Done.")

if __name__ == "__main__":
    generate_templates()
