import pandas as pd
import json
import numpy as np
import os

# Configuration
INPUT_FILE = r'c:\Users\laamo\ynov-pfe\scripts\Airline_Delay_Cause_Cpt.csv'
OUTPUT_SCRIPT = r'c:\Users\laamo\ynov-pfe\scripts\nifi_generator.groovy'

def analyze_and_build():
    print(f"Reading {INPUT_FILE}...")
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print(f"Error: File not found at {INPUT_FILE}")
        return

    print("Analyzing data structure and correlations (Deep Dive)...")
    
    # Clean data
    df = df.dropna(subset=['carrier', 'carrier_name', 'airport', 'airport_name', 'month'])
    
    # --- 1. Valid Routes (Contexts) ---
    valid_routes = df[['carrier', 'carrier_name', 'airport', 'airport_name']].drop_duplicates().to_dict('records')
    print(f"Found {len(valid_routes)} valid carrier-airport routes.")

    # --- 2. Seasonality (Month -> Delay Probability Multiplier) ---
    monthly_stats = df.groupby('month').apply(
        lambda x: x['arr_del15'].sum() / x['arr_flights'].sum() if x['arr_flights'].sum() > 0 else 0
    ).to_dict()
    avg_rate = np.mean(list(monthly_stats.values()))
    seasonality_map = {str(m): round(rate / avg_rate, 2) for m, rate in monthly_stats.items()}
    
    # --- 3. Airport Profiles ---
    airport_profiles = {}
    delay_cols = ['carrier_delay', 'weather_delay', 'nas_delay', 'security_delay', 'late_aircraft_delay']
    airport_groups = df.groupby('airport')
    
    for airport, group in airport_groups:
        vol_mean = group['arr_flights'].mean()
        vol_std = group['arr_flights'].std()
        
        flights_stats = {
            'mean': round(vol_mean, 1) if not pd.isna(vol_mean) else 0.0,
            'std': round(vol_std, 1) if not pd.isna(vol_std) else 0.0
        }
        
        total_delay_minutes = group['arr_delay'].sum()
        cause_dist = {}
        if total_delay_minutes > 0:
            for col in delay_cols:
                cause_dist[col] = round(group[col].sum() / total_delay_minutes, 3)
        else:
            cause_dist = {c: 0.2 for c in delay_cols}
            
        airport_profiles[airport] = {
            'volume': flights_stats,
            'causes': cause_dist,
            'base_delay_rate': round(group['arr_del15'].sum() / group['arr_flights'].sum(), 3) if group['arr_flights'].sum() > 0 else 0.0
        }

    # --- 4. Carrier Profiles ---
    global_rate = df['arr_del15'].sum() / df['arr_flights'].sum()
    carrier_performance = df.groupby('carrier').apply(
        lambda x: (x['arr_del15'].sum() / x['arr_flights'].sum()) / global_rate if x['arr_flights'].sum() > 0 else 1.0
    ).to_dict()
    carrier_performance = {k: round(v, 2) for k, v in carrier_performance.items()}

    print("Generating Groovy script code...")

    # BOILERPLATE GROOVY
    # We embed the JSON data directly into the script string
    script_content = f'''import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter

/*
 * GENERATED NIFI SCRIPT
 * Purpose: Infinite stream generation based on statistical models
 * Usage: Use in ExecuteScript processor. 
 *        Connect a "GenerateFlowFile" (timer 1 sec) -> ExecuteScript
 */

def flowFile = session.get()
if (!flowFile) return

try {{
    // --- 1. Load Statistical Models (Embedded JSON) ---
    def jsonSlurper = new JsonSlurper()
    
    def VALID_ROUTES = jsonSlurper.parseText(\'\'\'{json.dumps(valid_routes)}\'\'\')
    def SEASONALITY = jsonSlurper.parseText(\'\'\'{json.dumps(seasonality_map)}\'\'\')
    def AIRPORT_PROFILES = jsonSlurper.parseText(\'\'\'{json.dumps(airport_profiles)}\'\'\')
    def CARRIER_ALPHAS = jsonSlurper.parseText(\'\'\'{json.dumps(carrier_performance)}\'\'\')
    
    // --- 2. Generation Logic ---
    def random = new Random()
    def records = []
    
    // Generate 1 to 5 records per execution trigger
    def numRecords = random.nextInt(5) + 1
    
    for (int i = 0; i < numRecords; i++) {{
        // A. Context
        def ctx = VALID_ROUTES[random.nextInt(VALID_ROUTES.size())]
        def airport = ctx.airport
        def carrier = ctx.carrier
        
        // B. Profiles
        def apoProf = AIRPORT_PROFILES[airport]
        // Fallback if missing
        if (!apoProf) {{
             apoProf = [volume: [mean: 50, std: 10], causes: [carrier_delay: 0.2, weather_delay: 0.2, nas_delay: 0.2, security_delay: 0.2, late_aircraft_delay: 0.2], base_delay_rate: 0.2]
        }}
        
        def carrAlpha = CARRIER_ALPHAS[carrier] ?: 1.0
        
        // C. Time & Seasonality
        def now = LocalDateTime.now()
        def monthStr = String.valueOf(now.getMonthValue())
        def monthMult = SEASONALITY[monthStr] ?: 1.0
        
        // D. Volume
        def mu = apoProf.volume.mean
        def sigma = apoProf.volume.std
        def val = (random.nextGaussian() * sigma) + mu
        def arr_flights = Math.max(10, val.toInteger())
        
        // E. Cancel/Divert
        def arr_cancelled = 0
        if (random.nextDouble() < (0.02 * monthMult)) {{
            arr_cancelled = (arr_flights * (random.nextDouble() * 0.09 + 0.01)).toInteger()
        }}
        
        def arr_diverted = 0
        if (random.nextDouble() < 0.005) {{
            arr_diverted = (arr_flights * (random.nextDouble() * 0.04 + 0.01)).toInteger()
        }}
        
        // F. Delays
        def effective_flights = arr_flights - arr_cancelled - arr_diverted
        def prob_delay = apoProf.base_delay_rate * carrAlpha * monthMult
        prob_delay = Math.max(0.01, Math.min(0.95, prob_delay))
        
        def arr_del15 = 0
        if (effective_flights > 0) {{
             def instance_prob = Math.max(0.0, Math.min(1.0, prob_delay + (random.nextDouble() * 0.1 - 0.05)))
             arr_del15 = (effective_flights * instance_prob).toInteger()
        }}
        
        // G. Delay Minutes & Causes
        def total_delay_minutes = 0
        def causes_map = [
            carrier_delay: 0, weather_delay: 0, nas_delay: 0, security_delay: 0, late_aircraft_delay: 0,
            carrier_ct: 0.0, weather_ct: 0.0, nas_ct: 0.0, security_ct: 0.0, late_aircraft_ct: 0.0
        ]
        
        if (arr_del15 > 0) {{
            def avg_delay = 30 + (random.nextDouble() * 45) // 30-75 mins
            total_delay_minutes = (arr_del15 * avg_delay).toInteger()
            
            def remaining = total_delay_minutes
            def causeKeys = new ArrayList(apoProf.causes.keySet())
            
            for (int k = 0; k < causeKeys.size(); k++) {{
                def causeKey = causeKeys[k]
                def baseShare = apoProf.causes[causeKey]
                
                def minutes = 0
                if (k == causeKeys.size() - 1) {{
                    minutes = remaining
                }} else {{
                    def share = baseShare * (0.8 + (random.nextDouble() * 0.4)) // 0.8 to 1.2
                    minutes = (total_delay_minutes * share).toInteger()
                    minutes = Math.min(minutes, remaining)
                }}
                
                causes_map[causeKey] = minutes
                remaining -= minutes
                
                // Count calc
                if (minutes > 0) {{
                     def ctKey = causeKey.replace('_delay', '_ct')
                     def divisor = 40 + (random.nextDouble() * 20)
                     def ctVal = minutes / divisor
                     causes_map[ctKey] = Math.round(ctVal * 100.0) / 100.0
                }}
            }}
        }}

        // H. Build Record Map
        def record = [
            id: UUID.randomUUID().toString(),
            year: now.getYear(),
            month: now.getMonthValue(),
            carrier: carrier,
            carrier_name: ctx.carrier_name,
            airport: airport,
            airport_name: ctx.airport_name,
            arr_flights: arr_flights,
            arr_del15: arr_del15,
            arr_cancelled: arr_cancelled,
            arr_diverted: arr_diverted,
            arr_delay: total_delay_minutes,
            timestamp: now.toString()
        ]
        record.putAll(causes_map)
        records.add(record)
    }}
    
    // --- 3. Output to FlowFiles ---
    records.each {{ rec ->
        def newFlowFile = session.create(flowFile)
        def jsonStr = JsonOutput.toJson(rec)
        
        newFlowFile = session.write(newFlowFile, {{ outStream ->
            outStream.write(jsonStr.getBytes(StandardCharsets.UTF_8))
        }} as OutputStreamCallback)
        
        newFlowFile = session.putAttribute(newFlowFile, 'mime.type', 'application/json')
        newFlowFile = session.putAttribute(newFlowFile, 'kafka.key', rec.carrier)
        
        session.transfer(newFlowFile, REL_SUCCESS)
    }}
    
    session.remove(flowFile)
    
}} catch (e) {{
    log.error('Error generating data', e)
    session.transfer(flowFile, REL_FAILURE)
}}
'''
    
    with open(OUTPUT_SCRIPT, 'w', encoding='utf-8') as f:
        f.write(script_content)
    print(f"✅ Successfully built NiFi Groovy script based on {len(df)} records.")

if __name__ == "__main__":
    analyze_and_build()
