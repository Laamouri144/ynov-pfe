🎯 COMPLETE NO-CODE GUIDE: Generate 100,000+ Rows in Nifi
Here are the exact steps for your Nifi team to generate massive, accurate streaming data WITHOUT scripting:

📊 STEP 1: PREPARE THE TEMPLATE DATA
Your CSV should have:
1000+ unique template rows (you have 751, need ~250 more)
Each row unique combination (airport+carrier+month)
Add more variation in delay patterns
Quick Python to expand your template: (I have already executed this for you. The file is at data/Nifi_Templates_1500.csv)

🔧 STEP 2: NIFI FLOW DESIGN
Complete Processor Chain:
[1. GetFile] → [2. SplitText] → [3. UpdateRecord] → [4. DuplicateFlowFile] 
     ↓
[5. UpdateRecord] → [6. ValidateRecord] → [7. RouteOnAttribute]
                               ↓
                        [8. ConvertRecord] → [9. PublishKafka]
[!NOTE] I have updated Step 4 to DuplicateFlowFile because GenerateFlowFile creates empty files and breaks the template chain. DuplicateFlowFile correctly multiplies your templates.

⚙️ STEP 3: EXACT PROCESSOR CONFIGURATIONS
1. GetFile Processor:
Input Directory: c:\Users\laamo\ynov-pfe\data
File Filter: Nifi_Templates_1500.csv
Keep Source File: true
Polling Interval: 0 sec (load once)
2. SplitText Processor:
Line Split Count: 1 (one row per FlowFile)
Header Line Count: 1 (skip header)
Header Fragment: true
3. First UpdateRecord (Template preparation):
{
  "base_template_id": "${template_id}",
  "variation_id": "${UUID():substring(0,8)}",
  "unique_row_id": "${template_id}_${now():format('yyyyMMddHHmmssSSS')}",
  
  // Add some initial randomness
  "arr_flights_base": "${arr_flights:toNumber()}",
  "arr_del15_base": "${arr_del15:toNumber()}",
  "arr_delay_base": "${arr_delay:toNumber()}"
}
4. DuplicateFlowFile (Mass generation):
Number of Copies: 1000  ← KEY: Generate 1000 FlowFiles per template!
(Replaces GenerateFlowFile to preserve template data)

5. UpdateRecord (Apply variations - MOST IMPORTANT):
{
  // Unique identifiers
  "message_id": "${UUID()}",
  "kafka_key": "${airport}_${carrier}_${now():format('yyyyMMddHHmmssSSS')}",
  "processing_timestamp": "${now():format('yyyy-MM-dd HH:mm:ss.SSS')}",
  
  // Change year to 2025
  "year": 2025,
  
  // Randomize month (1-12)
  "month": "${random():mod(12):plus(1)}",
  
  // Add realistic variations (±15-25%)
  "arr_flights": "${arr_flights_base:toNumber():multiply(${random():mod(30):plus(85)}):divide(100)}",
  "arr_del15": "${arr_del15_base:toNumber():multiply(${random():mod(40):plus(80)}):divide(100)}",
  
  // Ensure arr_del15 ≤ arr_flights
  "arr_del15": "${arr_del15:toNumber():min(${arr_flights:toNumber()})}",
  
  // Update total delay with variation
  "arr_delay": "${arr_delay_base:toNumber():multiply(${random():mod(30):plus(85)}):divide(100)}",
  
  // Update individual delays proportionally
  "carrier_delay": "${carrier_delay:toNumber():multiply(${random():mod(30):plus(85)}):divide(100)}",
  "weather_delay": "${weather_delay:toNumber():multiply(${random():mod(50):plus(75)}):divide(100)}",
  "nas_delay": "${nas_delay:toNumber():multiply(${random():mod(30):plus(85)}):divide(100)}",
  "security_delay": "${security_delay:toNumber():multiply(${random():mod(70):plus(65)}):divide(100)}",
  "late_aircraft_delay": "${late_aircraft_delay:toNumber():multiply(${random():mod(30):plus(85)}):divide(100)}",
  
  // Update delay counts (keep rare events rare)
  "weather_ct": "${weather_ct:toNumber():mod(3)}",  // 0-2 only
  "security_ct": "${security_ct:toNumber():mod(2)}", // 0-1 only
  
  // Sequence tracking
  "generation_batch": "${now():format('yyyyMMdd')}",
  "sequence_number": "${now():toNumber()}",
  
  // Ensure mathematical consistency (Optional check field)
  "delay_sum_check": "${carrier_delay:plus(${weather_delay}):plus(${nas_delay}):plus(${security_delay}):plus(${late_aircraft_delay})}"
}
6. ValidateRecord (Quality check):
{
  "validators": [
    {
      "name": "flightConsistency",
      "expression": "${arr_flights:gt(0):and(${arr_del15:le(${arr_flights})})}"
    },
    {
      "name": "delayRealism", 
      "expression": "${arr_delay:ge(0):and(${arr_delay:lt(1000000)})}"
    },
    {
      "name": "monthValid",
      "expression": "${month:ge(1):and(${month:le(12)})}"
    },
    {
      "name": "weatherRare",
      "expression": "${weather_ct:ge(0):and(${weather_ct:le(3)})}"
    },
    {
      "name": "securityVeryRare",
      "expression": "${security_ct:ge(0):and(${security_ct:le(1)})}"
    }
  ]
}
7. RouteOnAttribute:
Success Route: ${validation.status} == 'valid'
Failure Route: ${validation.status} != 'valid' → Connect to LogAttribute
8. ConvertRecord (CSV → JSON):
Reader: CSVReader (use schema from template)
Writer: JsonRecordSetWriter
9. PublishKafka:
Kafka Brokers: localhost:9092
Topic: flight_data_stream
Delivery Guarantee: Best Effort
Compression Type: snappy
Batch Size: 500
⚡ STEP 4: SCALING FOR 100,000+ ROWS
Tuning for High Volume:
1. Parallel Processing:

DuplicateFlowFile: Concurrent Tasks = 10
UpdateRecord: Concurrent Tasks = 8  
PublishKafka: Concurrent Tasks = 5
2. Batch Sizes:

PublishKafka: Batch Size = 500
3. Performance Settings:

Backpressure: 
  • Max Queue Size = 10000
  • Swap Threshold = 20000
  
Timeouts:
  • Yield Duration = 1 sec
  • Run Schedule = 0 sec
4. Memory Allocation:

JVM Heap Size: 4GB minimum
Nifi Buffer Size: 1MB
🎯 STEP 5: GENERATION STRATEGY
Option A: Burst Generation (Recommended)
Run for: 5 minutes
Records per second: ~350
Total in 5 min: 100,000+ records
Then: Reduce to 10 records/sec for continuous stream
