import json
import sys
import traceback
import uuid
import random
import time
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback

class TransformCallback(StreamCallback):
    def __init__(self):
        self.template_record = None
        self.output_record = None
    
    def process(self, inputStream, outputStream):
        try:
            # Read input
            text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
            self.template_record = json.loads(text)
            
            # Extract base values
            base_flights = self.template_record.get('arr_flights', 0)
            base_carrier = self.template_record.get('carrier', '')
            base_carrier_name = self.template_record.get('carrier_name', '')
            base_airport = self.template_record.get('airport', '')
            base_airport_name = self.template_record.get('airport_name', '')
            
            # Apply random variation to flight count (±15-25%)
            variation_factor = 0.85 + (random.random() * 0.30)
            arr_flights = max(1, int(round(base_flights * variation_factor)))
            
            # Determine if there are delays (realistic probability ~20-30%)
            has_delays = random.random() < 0.25
            arr_del15 = 0
            arr_delay = 0
            carrier_delay = 0
            weather_delay = 0
            nas_delay = 0
            security_delay = 0
            late_aircraft_delay = 0
            carrier_ct = 0
            weather_ct = 0
            nas_ct = 0
            security_ct = 0
            late_aircraft_ct = 0
            
            if has_delays and arr_flights > 0:
                # Calculate number of delayed flights
                arr_del15 = max(1, int(round(arr_flights * (0.1 + random.random() * 0.3))))
                
                # Generate total delay minutes
                avg_delay_per_flight = 20 + random.randint(0, 480)
                arr_delay = arr_del15 * avg_delay_per_flight
                
                # RULE 1: Distribute arrDelay across components
                # RULE 5: Weather is rare (5-10%)
                # RULE 6: Security is VERY rare (0-1%)
                
                carrier_prop = 0.30 + random.random() * 0.15    # 30-45%
                weather_prop = random.random() * 0.08           # 0-8% (RARE)
                security_prop = random.random() * 0.012         # 0-1.2% (VERY RARE)
                nas_prop = 0.20 + random.random() * 0.15        # 20-35%
                
                # Normalize
                total_prop = carrier_prop + weather_prop + security_prop + nas_prop
                late_aircraft_prop = max(0, 1.0 - total_prop)
                
                # Recalculate
                total = total_prop + late_aircraft_prop
                carrier_prop = carrier_prop / total
                weather_prop = weather_prop / total
                security_prop = security_prop / total
                nas_prop = nas_prop / total
                late_aircraft_prop = late_aircraft_prop / total
                
                # Apply proportions
                carrier_delay = int(round(arr_delay * carrier_prop))
                weather_delay = int(round(arr_delay * weather_prop))
                security_delay = int(round(arr_delay * security_prop))
                nas_delay = int(round(arr_delay * nas_prop))
                late_aircraft_delay = int(round(arr_delay * late_aircraft_prop))
                
                # RULE 1: Ensure exact sum
                calculated_sum = carrier_delay + weather_delay + security_delay + nas_delay + late_aircraft_delay
                difference = arr_delay - calculated_sum
                carrier_delay += difference
                
                # RULE 3: Calculate count fields
                carrier_ct = max(1, int(round(arr_del15 * carrier_prop * 1.2))) if carrier_delay > 0 else 0
                weather_ct = max(0, int(round(arr_del15 * weather_prop * 1.5))) if weather_delay > 0 else 0
                security_ct = 1 if (security_delay > 0 and random.random() < 0.5) else 0
                nas_ct = max(1, int(round(arr_del15 * nas_prop * 1.2))) if nas_delay > 0 else 0
                late_aircraft_ct = max(1, int(round(arr_del15 * late_aircraft_prop * 1.2))) if late_aircraft_delay > 0 else 0
                
                # RULE 5: Ensure weather_ct stays very low
                weather_ct = min(weather_ct, 3)
                
                # RULE 6: Ensure security_ct is 0 or 1
                security_ct = min(security_ct, 1)
            
            # RULE 8: Month-based seasonality
            month = 1 + random.randint(0, 11)
            
            # Generate unique ID
            unique_id = "{}_{}".format(str(uuid.uuid4()), int(time.time() * 1000))
            
            # Build output record
            self.output_record = {
                'id': unique_id,
                'message_id': str(uuid.uuid4()),
                'kafka_key': "{}_{}_{}".format(base_airport, base_carrier, int(time.time() * 1000)),
                'processing_timestamp': time.strftime('%Y-%m-%d %H:%M:%S.000'),
                'sequence_number': int(time.time() * 1000),
                
                'year': 2025,
                'month': month,
                'carrier': base_carrier,
                'carrier_name': base_carrier_name,
                'airport': base_airport,
                'airport_name': base_airport_name,
                
                'arr_flights': arr_flights,
                'arr_del15': arr_del15,
                'arr_delay': arr_delay,
                
                'carrier_ct': carrier_ct,
                'weather_ct': weather_ct,
                'nas_ct': nas_ct,
                'security_ct': security_ct,
                'late_aircraft_ct': late_aircraft_ct,
                
                'carrier_delay': carrier_delay,
                'weather_delay': weather_delay,
                'nas_delay': nas_delay,
                'security_delay': security_delay,
                'late_aircraft_delay': late_aircraft_delay,
                
                'arr_cancelled': 0,
                'arr_diverted': 0
            }
            
            # Write output
            output_json = json.dumps(self.output_record)
            outputStream.write(bytearray(output_json.encode('utf-8')))
            
        except Exception as e:
            traceback.print_exc(file=sys.stderr)
            raise e

# Main execution
flowFile = session.get()
if flowFile is not None:
    try:
        callback = TransformCallback()
        flowFile = session.write(flowFile, callback)
        session.transfer(flowFile, REL_SUCCESS)
    except Exception as e:
        log.error('Error processing FlowFile: {}'.format(str(e)))
        session.transfer(flowFile, REL_FAILURE)
