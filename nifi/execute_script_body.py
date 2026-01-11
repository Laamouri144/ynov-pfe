import json
import sys
import traceback
import uuid
import random
import time
from org.apache.commons.io import IOUtils
from java.nio.charset import StandardCharsets
from org.apache.nifi.processor.io import StreamCallback


def clamp(v, lo, hi):
    try:
        if v < lo:
            return lo
        if v > hi:
            return hi
        return v
    except:
        return lo


class TransformCallback(StreamCallback):
    def __init__(self):
        self.template_record = None
        self.output_record = None

    def process(self, inputStream, outputStream):
        try:
            text = IOUtils.toString(inputStream, StandardCharsets.UTF_8)
            self.template_record = json.loads(text)

            # Calibration factor to better match the real overall delay_rate.
            # Based on benchmark vs. stream comparisons (tunable).
            DELAY_RATE_CALIBRATION = 1.085

            # Calibration factor to better match the real overall divert_rate.
            # Based on benchmark vs. stream comparisons (tunable).
            DIVERT_RATE_CALIBRATION = 1.19

            base_flights = float(self.template_record.get('arr_flights', 0) or 0)
            base_del15 = float(self.template_record.get('arr_del15', 0) or 0)
            base_delay = float(self.template_record.get('arr_delay', 0) or 0)

            base_carrier = self.template_record.get('carrier', '')
            base_carrier_name = self.template_record.get('carrier_name', '')
            base_airport = self.template_record.get('airport', '')
            base_airport_name = self.template_record.get('airport_name', '')

            # KEEP template month to preserve real seasonality.
            month = int(self.template_record.get('month', 1) or 1)
            if month < 1:
                month = 1
            if month > 12:
                month = 12

            # Flight volume: small multiplicative noise around template.
            variation_factor = 0.90 + (random.random() * 0.20)  # 0.90..1.10
            arr_flights = max(1, int(round(base_flights * variation_factor)))

            # Delay-rate: use template's delay rate (+ small noise), rather than a fixed 25%.
            if base_flights > 0:
                base_delay_rate = base_del15 / base_flights
            else:
                base_delay_rate = 0.0

            # add mild jitter so stream isn't constant
            p = (base_delay_rate * DELAY_RATE_CALIBRATION) * (0.90 + 0.20 * random.random())
            p = clamp(p, 0.0, 0.95)
            arr_del15 = int(round(arr_flights * p))
            arr_del15 = max(0, min(arr_flights, arr_del15))

            # Delay minutes: preserve template's avg delay per delayed flight (+ noise)
            if base_del15 > 0:
                base_avg_per_delayed = base_delay / base_del15
            else:
                base_avg_per_delayed = 0.0

            if base_avg_per_delayed <= 0 and arr_del15 > 0:
                base_avg_per_delayed = 55.0

            arr_delay = 0
            carrier_delay = 0
            weather_delay = 0
            nas_delay = 0
            security_delay = 0
            late_aircraft_delay = 0

            carrier_ct = 0.0
            weather_ct = 0.0
            nas_ct = 0.0
            security_ct = 0.0
            late_aircraft_ct = 0.0

            if arr_del15 > 0:
                avg_per_delayed = base_avg_per_delayed * (0.85 + 0.30 * random.random())
                arr_delay = int(round(arr_del15 * avg_per_delayed))
                arr_delay = max(0, arr_delay)

                # Cause shares (minutes): start from template, add small jitter, renormalize.
                base_carrier_delay = float(self.template_record.get('carrier_delay', 0) or 0)
                base_weather_delay = float(self.template_record.get('weather_delay', 0) or 0)
                base_nas_delay = float(self.template_record.get('nas_delay', 0) or 0)
                base_security_delay = float(self.template_record.get('security_delay', 0) or 0)
                base_late_delay = float(self.template_record.get('late_aircraft_delay', 0) or 0)

                base_sum = base_carrier_delay + base_weather_delay + base_nas_delay + base_security_delay + base_late_delay
                if base_sum <= 0:
                    # fallback typical distribution
                    shares = [0.30, 0.06, 0.26, 0.002, 0.378]
                else:
                    shares = [
                        base_carrier_delay / base_sum,
                        base_weather_delay / base_sum,
                        base_nas_delay / base_sum,
                        base_security_delay / base_sum,
                        base_late_delay / base_sum,
                    ]

                # jitter
                jittered = []
                for s in shares:
                    jittered.append(max(0.0, s * (0.90 + 0.20 * random.random())))

                total = sum(jittered)
                if total <= 0:
                    jittered = [0.30, 0.06, 0.26, 0.002, 0.378]
                    total = sum(jittered)

                jittered = [x / total for x in jittered]

                carrier_delay = int(round(arr_delay * jittered[0]))
                weather_delay = int(round(arr_delay * jittered[1]))
                nas_delay = int(round(arr_delay * jittered[2]))
                security_delay = int(round(arr_delay * jittered[3]))
                late_aircraft_delay = int(round(arr_delay * jittered[4]))

                # exact sum fix (keep components non-negative)
                target_sum = int(arr_delay)
                parts = [
                    int(carrier_delay),
                    int(weather_delay),
                    int(nas_delay),
                    int(security_delay),
                    int(late_aircraft_delay),
                ]

                # If rounding overshot arr_delay, remove minutes from components (never below 0).
                overshoot = sum(parts) - target_sum
                if overshoot > 0:
                    for i in range(len(parts)):
                        if overshoot <= 0:
                            break
                        take = min(parts[i], overshoot)
                        parts[i] -= take
                        overshoot -= take

                # If rounding undershot arr_delay, add missing minutes to carrier delay.
                undershoot = target_sum - sum(parts)
                if undershoot > 0:
                    parts[0] += undershoot

                carrier_delay, weather_delay, nas_delay, security_delay, late_aircraft_delay = parts

                # Cause counts: scale from template counts per delayed flight.
                def per_delayed(name):
                    try:
                        v = float(self.template_record.get(name, 0) or 0)
                        if base_del15 > 0:
                            return v / base_del15
                        return 0.0
                    except:
                        return 0.0

                carrier_ct = clamp(arr_del15 * per_delayed('carrier_ct') * (0.85 + 0.30 * random.random()), 0.0, float(arr_del15))
                weather_ct = clamp(arr_del15 * per_delayed('weather_ct') * (0.85 + 0.30 * random.random()), 0.0, float(arr_del15))
                nas_ct = clamp(arr_del15 * per_delayed('nas_ct') * (0.85 + 0.30 * random.random()), 0.0, float(arr_del15))
                security_ct = clamp(arr_del15 * per_delayed('security_ct') * (0.85 + 0.30 * random.random()), 0.0, float(arr_del15))
                late_aircraft_ct = clamp(arr_del15 * per_delayed('late_aircraft_ct') * (0.85 + 0.30 * random.random()), 0.0, float(arr_del15))

            # Cancel/divert: preserve template rates (+ small noise)
            base_cancel = float(self.template_record.get('arr_cancelled', 0) or 0)
            base_divert = float(self.template_record.get('arr_diverted', 0) or 0)
            cancel_rate = (base_cancel / base_flights) if base_flights > 0 else 0.0
            divert_rate = (base_divert / base_flights) if base_flights > 0 else 0.0
            cancel_rate = clamp(cancel_rate * (0.90 + 0.20 * random.random()), 0.0, 0.20)
            divert_rate = clamp(divert_rate * DIVERT_RATE_CALIBRATION * (0.90 + 0.20 * random.random()), 0.0, 0.05)
            arr_cancelled = int(round(arr_flights * cancel_rate))
            arr_diverted = int(round(arr_flights * divert_rate))

            unique_id = "{}_{}".format(str(uuid.uuid4()), int(time.time() * 1000))

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

                'carrier_ct': carrier_ct,
                'weather_ct': weather_ct,
                'nas_ct': nas_ct,
                'security_ct': security_ct,
                'late_aircraft_ct': late_aircraft_ct,

                'arr_cancelled': arr_cancelled,
                'arr_diverted': arr_diverted,

                'arr_delay': arr_delay,
                'carrier_delay': carrier_delay,
                'weather_delay': weather_delay,
                'nas_delay': nas_delay,
                'security_delay': security_delay,
                'late_aircraft_delay': late_aircraft_delay,
            }

            output_json = json.dumps(self.output_record)
            outputStream.write(bytearray(output_json.encode('utf-8')))

        except Exception as e:
            traceback.print_exc(file=sys.stderr)
            raise e


flowFile = session.get()
if flowFile is not None:
    try:
        callback = TransformCallback()
        flowFile = session.write(flowFile, callback)
        session.transfer(flowFile, REL_SUCCESS)
    except Exception as e:
        log.error('Error processing FlowFile: {}'.format(str(e)))
        session.transfer(flowFile, REL_FAILURE)
