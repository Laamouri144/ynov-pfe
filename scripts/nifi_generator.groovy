import org.apache.commons.io.IOUtils
import java.nio.charset.StandardCharsets
import groovy.json.JsonOutput
import groovy.json.JsonSlurper
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import org.apache.nifi.processor.io.OutputStreamCallback

/*
 * GENERATED NIFI SCRIPT
 * Purpose: Infinite stream generation based on statistical models
 * DEBUG MODE: Writes to /data/nifi_debug.log
 * MODEL SOURCE: Reads from /data/nifi_models.json
 */

def logFile = new File("/data/nifi_debug.log")
def logMsg = { msg -> 
    try { logFile.append(LocalDateTime.now().toString() + " - " + msg + "\n") } catch (e) { }
}

logMsg("Script triggered")

def flowFile = session.get()
if (!flowFile) {
    return
}

logMsg("FlowFile received: " + flowFile.getId())

// Define Relationships
def REL_SUCCESS = relationships.find { it.name == 'success' }
def REL_FAILURE = relationships.find { it.name == 'failure' }

if (!REL_SUCCESS) logMsg("WARNING: REL_SUCCESS is null! Available: " + relationships.collect { it.name })
if (!REL_FAILURE) logMsg("WARNING: REL_FAILURE is null!")

try {
    logMsg("Loading external JSON models from /data/nifi_models.json ...")
    // --- 1. Load Statistical Models (External JSON) ---
    def jsonSlurper = new JsonSlurper()
    def modelFile = new File("/data/nifi_models.json")
    
    if (!modelFile.exists()) {
        logMsg("ERROR: Model file not found at /data/nifi_models.json")
        throw new RuntimeException("Model file missing")
    }

    def models = jsonSlurper.parse(modelFile)
    logMsg("Models loaded successfully.")
    
    def VALID_ROUTES = models.VALID_ROUTES
    def SEASONALITY = models.SEASONALITY
    def AIRPORT_PROFILES = models.AIRPORT_PROFILES
    def CARRIER_ALPHAS = models.CARRIER_ALPHAS
    
    logMsg("Routes: " + VALID_ROUTES.size() + ", Airports: " + AIRPORT_PROFILES.size())
    
    // --- 2. Generation Logic ---
    def random = new Random()
    def records = []
    
    // Generate 1 to 5 records per execution trigger
    def numRecords = random.nextInt(5) + 1
    
    for (int i = 0; i < numRecords; i++) {
        // A. Context
        def ctx = VALID_ROUTES[random.nextInt(VALID_ROUTES.size())]
        def airport = ctx.airport
        def carrier = ctx.carrier
        
        // B. Profiles
        def apoProf = AIRPORT_PROFILES[airport]
        // Fallback if missing
        if (!apoProf) {
             apoProf = [volume: [mean: 50, std: 10], causes: [carrier_delay: 0.2, weather_delay: 0.2, nas_delay: 0.2, security_delay: 0.2, late_aircraft_delay: 0.2], base_delay_rate: 0.2]
        }
        
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
        if (random.nextDouble() < (0.02 * monthMult)) {
            arr_cancelled = (arr_flights * (random.nextDouble() * 0.09 + 0.01)).toInteger()
        }
        
        def arr_diverted = 0
        if (random.nextDouble() < 0.005) {
            arr_diverted = (arr_flights * (random.nextDouble() * 0.04 + 0.01)).toInteger()
        }
        
        // F. Delays
        def effective_flights = arr_flights - arr_cancelled - arr_diverted
        def prob_delay = apoProf.base_delay_rate * carrAlpha * monthMult
        prob_delay = Math.max(0.01, Math.min(0.95, prob_delay))
        
        def arr_del15 = 0
        if (effective_flights > 0) {
             def instance_prob = Math.max(0.0, Math.min(1.0, prob_delay + (random.nextDouble() * 0.1 - 0.05)))
             arr_del15 = (effective_flights * instance_prob).toInteger()
        }
        
        // G. Delay Minutes & Causes
        def total_delay_minutes = 0
        def causes_map = [
            carrier_delay: 0, weather_delay: 0, nas_delay: 0, security_delay: 0, late_aircraft_delay: 0,
            carrier_ct: 0.0, weather_ct: 0.0, nas_ct: 0.0, security_ct: 0.0, late_aircraft_ct: 0.0
        ]
        
        if (arr_del15 > 0) {
            def avg_delay = 30 + (random.nextDouble() * 45) // 30-75 mins
            total_delay_minutes = (arr_del15 * avg_delay).toInteger()
            
            def remaining = total_delay_minutes
            def causeKeys = new ArrayList(apoProf.causes.keySet())
            
            for (int k = 0; k < causeKeys.size(); k++) {
                def causeKey = causeKeys[k]
                def baseShare = apoProf.causes[causeKey]
                
                def minutes = 0
                if (k == causeKeys.size() - 1) {
                    minutes = remaining
                } else {
                    def share = baseShare * (0.8 + (random.nextDouble() * 0.4)) // 0.8 to 1.2
                    minutes = (total_delay_minutes * share).toInteger()
                    minutes = Math.min(minutes, remaining)
                }
                
                causes_map[causeKey] = minutes
                remaining -= minutes
                
                // Count calc
                if (minutes > 0) {
                     def ctKey = causeKey.replace('_delay', '_ct')
                     def divisor = 40 + (random.nextDouble() * 20)
                     def ctVal = minutes / divisor
                     causes_map[ctKey] = Math.round(ctVal * 100.0) / 100.0
                }
            }
        }

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
    }
    
    // --- 3. Output to FlowFiles ---
    records.each { rec ->
        def newFlowFile = session.create(flowFile)
        def jsonStr = JsonOutput.toJson(rec)
        
        newFlowFile = session.write(newFlowFile, { outStream ->
            outStream.write(jsonStr.getBytes(StandardCharsets.UTF_8))
        } as OutputStreamCallback)
        
        newFlowFile = session.putAttribute(newFlowFile, 'mime.type', 'application/json')
        newFlowFile = session.putAttribute(newFlowFile, 'kafka.key', rec.carrier)
        
        session.transfer(newFlowFile, REL_SUCCESS)
    }
    
    session.remove(flowFile)
    
} catch (e) {
    def sw = new StringWriter()
    def pw = new PrintWriter(sw)
    e.printStackTrace(pw)
    logMsg("ERROR: " + sw.toString())
    
    // Attempt fallback transfer
    if (REL_FAILURE) {
        session.transfer(flowFile, REL_FAILURE)
    } else {
        session.remove(flowFile)
    }
}
