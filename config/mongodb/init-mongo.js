// MongoDB Initialization Script
// Creates database, collections, and indexes

db = db.getSiblingDB('airline_cache');

// Create collections
db.createCollection('aggregated_delays');
db.createCollection('carrier_stats');
db.createCollection('airport_stats');
db.createCollection('ml_predictions');

// Create indexes for aggregated_delays
db.aggregated_delays.createIndex({ "year": 1, "month": 1 });
db.aggregated_delays.createIndex({ "carrier": 1 });
db.aggregated_delays.createIndex({ "airport": 1 });
db.aggregated_delays.createIndex({ "timestamp": -1 });

// Create indexes for carrier_stats
db.carrier_stats.createIndex({ "carrier_code": 1 }, { unique: true });
db.carrier_stats.createIndex({ "delay_percentage": -1 });

// Create indexes for airport_stats
db.airport_stats.createIndex({ "airport_code": 1 }, { unique: true });
db.airport_stats.createIndex({ "delay_percentage": -1 });

// Create indexes for ml_predictions
db.ml_predictions.createIndex({ "prediction_date": -1 });
db.ml_predictions.createIndex({ "model_version": 1 });

// Insert sample metadata
db.metadata.insertOne({
    created_at: new Date(),
    version: "1.0",
    description: "Airline delay cache database",
    last_update: new Date()
});

print('MongoDB initialization completed successfully');
