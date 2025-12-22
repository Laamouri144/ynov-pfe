"""
Script 3: Machine Learning Training Pipeline
Trains models using Spark MLlib, Dask-ML, and Scikit-learn
Compares performance across frameworks
Run on PC2
"""

import os
import time
import json
from datetime import datetime
from typing import Dict, Tuple
import numpy as np
import pandas as pd
from pymongo import MongoClient
from loguru import logger
from dotenv import load_dotenv
import psutil

# Load environment variables
load_dotenv()

# Configuration
MONGODB_HOST = os.getenv('MONGODB_HOST', 'localhost')
MONGODB_PORT = int(os.getenv('MONGODB_PORT', '27017'))
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE', 'airline_cache')
MODEL_PATH = os.getenv('MODEL_PATH', './models')

# Setup logging
logger.add("logs/ml_training.log", rotation="100 MB", retention="10 days")


class MLPipelineComparison:
    """Compare ML frameworks for airline delay prediction"""

    def __init__(self):
        self.mongo_client = None
        self.data = None
        self.X_train = None
        self.X_test = None
        self.y_train = None
        self.y_test = None
        self.results = {}
        
        # Create model directory
        os.makedirs(MODEL_PATH, exist_ok=True)
        
        self.initialize_mongodb()

    def initialize_mongodb(self):
        """Initialize MongoDB connection"""
        self.mongo_client = MongoClient(f'mongodb://{MONGODB_HOST}:{MONGODB_PORT}/')
        logger.info(f"MongoDB client initialized: {MONGODB_HOST}:{MONGODB_PORT}")

    def load_data_from_mongodb(self):
        """Load aggregated data from MongoDB"""
        logger.info("Loading data from MongoDB...")
        
        db = self.mongo_client[MONGODB_DATABASE]
        collection = db['aggregated_delays']
        
        # Fetch all documents
        cursor = collection.find({})
        data = list(cursor)
        
        if not data:
            raise ValueError("No data found in MongoDB!")
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        logger.info(f"Loaded {len(df)} records from MongoDB")
        
        return df

    def prepare_features(self, df: pd.DataFrame) -> pd.DataFrame:
        """Feature engineering"""
        logger.info("Preparing features...")
        
        # Select relevant columns
        features = df[[
            'year', 'month', 'total_flights', 'total_delayed',
            'carrier_delay_minutes', 'weather_delay_minutes',
            'nas_delay_minutes', 'security_delay_minutes',
            'late_aircraft_delay_minutes', 'total_cancelled', 'total_diverted'
        ]].copy()
        
        # Create target variable (delay percentage)
        features['delay_percentage'] = df['delay_percentage']
        
        # Handle missing values
        features = features.fillna(0)
        
        logger.info(f"Features prepared: {features.shape}")
        return features

    def split_data(self, df: pd.DataFrame, test_size: float = 0.2):
        """Split data into train and test sets"""
        from sklearn.model_selection import train_test_split
        
        X = df.drop('delay_percentage', axis=1)
        y = df['delay_percentage']
        
        self.X_train, self.X_test, self.y_train, self.y_test = train_test_split(
            X, y, test_size=test_size, random_state=42
        )
        
        logger.info(f"Data split: train={len(self.X_train)}, test={len(self.X_test)}")

    def train_sklearn(self) -> Dict:
        """Train model using Scikit-learn + Pandas"""
        logger.info("=" * 60)
        logger.info("Training with Scikit-learn + Pandas")
        logger.info("=" * 60)
        
        from sklearn.ensemble import RandomForestRegressor
        from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error
        import joblib
        
        # Track metrics
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024  # MB
        
        # Train model
        model = RandomForestRegressor(n_estimators=100, random_state=42, n_jobs=-1)
        model.fit(self.X_train, self.y_train)
        
        # Predictions
        y_pred = model.predict(self.X_test)
        
        # Metrics
        training_time = time.time() - start_time
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024
        memory_used = end_memory - start_memory
        
        mse = mean_squared_error(self.y_test, y_pred)
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(self.y_test, y_pred)
        r2 = r2_score(self.y_test, y_pred)
        
        # Save model
        model_file = os.path.join(MODEL_PATH, 'sklearn_model.joblib')
        joblib.dump(model, model_file)
        
        results = {
            'framework': 'Scikit-learn',
            'training_time': training_time,
            'memory_used_mb': memory_used,
            'rmse': rmse,
            'mae': mae,
            'r2_score': r2,
            'model_file': model_file,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Training Time: {training_time:.2f}s")
        logger.info(f"Memory Used: {memory_used:.2f} MB")
        logger.info(f"RMSE: {rmse:.4f}")
        logger.info(f"MAE: {mae:.4f}")
        logger.info(f"R² Score: {r2:.4f}")
        
        return results

    def train_spark(self) -> Dict:
        """Train model using Spark MLlib"""
        logger.info("=" * 60)
        logger.info("Training with Spark MLlib")
        logger.info("=" * 60)
        
        from pyspark.sql import SparkSession
        from pyspark.ml.feature import VectorAssembler
        from pyspark.ml.regression import RandomForestRegressor as SparkRFRegressor
        from pyspark.ml.evaluation import RegressionEvaluator
        
        # Track metrics
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        # Create Spark session
        spark = SparkSession.builder \
            .appName("AirlineDelayPrediction") \
            .config("spark.driver.memory", "4g") \
            .getOrCreate()
        
        # Convert pandas to Spark DataFrame
        train_df = spark.createDataFrame(
            pd.concat([self.X_train, self.y_train], axis=1)
        )
        test_df = spark.createDataFrame(
            pd.concat([self.X_test, self.y_test], axis=1)
        )
        
        # Feature assembly
        feature_columns = self.X_train.columns.tolist()
        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
        train_df = assembler.transform(train_df)
        test_df = assembler.transform(test_df)
        
        # Train model
        rf = SparkRFRegressor(
            featuresCol="features",
            labelCol="delay_percentage",
            numTrees=100,
            seed=42
        )
        model = rf.fit(train_df)
        
        # Predictions
        predictions = model.transform(test_df)
        
        # Evaluate
        evaluator = RegressionEvaluator(
            labelCol="delay_percentage",
            predictionCol="prediction",
            metricName="rmse"
        )
        rmse = evaluator.evaluate(predictions)
        
        evaluator.setMetricName("mae")
        mae = evaluator.evaluate(predictions)
        
        evaluator.setMetricName("r2")
        r2 = evaluator.evaluate(predictions)
        
        # Metrics
        training_time = time.time() - start_time
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024
        memory_used = end_memory - start_memory
        
        # Save model
        model_file = os.path.join(MODEL_PATH, 'spark_model')
        model.write().overwrite().save(model_file)
        
        spark.stop()
        
        results = {
            'framework': 'Spark MLlib',
            'training_time': training_time,
            'memory_used_mb': memory_used,
            'rmse': rmse,
            'mae': mae,
            'r2_score': r2,
            'model_file': model_file,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Training Time: {training_time:.2f}s")
        logger.info(f"Memory Used: {memory_used:.2f} MB")
        logger.info(f"RMSE: {rmse:.4f}")
        logger.info(f"MAE: {mae:.4f}")
        logger.info(f"R² Score: {r2:.4f}")
        
        return results

    def train_dask(self) -> Dict:
        """Train model using Dask-ML"""
        logger.info("=" * 60)
        logger.info("Training with Dask-ML")
        logger.info("=" * 60)
        
        import dask.dataframe as dd
        from dask_ml.model_selection import train_test_split as dask_train_test_split
        from dask_ml.ensemble import RandomForestRegressor as DaskRFRegressor
        from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
        import joblib
        
        # Track metrics
        start_time = time.time()
        start_memory = psutil.Process().memory_info().rss / 1024 / 1024
        
        # Convert to Dask DataFrame
        X_train_dask = dd.from_pandas(self.X_train, npartitions=4)
        y_train_dask = dd.from_pandas(self.y_train, npartitions=4)
        
        # Train model
        model = DaskRFRegressor(n_estimators=100, random_state=42)
        model.fit(X_train_dask, y_train_dask)
        
        # Predictions
        y_pred = model.predict(self.X_test)
        
        # Metrics
        training_time = time.time() - start_time
        end_memory = psutil.Process().memory_info().rss / 1024 / 1024
        memory_used = end_memory - start_memory
        
        mse = mean_squared_error(self.y_test, y_pred)
        rmse = np.sqrt(mse)
        mae = mean_absolute_error(self.y_test, y_pred)
        r2 = r2_score(self.y_test, y_pred)
        
        # Save model
        model_file = os.path.join(MODEL_PATH, 'dask_model.joblib')
        joblib.dump(model, model_file)
        
        results = {
            'framework': 'Dask-ML',
            'training_time': training_time,
            'memory_used_mb': memory_used,
            'rmse': rmse,
            'mae': mae,
            'r2_score': r2,
            'model_file': model_file,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"Training Time: {training_time:.2f}s")
        logger.info(f"Memory Used: {memory_used:.2f} MB")
        logger.info(f"RMSE: {rmse:.4f}")
        logger.info(f"MAE: {mae:.4f}")
        logger.info(f"R² Score: {r2:.4f}")
        
        return results

    def compare_results(self):
        """Compare and save results"""
        logger.info("=" * 60)
        logger.info("COMPARISON RESULTS")
        logger.info("=" * 60)
        
        # Create comparison DataFrame
        comparison_df = pd.DataFrame([
            self.results['sklearn'],
            self.results['spark'],
            self.results['dask']
        ])
        
        logger.info("\n" + comparison_df.to_string(index=False))
        
        # Save to file
        comparison_file = os.path.join(MODEL_PATH, 'framework_comparison.json')
        with open(comparison_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        logger.info(f"\nComparison saved to: {comparison_file}")
        
        # Determine best framework
        best_framework = min(self.results.items(), key=lambda x: x[1]['rmse'])
        logger.info(f"\nBest Framework (lowest RMSE): {best_framework[0]}")
        
        return comparison_df

    def run(self):
        """Execute complete ML pipeline"""
        logger.info("Starting ML Training Pipeline...")
        
        # Load and prepare data
        raw_data = self.load_data_from_mongodb()
        prepared_data = self.prepare_features(raw_data)
        self.split_data(prepared_data)
        
        # Train with all frameworks
        self.results['sklearn'] = self.train_sklearn()
        self.results['spark'] = self.train_spark()
        self.results['dask'] = self.train_dask()
        
        # Compare results
        self.compare_results()
        
        logger.info("ML Training Pipeline completed!")


if __name__ == "__main__":
    pipeline = MLPipelineComparison()
    pipeline.run()
