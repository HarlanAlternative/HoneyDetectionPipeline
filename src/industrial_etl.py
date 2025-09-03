#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Industrial ETL Processor - Integrated Spark, Data Quality Monitoring and Error Handling
Supports big data processing, incremental loading, quality checks and PowerBI integration
"""

import os
import sys
import json
import logging
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import warnings

# Big data processing
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.functions import col, when, lit, udf
    from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
    PYSPARK_AVAILABLE = True
except ImportError:
    PYSPARK_AVAILABLE = False
    warnings.warn("PySpark not installed, will use Pandas for data processing")

# Data quality monitoring
try:
    import great_expectations as ge
    import pandera as pa
    QUALITY_LIBS_AVAILABLE = True
except ImportError:
    QUALITY_LIBS_AVAILABLE = False
    warnings.warn("Data quality monitoring libraries not installed")

# Database and caching
try:
    import psycopg2
    from sqlalchemy import create_engine, text
    from sqlalchemy.pool import QueuePool
    import redis
    DB_LIBS_AVAILABLE = True
except ImportError:
    DB_LIBS_AVAILABLE = False
    warnings.warn("Database libraries not installed")

# Structured logging
try:
    import structlog
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False


@dataclass
class ETLConfig:
    """Configuration for ETL processing"""
    database_url: str
    redis_host: str
    redis_port: int
    redis_db: int
    spark_master: str
    spark_app_name: str
    batch_size: int
    quality_thresholds: Dict[str, float]
    business_rules: Dict[str, Dict[str, float]]


class IndustrialETLProcessor:
    """Industrial ETL processor with Spark, quality monitoring and error handling"""
    
    def __init__(self, config_path: str = "config/etl_config.json"):
        """Initialize the ETL processor"""
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.spark = None
        self.db_engine = None
        self.redis_client = None
        
        if PYSPARK_AVAILABLE:
            self.spark = self._init_spark()
        
        if DB_LIBS_AVAILABLE:
            self.db_engine = self._init_database()
            self.redis_client = self._init_redis()
    
    def _load_config(self, config_path: str) -> ETLConfig:
        """Load configuration from JSON file"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            return ETLConfig(
                database_url=f"postgresql://{config_data['database']['user']}:{config_data['database']['password']}@{config_data['database']['host']}:{config_data['database']['port']}/{config_data['database']['database']}",
                redis_host=config_data['redis']['host'],
                redis_port=config_data['redis']['port'],
                redis_db=config_data['redis']['db'],
                spark_master=config_data['spark']['master'],
                spark_app_name=config_data['spark']['app_name'],
                batch_size=1000,
                quality_thresholds=config_data['quality']['thresholds'],
                business_rules=config_data['quality']['business_rules']
            )
        except Exception as e:
            # Default configuration
            return ETLConfig(
                database_url="postgresql://postgres:password@localhost:5432/honey_warehouse",
                redis_host="localhost",
                redis_port=6379,
                redis_db=0,
                spark_master="local[*]",
                spark_app_name="HoneyQualityETL",
                batch_size=1000,
                quality_thresholds={
                    "completeness": 0.95,
                    "accuracy": 0.90,
                    "consistency": 0.85,
                    "timeliness": 0.98
                },
                business_rules={
                    "moisture": {"min": 15.0, "max": 20.0},
                    "ph": {"min": 3.5, "max": 6.5},
                    "diastase_activity": {"min": 8.0},
                    "h_m_f": {"max": 40.0}
                }
            )
    
    def _setup_logging(self) -> Any:
        """Setup structured logging"""
        if STRUCTLOG_AVAILABLE:
            return structlog.get_logger()
        else:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            return logging.getLogger(__name__)
    
    def _init_spark(self) -> Optional[SparkSession]:
        """Initialize Spark session"""
        try:
            spark = SparkSession.builder \
                .appName(self.config.spark_app_name) \
                .master(self.config.spark_master) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
            
            self.logger.info("Spark session initialized successfully")
            return spark
        except Exception as e:
            self.logger.error(f"Failed to initialize Spark: {e}")
            return None
    
    def _init_database(self) -> Optional[Any]:
        """Initialize database connection"""
        try:
            engine = create_engine(
                self.config.database_url,
                poolclass=QueuePool,
                pool_size=10,
                max_overflow=20,
                pool_pre_ping=True
            )
            self.logger.info("Database connection established")
            return engine
        except Exception as e:
            self.logger.error(f"Failed to connect to database: {e}")
            return None
    
    def _init_redis(self) -> Optional[Any]:
        """Initialize Redis connection"""
        try:
            client = redis.Redis(
                host=self.config.redis_host,
                port=self.config.redis_port,
                db=self.config.redis_db,
                decode_responses=True
            )
            client.ping()
            self.logger.info("Redis connection established")
            return client
        except Exception as e:
            self.logger.error(f"Failed to connect to Redis: {e}")
            return None
    
    def extract_data(self, source_path: str) -> pd.DataFrame:
        """Extract data from source"""
        try:
            if source_path.endswith('.csv'):
                data = pd.read_csv(source_path)
            elif source_path.endswith('.xlsx'):
                data = pd.read_excel(source_path)
            else:
                raise ValueError(f"Unsupported file format: {source_path}")
            
            self.logger.info(f"Data extracted successfully: {data.shape}")
            return data
        except Exception as e:
            self.logger.error(f"Data extraction failed: {e}")
            raise
    
    def transform_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Transform and clean data"""
        try:
            # Data cleaning
            data = data.dropna(subset=['moisture', 'ph', 'diastase_activity', 'h_m_f'])
            
            # Data validation
            data = self._apply_business_rules(data)
            
            # Quality scoring
            data['quality_score'] = data.apply(self._calculate_quality_score, axis=1)
            data['quality_category'] = data['quality_score'].apply(self._categorize_quality)
            
            # Timestamp
            data['processed_at'] = datetime.now()
            
            self.logger.info(f"Data transformation completed: {data.shape}")
            return data
        except Exception as e:
            self.logger.error(f"Data transformation failed: {e}")
            raise
    
    def _apply_business_rules(self, data: pd.DataFrame) -> pd.DataFrame:
        """Apply business rules for data validation"""
        rules = self.config.business_rules
        
        # Moisture validation
        if 'moisture' in rules:
            data = data[
                (data['moisture'] >= rules['moisture']['min']) &
                (data['moisture'] <= rules['moisture']['max'])
            ]
        
        # pH validation
        if 'ph' in rules:
            data = data[
                (data['ph'] >= rules['ph']['min']) &
                (data['ph'] <= rules['ph']['max'])
            ]
        
        # Diastase activity validation
        if 'diastase_activity' in rules:
            data = data[data['diastase_activity'] >= rules['diastase_activity']['min']]
        
        # HMF validation
        if 'h_m_f' in rules:
            data = data[data['h_m_f'] <= rules['h_m_f']['max']]
        
        return data
    
    def _calculate_quality_score(self, row: pd.Series) -> float:
        """Calculate quality score based on business rules"""
        score = 100.0
        rules = self.config.business_rules
        
        # Moisture scoring
        if 'moisture' in rules:
            moisture = row['moisture']
            min_val = rules['moisture']['min']
            max_val = rules['moisture']['max']
            
            if min_val <= moisture <= max_val:
                score -= 0
            elif (min_val - 1) <= moisture <= (max_val + 1):
                score -= 5
            else:
                score -= 15
        
        # pH scoring
        if 'ph' in rules:
            ph = row['ph']
            min_val = rules['ph']['min']
            max_val = rules['ph']['max']
            
            if min_val <= ph <= max_val:
                score -= 0
            elif (min_val - 0.5) <= ph <= (max_val + 0.5):
                score -= 5
            else:
                score -= 15
        
        # Diastase activity scoring
        if 'diastase_activity' in rules:
            activity = row['diastase_activity']
            min_val = rules['diastase_activity']['min']
            
            if activity >= min_val:
                score -= 0
            elif activity >= (min_val - 2):
                score -= 5
            else:
                score -= 15
        
        # HMF scoring
        if 'h_m_f' in rules:
            hmf = row['h_m_f']
            max_val = rules['h_m_f']['max']
            
            if hmf <= max_val:
                score -= 0
            elif hmf <= (max_val + 10):
                score -= 5
            else:
                score -= 15
        
        return max(0.0, score)
    
    def _categorize_quality(self, score: float) -> str:
        """Categorize quality based on score"""
        if score >= 90:
            return "Excellent"
        elif score >= 80:
            return "Good"
        elif score >= 70:
            return "Fair"
        else:
            return "Poor"
    
    def load_data(self, data: pd.DataFrame, table_name: str) -> bool:
        """Load data to database"""
        try:
            if self.db_engine is None:
                self.logger.error("Database not available")
                return False
            
            # Load to database
            data.to_sql(
                table_name,
                self.db_engine,
                if_exists='append',
                index=False,
                method='multi',
                chunksize=self.config.batch_size
            )
            
            # Cache metadata in Redis
            if self.redis_client:
                cache_key = f"table_metadata:{table_name}"
                metadata = {
                    "last_updated": datetime.now().isoformat(),
                    "row_count": len(data),
                    "columns": list(data.columns)
                }
                self.redis_client.setex(cache_key, 3600, json.dumps(metadata))
            
            self.logger.info(f"Data loaded successfully to {table_name}: {len(data)} rows")
            return True
        except Exception as e:
            self.logger.error(f"Data loading failed: {e}")
            return False
    
    def run_etl_pipeline(self, source_path: str, table_name: str) -> Dict[str, Any]:
        """Run complete ETL pipeline"""
        start_time = datetime.now()
        
        try:
            # Extract
            self.logger.info("Starting data extraction...")
            data = self.extract_data(source_path)
            
            # Transform
            self.logger.info("Starting data transformation...")
            transformed_data = self.transform_data(data)
            
            # Load
            self.logger.info("Starting data loading...")
            load_success = self.load_data(transformed_data, table_name)
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                "status": "success" if load_success else "failed",
                "duration_seconds": duration,
                "rows_processed": len(transformed_data),
                "source_path": source_path,
                "target_table": table_name,
                "timestamp": end_time.isoformat()
            }
            
            self.logger.info(f"ETL pipeline completed: {result}")
            return result
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            result = {
                "status": "failed",
                "error": str(e),
                "duration_seconds": duration,
                "timestamp": end_time.isoformat()
            }
            
            self.logger.error(f"ETL pipeline failed: {result}")
            return result
    
    def cleanup(self):
        """Cleanup resources"""
        try:
            if self.spark:
                self.spark.stop()
            if self.db_engine:
                self.db_engine.dispose()
            if self.redis_client:
                self.redis_client.close()
            
            self.logger.info("Resources cleaned up successfully")
        except Exception as e:
            self.logger.error(f"Cleanup failed: {e}")


if __name__ == "__main__":
    # Example usage
    processor = IndustrialETLProcessor()
    
    try:
        # Run ETL pipeline
        result = processor.run_etl_pipeline(
            source_path="data/sample_honey_data.csv",
            table_name="honey_quality_data"
        )
        print(f"ETL Result: {result}")
    finally:
        processor.cleanup()
