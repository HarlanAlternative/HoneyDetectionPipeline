#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Industrial Honey Quality Analysis Data Pipeline - Main Entry Point
Integrated solution with ETL, monitoring, PowerBI and Airflow
"""

import os
import sys
import json
import argparse
from datetime import datetime
from typing import Dict, Any, Optional

# Add src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'src'))

from industrial_etl import IndustrialETLProcessor
from powerbi_integration import PowerBIIntegration
from monitoring_system import MonitoringSystem
from unified_powerbi_generator import UnifiedPowerBIGenerator


class IndustrialHoneyPipeline:
    """Industrial Honey Quality Analysis Data Pipeline"""
    
    def __init__(self, config_path: str = "config/etl_config.json"):
        """Initialize the pipeline"""
        self.config_path = config_path
        self.etl_processor = None
        self.powerbi_integration = None
        self.monitoring_system = None
        self.powerbi_generator = None
        self.pipeline_results = {}
        
        self._initialize_components()
    
    def _initialize_components(self):
        """Initialize all components"""
        try:
            # Initialize ETL processor
            self.etl_processor = IndustrialETLProcessor(self.config_path)
            print("ETL processor initialized successfully")
            
            # Initialize PowerBI integration
            self.powerbi_integration = PowerBIIntegration(self.config_path)
            print("PowerBI integration initialized successfully")
            
            # Initialize monitoring system
            self.monitoring_system = MonitoringSystem(self.config_path)
            print("Monitoring system initialized successfully")
            
            # Initialize PowerBI generator
            self.powerbi_generator = UnifiedPowerBIGenerator()
            print("PowerBI generator initialized successfully")
            
        except Exception as e:
            print(f"Component initialization failed: {e}")
            raise
    
    def run_demo(self):
        """Run quick demo of the pipeline"""
        print("Running quick demo...")
        
        try:
            # Generate sample data
            sample_data = self._generate_sample_data()
            sample_path = "data/sample_honey_data.csv"
            
            # Ensure data directory exists
            os.makedirs(os.path.dirname(sample_path), exist_ok=True)
            sample_data.to_csv(sample_path, index=False)
            print(f"Sample data generated: {sample_path}")
            
            # Run industrial pipeline
            print("Starting industrial data pipeline...")
            
            # Step 1: Start monitoring
            print("Step 1: Starting monitoring system")
            if self.monitoring_system:
                self.monitoring_system.start_monitoring()
            
            # Step 2: Run ETL pipeline
            print("Step 2: Running ETL pipeline")
            if self.etl_processor:
                etl_result = self.etl_processor.run_etl_pipeline(
                    source_path=sample_path,
                    table_name="honey_quality_data"
                )
                self.pipeline_results["etl"] = etl_result
                
                if etl_result["status"] == "success":
                    print("ETL pipeline completed successfully")
                else:
                    print(f"ETL pipeline failed: {etl_result.get('error', 'Unknown error')}")
            
            # Step 3: Generate PowerBI templates
            print("Step 3: Generating PowerBI templates")
            if self.powerbi_integration:
                try:
                    self.powerbi_integration.generate_powerbi_template()
                    print("PowerBI templates generated successfully")
                except Exception as e:
                    print(f"PowerBI template generation failed: {e}")
            
            # Step 4: Generate comprehensive PowerBI reports
            print("Step 4: Generating comprehensive PowerBI reports")
            if self.powerbi_generator:
                try:
                    # Generate reports using the processed data
                    etl_data = self.etl_processor.get_processed_data() if hasattr(self.etl_processor, 'get_processed_data') else None
                    
                    if etl_data is not None:
                        # Use processed data from ETL
                        self.powerbi_generator.generate_all_reports_from_data(etl_data)
                    else:
                        # Generate sample data for reports
                        self.powerbi_generator.generate_all_reports()
                    
                    print("PowerBI reports generated successfully")
                except Exception as e:
                    print(f"PowerBI report generation failed: {e}")
            
            # Save pipeline results
            self._save_pipeline_results()
            
            print("Demo completed!")
            
        except Exception as e:
            print(f"Demo execution failed: {e}")
            raise
        finally:
            # Cleanup resources
            print("Cleaning up resources...")
            self.cleanup()
    
    def _generate_sample_data(self):
        """Generate sample honey quality data"""
        import pandas as pd
        import numpy as np
        
        np.random.seed(42)
        
        # Generate sample data
        n_samples = 100
        data = {
            'batch_id': [f'BATCH_{i:03d}' for i in range(1, n_samples + 1)],
            'sample_id': [f'SAMPLE_{i:06d}' for i in range(1, n_samples + 1)],
            'moisture': np.random.uniform(15.0, 20.0, n_samples),
            'ph': np.random.uniform(3.5, 6.5, n_samples),
            'diastase_activity': np.random.uniform(8.0, 15.0, n_samples),
            'h_m_f': np.random.uniform(20.0, 40.0, n_samples),
            'collection_date': pd.date_range('2024-01-01', periods=n_samples, freq='D'),
            'lab_id': np.random.choice(['LAB_A', 'LAB_B', 'LAB_C'], n_samples),
            'analyst': np.random.choice(['Analyst_1', 'Analyst_2', 'Analyst_3'], n_samples)
        }
        
        return pd.DataFrame(data)
    
    def _save_pipeline_results(self):
        """Save pipeline execution results"""
        try:
            output_dir = "output"
            os.makedirs(output_dir, exist_ok=True)
            
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"pipeline_results_{timestamp}.json"
            filepath = os.path.join(output_dir, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                json.dump(self.pipeline_results, f, indent=2, ensure_ascii=False, default=str)
            
            print(f"Pipeline results saved to: {filepath}")
            
        except Exception as e:
            print(f"Failed to save pipeline results: {e}")
    
    def show_status(self):
        """Show system status"""
        print("System Status:")
        print("=" * 50)
        
        # ETL Status
        if self.etl_processor:
            print("ETL Processor: Active")
        else:
            print("ETL Processor: Inactive")
        
        # PowerBI Status
        if self.powerbi_integration:
            print("PowerBI Integration: Active")
        else:
            print("PowerBI Integration: Inactive")
        
        # Monitoring Status
        if self.monitoring_system:
            print("Monitoring System: Active")
        else:
            print("Monitoring System: Inactive")
        
        # Configuration Status
        if os.path.exists(self.config_path):
            print(f"Configuration: Loaded from {self.config_path}")
        else:
            print("Configuration: Not found")
        
        print("=" * 50)
    
    def cleanup(self):
        """Cleanup all resources"""
        try:
            if self.etl_processor:
                self.etl_processor.cleanup()
            
            if self.monitoring_system:
                self.monitoring_system.cleanup()
            
            print("Resources cleaned up successfully")
            
        except Exception as e:
            print(f"Cleanup failed: {e}")


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description="Industrial Honey Quality Analysis Data Pipeline"
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Run quick demo of the pipeline"
    )
    parser.add_argument(
        "--status",
        action="store_true",
        help="Show system status"
    )
    parser.add_argument(
        "--config",
        default="config/etl_config.json",
        help="Path to configuration file"
    )
    
    args = parser.parse_args()
    
    try:
        pipeline = IndustrialHoneyPipeline(args.config)
        
        if args.demo:
            pipeline.run_demo()
        elif args.status:
            pipeline.show_status()
        else:
            parser.print_help()
            
    except KeyboardInterrupt:
        print("\nPipeline interrupted by user")
    except Exception as e:
        print(f"Pipeline execution failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
