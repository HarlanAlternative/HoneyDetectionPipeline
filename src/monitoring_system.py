#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Monitoring System for Industrial Honey Quality Pipeline
Provides metrics collection, alerting and performance monitoring
"""

import os
import json
import time
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
import warnings

# Metrics collection
try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    warnings.warn("psutil not available for system metrics")

# Prometheus metrics
try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
    PROMETHEUS_AVAILABLE = True
except ImportError:
    PROMETHEUS_AVAILABLE = False
    warnings.warn("Prometheus client not available")

# Structured logging
try:
    import structlog
    STRUCTLOG_AVAILABLE = True
except ImportError:
    STRUCTLOG_AVAILABLE = False


@dataclass
class MonitoringConfig:
    """Configuration for monitoring system"""
    enabled: bool
    metrics_port: int
    collect_pipeline_metrics: bool
    collect_data_quality_metrics: bool
    collect_performance_metrics: bool
    alert_thresholds: Dict[str, float]


class MonitoringSystem:
    """Comprehensive monitoring and alerting system"""
    
    def __init__(self, config_path: str = "config/etl_config.json"):
        """Initialize monitoring system"""
        self.config = self._load_config(config_path)
        self.logger = self._setup_logging()
        self.metrics = {}
        self.alerts = []
        self.monitoring_active = False
        
        if self.config.enabled:
            self._initialize_metrics()
    
    def _load_config(self, config_path: str) -> MonitoringConfig:
        """Load monitoring configuration"""
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config_data = json.load(f)
            
            monitoring_config = config_data.get('monitoring', {})
            
            return MonitoringConfig(
                enabled=monitoring_config.get('enabled', True),
                metrics_port=monitoring_config.get('metrics_port', 9090),
                collect_pipeline_metrics=monitoring_config.get('metrics', {}).get('collect_pipeline_metrics', True),
                collect_data_quality_metrics=monitoring_config.get('metrics', {}).get('collect_data_quality_metrics', True),
                collect_performance_metrics=monitoring_config.get('metrics', {}).get('collect_performance_metrics', True),
                alert_thresholds=monitoring_config.get('alerts', {})
            )
        except Exception as e:
            # Default configuration
            return MonitoringConfig(
                enabled=True,
                metrics_port=9090,
                collect_pipeline_metrics=True,
                collect_data_quality_metrics=True,
                collect_performance_metrics=True,
                alert_thresholds={
                    "quality_threshold_breach": 0.1,
                    "pipeline_failure": 0.05,
                    "performance_degradation": 0.2
                }
            )
    
    def _setup_logging(self) -> Any:
        """Setup logging system"""
        if STRUCTLOG_AVAILABLE:
            return structlog.get_logger()
        else:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            return logging.getLogger(__name__)
    
    def _initialize_metrics(self):
        """Initialize Prometheus metrics"""
        if not PROMETHEUS_AVAILABLE:
            self.logger.warning("Prometheus metrics not available")
            return
        
        try:
            # Pipeline metrics
            if self.config.collect_pipeline_metrics:
                self.metrics['pipeline_runs_total'] = Counter(
                    'pipeline_runs_total',
                    'Total number of pipeline runs',
                    ['status', 'pipeline_name']
                )
                self.metrics['pipeline_duration_seconds'] = Histogram(
                    'pipeline_duration_seconds',
                    'Pipeline execution duration in seconds',
                    ['pipeline_name']
                )
            
            # Data quality metrics
            if self.config.collect_data_quality_metrics:
                self.metrics['data_quality_score'] = Gauge(
                    'data_quality_score',
                    'Data quality score (0-100)',
                    ['dataset_name']
                )
                self.metrics['quality_violations_total'] = Counter(
                    'quality_violations_total',
                    'Total number of quality violations',
                    ['violation_type', 'dataset_name']
                )
            
            # Performance metrics
            if self.config.collect_performance_metrics:
                self.metrics['system_cpu_usage'] = Gauge(
                    'system_cpu_usage',
                    'System CPU usage percentage'
                )
                self.metrics['system_memory_usage'] = Gauge(
                    'system_memory_usage',
                    'System memory usage percentage'
                )
                self.metrics['system_disk_usage'] = Gauge(
                    'system_disk_usage',
                    'System disk usage percentage'
                )
            
            self.logger.info("Monitoring metrics initialized successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to initialize metrics: {e}")
    
    def start_monitoring(self):
        """Start monitoring system"""
        if not self.config.enabled:
            self.logger.info("Monitoring system is disabled")
            return
        
        try:
            # Start Prometheus HTTP server
            if PROMETHEUS_AVAILABLE:
                start_http_server(self.config.metrics_port)
                self.logger.info(f"Prometheus metrics server started on port {self.config.metrics_port}")
            
            # Start system monitoring
            if self.config.collect_performance_metrics and PSUTIL_AVAILABLE:
                self._start_system_monitoring()
            
            self.monitoring_active = True
            self.logger.info("Monitoring system started successfully")
            
        except Exception as e:
            self.logger.error(f"Failed to start monitoring: {e}")
    
    def _start_system_monitoring(self):
        """Start system resource monitoring"""
        def collect_system_metrics():
            try:
                # CPU usage
                cpu_percent = psutil.cpu_percent(interval=1)
                if 'system_cpu_usage' in self.metrics:
                    self.metrics['system_cpu_usage'].set(cpu_percent)
                
                # Memory usage
                memory = psutil.virtual_memory()
                if 'system_memory_usage' in self.metrics:
                    self.metrics['system_memory_usage'].set(memory.percent)
                
                # Disk usage
                disk = psutil.disk_usage('/')
                if 'system_disk_usage' in self.metrics:
                    self.metrics['system_disk_usage'].set((disk.used / disk.total) * 100)
                
            except Exception as e:
                self.logger.error(f"System metrics collection failed: {e}")
        
        # Run system monitoring in background
        import threading
        self.system_monitor_thread = threading.Thread(
            target=self._run_system_monitor,
            args=(collect_system_metrics,),
            daemon=True
        )
        self.system_monitor_thread.start()
    
    def _run_system_monitor(self, metrics_collector):
        """Run system monitoring loop"""
        while self.monitoring_active:
            try:
                metrics_collector()
                time.sleep(30)  # Collect metrics every 30 seconds
            except Exception as e:
                self.logger.error(f"System monitoring loop failed: {e}")
                time.sleep(60)  # Wait longer on error
    
    def record_pipeline_run(self, pipeline_name: str, status: str, duration: float):
        """Record pipeline execution metrics"""
        if not self.monitoring_active or not self.config.collect_pipeline_metrics:
            return
        
        try:
            if 'pipeline_runs_total' in self.metrics:
                self.metrics['pipeline_runs_total'].labels(
                    status=status,
                    pipeline_name=pipeline_name
                ).inc()
            
            if 'pipeline_duration_seconds' in self.metrics:
                self.metrics['pipeline_duration_seconds'].labels(
                    pipeline_name=pipeline_name
                ).observe(duration)
            
            self.logger.info(f"Pipeline metrics recorded: {pipeline_name} - {status} - {duration:.2f}s")
            
        except Exception as e:
            self.logger.error(f"Failed to record pipeline metrics: {e}")
    
    def record_data_quality(self, dataset_name: str, quality_score: float, violations: Dict[str, int]):
        """Record data quality metrics"""
        if not self.monitoring_active or not self.config.collect_data_quality_metrics:
            return
        
        try:
            if 'data_quality_score' in self.metrics:
                self.metrics['data_quality_score'].labels(
                    dataset_name=dataset_name
                ).set(quality_score)
            
            if 'quality_violations_total' in self.metrics:
                for violation_type, count in violations.items():
                    self.metrics['quality_violations_total'].labels(
                        violation_type=violation_type,
                        dataset_name=dataset_name
                    ).inc(count)
            
            self.logger.info(f"Data quality metrics recorded: {dataset_name} - Score: {quality_score:.2f}")
            
        except Exception as e:
            self.logger.error(f"Failed to record data quality metrics: {e}")
    
    def check_alerts(self) -> List[Dict[str, Any]]:
        """Check for alert conditions"""
        alerts = []
        
        try:
            # Check quality threshold breaches
            if self.config.collect_data_quality_metrics:
                quality_alerts = self._check_quality_alerts()
                alerts.extend(quality_alerts)
            
            # Check performance degradation
            if self.config.collect_performance_metrics:
                performance_alerts = self._check_performance_alerts()
                alerts.extend(performance_alerts)
            
            # Store alerts
            self.alerts.extend(alerts)
            
            # Log alerts
            for alert in alerts:
                self.logger.warning(f"Alert triggered: {alert['message']}")
            
        except Exception as e:
            self.logger.error(f"Alert checking failed: {e}")
        
        return alerts
    
    def _check_quality_alerts(self) -> List[Dict[str, Any]]:
        """Check data quality alert conditions"""
        alerts = []
        
        try:
            # This would typically check against actual quality metrics
            # For now, return empty list
            pass
            
        except Exception as e:
            self.logger.error(f"Quality alert checking failed: {e}")
        
        return alerts
    
    def _check_performance_alerts(self) -> List[Dict[str, Any]]:
        """Check performance alert conditions"""
        alerts = []
        
        try:
            if PSUTIL_AVAILABLE:
                # CPU usage alert
                cpu_percent = psutil.cpu_percent(interval=1)
                if cpu_percent > 80:
                    alerts.append({
                        'type': 'performance_degradation',
                        'severity': 'warning',
                        'message': f'High CPU usage: {cpu_percent:.1f}%',
                        'timestamp': datetime.now().isoformat()
                    })
                
                # Memory usage alert
                memory = psutil.virtual_memory()
                if memory.percent > 85:
                    alerts.append({
                        'type': 'performance_degradation',
                        'severity': 'warning',
                        'message': f'High memory usage: {memory.percent:.1f}%',
                        'timestamp': datetime.now().isoformat()
                    })
                
        except Exception as e:
            self.logger.error(f"Performance alert checking failed: {e}")
        
        return alerts
    
    def get_metrics_summary(self) -> Dict[str, Any]:
        """Get summary of current metrics"""
        summary = {
            'timestamp': datetime.now().isoformat(),
            'monitoring_active': self.monitoring_active,
            'alerts_count': len(self.alerts),
            'metrics_available': list(self.metrics.keys())
        }
        
        try:
            if PSUTIL_AVAILABLE:
                summary['system'] = {
                    'cpu_percent': psutil.cpu_percent(interval=1),
                    'memory_percent': psutil.virtual_memory().percent,
                    'disk_percent': (psutil.disk_usage('/').used / psutil.disk_usage('/').total) * 100
                }
        except Exception as e:
            self.logger.error(f"Failed to collect system metrics: {e}")
        
        return summary
    
    def export_metrics(self, format_type: str = "json") -> str:
        """Export metrics to specified format"""
        try:
            if format_type == "json":
                return self._export_json_metrics()
            elif format_type == "csv":
                return self._export_csv_metrics()
            else:
                raise ValueError(f"Unsupported export format: {format_type}")
                
        except Exception as e:
            self.logger.error(f"Metrics export failed: {e}")
            raise
    
    def _export_json_metrics(self) -> str:
        """Export metrics to JSON format"""
        output_dir = "monitoring_metrics"
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"metrics_export_{timestamp}.json"
        filepath = os.path.join(output_dir, filename)
        
        export_data = {
            'timestamp': datetime.now().isoformat(),
            'metrics_summary': self.get_metrics_summary(),
            'alerts': self.alerts,
            'configuration': {
                'enabled': self.config.enabled,
                'metrics_port': self.config.metrics_port,
                'collect_pipeline_metrics': self.config.collect_pipeline_metrics,
                'collect_data_quality_metrics': self.config.collect_data_quality_metrics,
                'collect_performance_metrics': self.config.collect_performance_metrics
            }
        }
        
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(export_data, f, indent=2, ensure_ascii=False, default=str)
        
        self.logger.info(f"Metrics exported to JSON: {filepath}")
        return filepath
    
    def _export_csv_metrics(self) -> str:
        """Export metrics to CSV format"""
        output_dir = "monitoring_metrics"
        os.makedirs(output_dir, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"metrics_export_{timestamp}.csv"
        filepath = os.path.join(output_dir, filename)
        
        # This would create a CSV with metrics data
        # For now, just create a simple CSV
        import csv
        
        with open(filepath, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(['Metric', 'Value', 'Timestamp'])
            writer.writerow(['monitoring_active', self.monitoring_active, datetime.now().isoformat()])
            writer.writerow(['alerts_count', len(self.alerts), datetime.now().isoformat()])
        
        self.logger.info(f"Metrics exported to CSV: {filepath}")
        return filepath
    
    def cleanup(self):
        """Cleanup monitoring resources"""
        try:
            self.monitoring_active = False
            
            # Wait for system monitor thread to finish
            if hasattr(self, 'system_monitor_thread'):
                self.system_monitor_thread.join(timeout=5)
            
            self.logger.info("Monitoring system cleaned up successfully")
            
        except Exception as e:
            self.logger.error(f"Monitoring cleanup failed: {e}")


if __name__ == "__main__":
    # Example usage
    monitor = MonitoringSystem()
    
    try:
        monitor.start_monitoring()
        time.sleep(10)  # Run for 10 seconds
        
        summary = monitor.get_metrics_summary()
        print(f"Metrics Summary: {summary}")
        
        alerts = monitor.check_alerts()
        print(f"Alerts: {alerts}")
        
    finally:
        monitor.cleanup()
