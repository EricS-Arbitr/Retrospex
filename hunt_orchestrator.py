from pyspark.sql import SparkSession
from datetime import datetime, date, timedelta
import sys
import os
from pathlib import Path

sys.path.append('/home/eric_s/dev_work/github.com/EricS-Arbitr/retro-hunt-lab/end_to_end/hunt_library')

from hunt_base import HuntBase
from hunt_c2_beaconing import HuntC2Beaconing
from hunt_dns_tunneling import HuntDNSTunneling
from hunt_lateral_movement import HuntLateralMovement

class HuntOrchestrator:
    """Orchestrate execution of multiple hunts"""
    
    def __init__(self):
        self.spark = self._create_spark_session()
        self.mysql_config = {
            'host': '172.25.41.34',
            'port': 3306,
            'database': 'hunt_results',
            'user': 'root',
            'password': 'We3King$'
        }
        # Register available hunts
        self.hunts = {
            'c2_beaconing': HuntC2Beaconing(self.spark, self.mysql_config),
            'dns_tunneling': HuntDNSTunneling(self.spark, self.mysql_config),
            'lateral_movement': HuntLateralMovement(self.spark, self.mysql_config)
        }

    def _create_spark_session(self):
        """Initialize Spark with Delta Lake support"""
        spark = SparkSession.builder \
            .master("local[*]") \
            .appName("HuntOrchestrator") \
            .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.1.0") \
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
            .config("spark.driver.memory", "8g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.debug.maxToStringFields", "100") \
            .getOrCreate()

        # Reduce logging noise
        spark.sparkContext.setLogLevel("ERROR")

        return spark
    
    def run_hunt(self, hunt_name, start_date, end_date, **kwargs):
        """
        Execute a single hunt
        
        Args:
            hunt_name: Name of hunt to execute
            start_date: Start date (YYYY-MM-DD string or date object)
            end_date: End date (YYYY-MM-DD string or date object)
            **kwargs: Hunt-specific parameters
            
        Returns:
            Dict with execution results
        """
        if hunt_name not in self.hunts:
            raise ValueError(f"Unknown hunt: {hunt_name}. Available: {list(self.hunts.keys())}")
        
        print(f"\n{'=' * 70}")
        print(f"EXECUTING HUNT: {hunt_name}")
        print(f"Date Range: {start_date} to {end_date}")
        print(f"Parameters: {kwargs}")
        print(f"{'=' * 70}\n")
        
        hunt = self.hunts[hunt_name]
        start_time = datetime.now()
        
        try:
            results = hunt.execute(start_date, end_date, **kwargs)
            execution_time = (datetime.now() - start_time).total_seconds()
            
            print(f"\n{'=' * 70}")
            print(f"HUNT COMPLETE: {hunt_name}")
            print(f"Status: {results['status']}")
            print(f"Findings: {results['findings_count']}")
            print(f"Records Analyzed: {results['records_analyzed']:,}")
            print(f"Execution Time: {execution_time:.2f}s")
            print(f"{'=' * 70}\n")
            
            return results
            
        except Exception as e:
            print(f"\n✗ Hunt failed: {e}")
            raise
 
    def run_all_hunts(self, start_date, end_date, **global_kwargs):
        """
        Execute all registered hunts
        
        Args:
            start_date: Start date for all hunts
            end_date: End date for all hunts
            **global_kwargs: Parameters applied to all hunts
            
        Returns:
            List of result dicts
        """
        print(f"\n{'#' * 70}")
        print(f"STARTING HUNT CAMPAIGN")
        print(f"Date Range: {start_date} to {end_date}")
        print(f"Hunts: {len(self.hunts)}")
        print(f"{'#' * 70}\n")
        
        results = []
        campaign_start = datetime.now()
        
        for hunt_name in self.hunts.keys():
            try:
                result = self.run_hunt(hunt_name, start_date, end_date, **global_kwargs)
                results.append(result)
            except Exception as e:
                print(f"⚠ Skipping {hunt_name} due to error: {e}")
                results.append({
                    'hunt_name': hunt_name,
                    'status': 'failed',
                    'error': str(e)
                })
        
        campaign_time = (datetime.now() - campaign_start).total_seconds()
        
        # Summary
        successful = sum(1 for r in results if r.get('status') == 'completed')
        total_findings = sum(r.get('findings_count', 0) for r in results)
        
        print(f"\n{'#' * 70}")
        print(f"HUNT CAMPAIGN COMPLETE")
        print(f"Successful Hunts: {successful}/{len(results)}")
        print(f"Total Findings: {total_findings}")
        print(f"Campaign Time: {campaign_time:.2f}s")
        print(f"{'#' * 70}\n")
        
        return results
    
    def run_lookback_hunt(self, hunt_name, lookback_days=30, **kwargs):
        """
        Execute hunt for last N days
        
        Args:
            hunt_name: Name of hunt to execute
            lookback_days: Number of days to look back (default: 30)
            **kwargs: Hunt-specific parameters
            
        Returns:
            Dict with execution results
        """
        end_date = date.today()
        start_date = end_date - timedelta(days=lookback_days)
        
        return self.run_hunt(hunt_name, start_date, end_date, **kwargs)
    
    def cleanup(self):
        """Stop Spark session"""
        if self.spark:
            self.spark.stop()


def main():
    """Example usage"""
    orchestrator = HuntOrchestrator()
    
    try:
        # Example 1: Run single hunt for last 90 days
        print("Example 1: Single hunt with 90-day lookback")
        orchestrator.run_lookback_hunt(
            'c2_beaconing',
            lookback_days=2190,
            min_connections=15,
            jitter_threshold=300
        )
        
        # Example 2: Run all hunts for specific date range
        print("\nExample 2: All hunts for specific date range")
        orchestrator.run_all_hunts(
            start_date='2020-01-01',
            end_date='2023-01-31'
        )
        
        # Example 3: Run targeted hunt with custom parameters
        print("\nExample 3: Targeted DNS tunneling hunt")
        orchestrator.run_hunt(
            'dns_tunneling',
            start_date='2020-06-01',
            end_date='2025-06-30',
            min_query_length=40,
            min_query_count=50
        )
        
    finally:
        orchestrator.cleanup()


if __name__ == "__main__":
    main()
