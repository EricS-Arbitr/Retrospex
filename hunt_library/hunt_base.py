from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime, date
import json
from sqlalchemy import create_engine, text
import pandas as pd

class HuntBase:
    """Base class for all retrospective hunts"""
    
    def __init__(self, spark, mysql_config):
        """
        Initialize hunt
        
        Args:
            spark: Active SparkSession with Delta Lake support
            mysql_config: Dict with MySQL connection parameters
        """
        self.spark = spark
        self.mysql_config = mysql_config
        self.hunt_name = self.__class__.__name__
        self.hunt_version = "1.0"
        self.execution_id = None
        self.findings = []

    def execute(self, start_date, end_date, **kwargs):
        """
        Execute the hunt
        
        Args:
            start_date: Start date for hunt (date object or string YYYY-MM-DD)
            end_date: End date for hunt (date object or string YYYY-MM-DD)
            **kwargs: Additional hunt-specific parameters
            
        Returns:
            Dict with execution results
        """
        # Convert string dates to date objects
        if isinstance(start_date, str):
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        if isinstance(end_date, str):
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
        
        # Start execution tracking
        self.execution_id = self._start_execution(start_date, end_date, kwargs)
        
        try:
            # Run the hunt logic (implemented by subclass)
            results = self.hunt_logic(start_date, end_date, **kwargs)
            
            # Store findings
            if results and len(results) > 0:
                self._store_findings(results)
            
            # Complete execution tracking
            self._complete_execution(
                status='completed',
                records_analyzed=results.get('records_analyzed', 0) if results else 0,
                findings_count=len(self.findings)
            )
            
            return {
                'execution_id': self.execution_id,
                'hunt_name': self.hunt_name,
                'status': 'completed',
                'findings_count': len(self.findings),
                'records_analyzed': results.get('records_analyzed', 0) if results else 0
            }
            
        except Exception as e:
            self._complete_execution(status='failed', error_message=str(e))
            raise

    def hunt_logic(self, start_date, end_date, **kwargs):
        """
        Implement hunt-specific logic (override in subclass)
        
        Returns:
            Dict with 'findings' list and 'records_analyzed' count
        """
        raise NotImplementedError("Subclass must implement hunt_logic()")

    def _start_execution(self, start_date, end_date, parameters):
        """Record hunt execution start in MySQL"""
        engine = self._get_mysql_engine()

        query = text("""
        INSERT INTO hunt_executions
        (hunt_name, hunt_version, execution_start, status, date_range_start, date_range_end, parameters)
        VALUES (:hunt_name, :hunt_version, :execution_start, :status, :date_range_start, :date_range_end, :parameters)
        """)

        with engine.connect() as conn:
            result = conn.execute(
                query,
                {
                    'hunt_name': self.hunt_name,
                    'hunt_version': self.hunt_version,
                    'execution_start': datetime.now(),
                    'status': 'running',
                    'date_range_start': start_date,
                    'date_range_end': end_date,
                    'parameters': json.dumps(parameters)
                }
            )
            conn.commit()
            return result.lastrowid
    
    def _complete_execution(self, status, records_analyzed=0, findings_count=0, error_message=None):
        """Record hunt execution completion"""
        engine = self._get_mysql_engine()

        query = text("""
        UPDATE hunt_executions
        SET execution_end = :execution_end, status = :status, records_analyzed = :records_analyzed,
            findings_count = :findings_count, error_message = :error_message
        WHERE execution_id = :execution_id
        """)

        with engine.connect() as conn:
            conn.execute(
                query,
                {
                    'execution_end': datetime.now(),
                    'status': status,
                    'records_analyzed': records_analyzed,
                    'findings_count': findings_count,
                    'error_message': error_message,
                    'execution_id': self.execution_id
                }
            )
            conn.commit()
    
    def _store_findings(self, results):
        """Store findings in MySQL"""
        if 'findings' not in results or len(results['findings']) == 0:
            return
        
        # Import deduplicator
        from hunt_deduplication import HuntDeduplicator

        # Filter duplicates
        deduplicator = HuntDeduplicator(self.mysql_config)
        unique_findings = deduplicator.filter_duplicates(
            results['findings'],
            lookback_days=7
        )
    
        if len(unique_findings) == 0:
            print("  ℹ All findings were duplicates - nothing to store")
            return

        # Store unique findings
        findings_df = pd.DataFrame(unique_findings)
        findings_df['execution_id'] = self.execution_id
        findings_df['hunt_name'] = self.hunt_name
        findings_df['created_at'] = datetime.now()

        # Ensure required columns exist
        required_cols = ['severity', 'finding_type', 'description', 'timestamp']
        for col in required_cols:
            if col not in findings_df.columns:
                findings_df[col] = None
        
        engine = self._get_mysql_engine()
        findings_df.to_sql(
            'hunt_findings',
            engine,
            if_exists='append',
            index=False,
            chunksize=1000
        )
        
        # Update IOC tracker
        deduplicator.update_ioc_tracker(unique_findings)

        self.findings = unique_findings
        print(f"  ✓ Stored {len(unique_findings)} unique findings")
    
    def _get_mysql_engine(self):
        """Create SQLAlchemy engine for MySQL"""
        connection_string = (
            f"mysql+pymysql://{self.mysql_config['user']}:{self.mysql_config['password']}"
            f"@{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}"
        )
        return create_engine(connection_string)
    
    def read_delta_table(self, table_path, start_date, end_date):
        """
        Read Delta table with date filtering

        Args:
            table_path: Path to Delta table (can be parent dir with partitioned tables)
            start_date: Start date (date object)
            end_date: End date (date object)

        Returns:
            Filtered DataFrame
        """
        import os
        from pathlib import Path

        # Check if this is a partitioned table structure (organization/operation)
        table_path_obj = Path(table_path)
        if not (table_path_obj / "_delta_log").exists():
            # Find all Delta tables in subdirectories
            delta_tables = []
            if table_path_obj.exists():
                for root, dirs, files in os.walk(table_path):
                    if "_delta_log" in dirs:
                        delta_tables.append(root)

            if not delta_tables:
                raise ValueError(f"No Delta tables found at {table_path}")

            # Read and union all partition tables
            df = None
            for delta_path in delta_tables:
                df_part = self.spark.read \
                    .option("mergeSchema", "true") \
                    .format("delta") \
                    .load(delta_path)

                if df is None:
                    df = df_part
                else:
                    df = df.union(df_part)
        else:
            # Single Delta table
            df = self.spark.read \
                .option("mergeSchema", "true") \
                .format("delta") \
                .load(table_path)
        
        # Filter by date if partitioned
        if "year" in df.columns and "month" in df.columns and "day" in df.columns:
            df = df.filter(
                (
                    (col("year") > start_date.year) |
                    (
                        (col("year") == start_date.year) &
                        (col("month") > start_date.month)
                    ) |
                    (
                        (col("year") == start_date.year) &
                        (col("month") == start_date.month) &
                        (col("day") >= start_date.day)
                    )
                ) &
                (
                    (col("year") < end_date.year) |
                    (
                        (col("year") == end_date.year) &
                        (col("month") < end_date.month)
                    ) |
                    (
                        (col("year") == end_date.year) &
                        (col("month") == end_date.month) &
                        (col("day") <= end_date.day)
                    )
                )
            )
        
        return df
    