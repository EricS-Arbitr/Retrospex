# hunt_library/hunt_lateral_movement.py
from hunt_base import HuntBase
from pyspark.sql.functions import *
from pyspark.sql.types import BooleanType
from pyspark.sql.window import Window
from datetime import datetime
import json
import builtins

class HuntLateralMovement(HuntBase):
    """
    Hunt for lateral movement using Windows Event logs
    
    Detection Logic:
    - Rapid authentication attempts across multiple systems
    - Unusual account usage patterns
    - Administrative account activity
    - Authentication from unusual source systems
    """
    
    def __init__(self, spark, mysql_config):
        super().__init__(spark, mysql_config)
        self.hunt_version = "1.0"
        self.description = "Detect lateral movement via authentication logs"
    
    def hunt_logic(self, start_date, end_date, **kwargs):
        """
        Execute lateral movement detection
        
        Parameters:
            time_window_minutes: Time window for clustering (default: 60)
            min_hosts: Minimum unique destination hosts (default: 5)
            privileged_accounts: List of privileged account patterns (default: ['admin', 'svc'])
        """
        time_window = kwargs.get('time_window_minutes', 60)
        min_hosts = kwargs.get('min_hosts', 5)
        privileged_patterns = kwargs.get('privileged_accounts', ['admin', 'svc', 'service'])
        
        print(f"Hunting for lateral movement from {start_date} to {end_date}")
        print(f"Parameters: time_window={time_window}min, min_hosts={min_hosts}")
        
        # Read Windows Security Event logs
        delta_path = "/home/eric_s/dev_work/github.com/EricS-Arbitr/retro-hunt-lab/data2/win_security"
        df = self.read_delta_table(delta_path, start_date, end_date)
        
        # Filter for logon events (Event ID 4624, 4625, 4648)
        logon_events = df.filter(
            col("event_code").isin("4624", "4625", "4648")
        ).select(
            col("@timestamp").alias("timestamp"),
            col("event_code"),
            col("user_name").alias("username"),
            col("host_name").alias("destination_host"),
            col("source_ip"),
            col("logon_type")
        )
        
        records_analyzed = logon_events.count()
        print(f"Analyzing {records_analyzed:,} Windows logon events...")
        
        # Create time windows
        logon_events = logon_events.withColumn(
            "time_bucket",
            (unix_timestamp(col("timestamp")) / (time_window * 60)).cast("long")
        )
        
        # Detect rapid movement across hosts
        lateral_movement = logon_events.groupBy(
            "username", "source_ip", "time_bucket"
        ).agg(
            countDistinct("destination_host").alias("unique_hosts"),
            count("*").alias("logon_attempts"),
            collect_set("destination_host").alias("hosts_accessed"),
            min("timestamp").alias("first_logon"),
            max("timestamp").alias("last_logon")
        ).filter(
            col("unique_hosts") >= min_hosts
        )
        
        # Check for privileged accounts
        def is_privileged(username):
            if username is None:
                return False
            username_lower = username.lower()
            return any(pattern in username_lower for pattern in privileged_patterns)
        
        is_privileged_udf = udf(is_privileged, BooleanType())
        
        lateral_movement = lateral_movement.withColumn(
            "is_privileged_account",
            is_privileged_udf(col("username"))
        )
        
        # Calculate confidence score
        lateral_movement = lateral_movement.withColumn(
            "confidence_score",
            least(
                lit(100.0),
                (
                    (col("unique_hosts") / lit(10.0) * 50) +
                    (col("logon_attempts") / lit(20.0) * 30) +
                    when(col("is_privileged_account"), lit(20)).otherwise(lit(0))
                )
            )
        ).filter(
            col("confidence_score") > 50
        )
        
        # Collect findings
        findings_list = []
        for row in lateral_movement.collect():
            severity = 'critical' if row['is_privileged_account'] else 'high'
            
            finding = {
                'severity': severity,
                'finding_type': 'lateral_movement',
                'source_ip': row['source_ip'],
                'username': row['username'],
                'timestamp': datetime.fromisoformat(row['first_logon'].replace('Z', '+00:00')),
                'confidence_score': builtins.round(float(row['confidence_score']), 2),
                'description': (
                    f"Potential lateral movement: Account '{row['username']}' "
                    f"accessed {row['unique_hosts']} unique hosts "
                    f"in {time_window} minutes from {row['source_ip']}"
                ),
                'raw_data': json.dumps({
                    'unique_hosts_count': int(row['unique_hosts']),
                    'logon_attempts': int(row['logon_attempts']),
                    'hosts_accessed': row['hosts_accessed'],
                    'is_privileged_account': bool(row['is_privileged_account']),
                    'first_logon': row['first_logon'],
                    'last_logon': row['last_logon'],
                    'time_window_minutes': time_window
                })
            }
            findings_list.append(finding)
        
        print(f"✓ Found {len(findings_list)} potential lateral movement indicators")
        
        return {
            'findings': findings_list,
            'records_analyzed': records_analyzed
        }