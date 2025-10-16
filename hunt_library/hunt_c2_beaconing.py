# hunt_library/hunt_c2_beaconing.py
from hunt_base import HuntBase
from pyspark.sql.functions import *
from pyspark.sql.window import Window
from datetime import datetime
import json
import builtins  # To access Python's built-in round()

class HuntC2Beaconing(HuntBase):
    """
    Hunt for C2 beaconing behavior in network connections
    
    Detection Logic:
    - Periodic connections to same destination
    - Consistent byte patterns
    - Long-running communication sessions
    - Unusual connection timing (off-hours)
    """
    
    def __init__(self, spark, mysql_config):
        super().__init__(spark, mysql_config)
        self.hunt_version = "1.0"
        self.description = "Detect periodic beaconing indicative of C2 communication"
    
    def hunt_logic(self, start_date, end_date, **kwargs):
        """
        Execute C2 beaconing detection
        
        Parameters:
            min_connections: Minimum connection count (default: 10)
            jitter_threshold: Max time variance in seconds (default: 300)
            byte_similarity_threshold: Min byte pattern similarity (default: 0.8)
        """
        min_connections = kwargs.get('min_connections', 10)
        jitter_threshold = kwargs.get('jitter_threshold', 300)
        
        print(f"Hunting for C2 beaconing from {start_date} to {end_date}")
        print(f"Parameters: min_connections={min_connections}, jitter_threshold={jitter_threshold}")
        

        # Read Zeek connection logs
        delta_path = "/home/eric_s/dev_work/github.com/EricS-Arbitr/retro-hunt-lab/data2/zeek_conn_logs"
        df = self.read_delta_table(delta_path, start_date, end_date)
        
        records_analyzed = df.count()
        print(f"Analyzing {records_analyzed:,} connection records...")
        
        # Calculate connection intervals for each source-destination pair
        # First alias the columns, then use the aliases in the window spec
        df_renamed = df.select(
            col("ts"),
            col("id_orig_h").alias("source_ip"),
            col("id_resp_h").alias("destination_ip"),
            col("id_resp_p").alias("destination_port"),
            col("orig_bytes"),
            col("resp_bytes"),
            col("duration")
        )

        window_spec = Window.partitionBy("source_ip", "destination_ip", "destination_port").orderBy("ts")

        df_intervals = df_renamed.withColumn(
            "prev_ts",
            lag("ts").over(window_spec)
        ).withColumn(
            "interval_seconds",
            when(col("prev_ts").isNotNull(),
                 unix_timestamp(col("ts")) - unix_timestamp(col("prev_ts")))
        )
        
        # Find connections with regular intervals
        beaconing_candidates = df_intervals.groupBy(
            "source_ip", "destination_ip", "destination_port"
        ).agg(
            count("*").alias("connection_count"),
            avg("interval_seconds").alias("avg_interval"),
            stddev("interval_seconds").alias("interval_stddev"),
            avg("orig_bytes").alias("avg_bytes_sent"),
            stddev("orig_bytes").alias("bytes_sent_stddev"),
            min(unix_timestamp("ts")).alias("first_seen"),
            max(unix_timestamp("ts")).alias("last_seen")
        ).filter(
            (col("connection_count") >= min_connections) &
            (col("interval_stddev") < jitter_threshold) &
            (col("avg_interval").isNotNull()) &
            (col("avg_interval") > 0)
        )
        
        # Calculate confidence score
        beaconing_candidates = beaconing_candidates.withColumn(
            "confidence_score",
            least(
                lit(100.0),
                (
                    (col("connection_count") / lit(10.0) * lit(30)) +  # Connection frequency
                    ((lit(1) - (col("interval_stddev") / col("avg_interval"))) * lit(40)) +  # Regularity
                    ((lit(1) - (col("bytes_sent_stddev") / col("avg_bytes_sent"))) * lit(30))  # Byte consistency
                )
            )
        ).filter(
            col("confidence_score") > 50
        )
        
        # Collect findings
        findings_list = []
        for row in beaconing_candidates.collect():
            finding = {
                'severity': 'high' if row['confidence_score'] > 80 else 'medium',
                'finding_type': 'c2_beaconing',
                'source_ip': row['source_ip'],
                'destination_ip': row['destination_ip'],
                'timestamp': datetime.fromtimestamp(float(row['first_seen'])),
                'confidence_score': builtins.round(float(row['confidence_score']), 2),
                'description': (
                    f"Potential C2 beaconing detected: {row['connection_count']} connections "
                    f"with {row['avg_interval']:.1f}s average interval "
                    f"(±{row['interval_stddev']:.1f}s jitter) "
                    f"to {row['destination_ip']}:{row['destination_port']}"
                ),
                'raw_data': json.dumps({
                    'connection_count': int(row['connection_count']),
                    'avg_interval_seconds': float(row['avg_interval']),
                    'interval_stddev': float(row['interval_stddev']),
                    'avg_bytes_sent': int(row['avg_bytes_sent']) if row['avg_bytes_sent'] else 0,
                    'first_seen': str(datetime.fromtimestamp(float(row['first_seen']))),
                    'last_seen': str(datetime.fromtimestamp(float(row['last_seen'])))
                })
            }
            findings_list.append(finding)
        
        print(f"✓ Found {len(findings_list)} potential C2 beaconing indicators")
        
        return {
            'findings': findings_list,
            'records_analyzed': records_analyzed
        }
    