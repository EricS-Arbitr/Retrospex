import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import config

from hunt_base import HuntBase
from pyspark.sql.functions import *
from datetime import datetime
import json

class HuntDNSTunneling(HuntBase):
    """
    Hunt for DNS tunneling indicators
    
    Detection Logic:
    - Unusually long DNS queries
    - High query frequency to specific domains
    - Suspicious subdomain patterns
    - Large volume of DNS traffic
    """
    
    def __init__(self, spark, mysql_config):
        super().__init__(spark, mysql_config)
        self.hunt_version = "1.0"
        self.description = "Detect DNS tunneling for data exfiltration"
    
    def hunt_logic(self, start_date, end_date, **kwargs):
        """
        Execute DNS tunneling detection
        
        Parameters:
            min_query_length: Minimum suspicious query length (default: 50)
            min_query_count: Minimum queries per source (default: 100)
            entropy_threshold: Minimum domain entropy (default: 3.5)
        """
        min_query_length = kwargs.get('min_query_length', 50)
        min_query_count = kwargs.get('min_query_count', 100)
        
        print(f"Hunting for DNS tunneling from {start_date} to {end_date}")
        print(f"Parameters: min_query_length={min_query_length}, min_query_count={min_query_count}")
        
        # Read DNS logs
        delta_path = str(config.ZEEK_DNS_LOGS)
        df = self.read_delta_table(delta_path, start_date, end_date)
        
        records_analyzed = df.count()
        print(f"Analyzing {records_analyzed:,} DNS query records...")
        
        # Analyze DNS query patterns
        dns_analysis = df.select(
            col("id_orig_h").alias("source_ip"),
            col("query").alias("query_domain"),
            col("ts").alias("timestamp")
        ).withColumn(
            "query_length",
            length(col("query_domain"))
        ).withColumn(
            "subdomain_count",
            size(split(col("query_domain"), "\\."))
        )
        
        # Find suspicious patterns
        suspicious_dns = dns_analysis.groupBy(
            "source_ip", "query_domain"
        ).agg(
            count("*").alias("query_count"),
            avg("query_length").alias("avg_query_length"),
            max("query_length").alias("max_query_length"),
            avg("subdomain_count").alias("avg_subdomain_count"),
            min("timestamp").alias("first_seen"),
            max("timestamp").alias("last_seen")
        ).filter(
            (col("avg_query_length") > min_query_length) |
            (col("query_count") > min_query_count) |
            (col("avg_subdomain_count") > 5)
        )
        
        # Calculate confidence score
        suspicious_dns = suspicious_dns.withColumn(
            "confidence_score",
            least(
                lit(100.0),
                (
                    (col("avg_query_length") / lit(100.0) * 40) +
                    (col("query_count") / lit(1000.0) * 40) +
                    (col("avg_subdomain_count") / lit(10.0) * 20)
                )
            )
        ).filter(
            col("confidence_score") > 50
        )
        
        # Collect findings
        findings_list = []
        for row in suspicious_dns.collect():
            severity = 'critical' if row['confidence_score'] > 80 else 'high'
            
            finding = {
                'severity': severity,
                'finding_type': 'dns_tunneling',
                'source_ip': row['source_ip'],
                'destination_hostname': row['query_domain'],
                'timestamp': datetime.fromisoformat(row['first_seen'].replace('Z', '+00:00')),
                'confidence_score': round(float(row['confidence_score']), 2),
                'description': (
                    f"Potential DNS tunneling: {row['query_count']} queries "
                    f"with avg length {row['avg_query_length']:.0f} chars "
                    f"to {row['query_domain']}"
                ),
                'raw_data': json.dumps({
                    'query_count': int(row['query_count']),
                    'avg_query_length': float(row['avg_query_length']),
                    'max_query_length': int(row['max_query_length']),
                    'avg_subdomain_count': float(row['avg_subdomain_count']),
                    'first_seen': row['first_seen'],
                    'last_seen': row['last_seen']
                })
            }
            findings_list.append(finding)
        
        print(f"✓ Found {len(findings_list)} potential DNS tunneling indicators")
        
        return {
            'findings': findings_list,
            'records_analyzed': records_analyzed
        }