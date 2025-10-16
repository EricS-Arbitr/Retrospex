# hunt_library/hunt_deduplication.py
from sqlalchemy import create_engine, text
from datetime import datetime, timedelta
import hashlib
import json

class HuntDeduplicator:
    """Deduplicate hunt findings to prevent alert fatigue"""
    
    def __init__(self, mysql_config):
        self.mysql_config = mysql_config
        self.engine = self._create_engine()
    
    def _create_engine(self):
        """Create SQLAlchemy engine"""
        connection_string = (
            f"mysql+pymysql://{self.mysql_config['user']}:{self.mysql_config['password']}"
            f"@{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}"
        )
        return create_engine(connection_string)
    
    def generate_finding_hash(self, finding):
        """
        Generate unique hash for a finding based on key attributes
        
        Args:
            finding: Dict with finding details
            
        Returns:
            Hash string
        """
        # Create hash from key attributes
        key_attributes = [
            finding.get('finding_type', ''),
            finding.get('source_ip', ''),
            finding.get('destination_ip', ''),
            finding.get('destination_hostname', ''),
            finding.get('username', '')
        ]
        
        # Join and hash
        key_string = '|'.join(str(attr) for attr in key_attributes)
        return hashlib.sha256(key_string.encode()).hexdigest()
    
    def is_duplicate(self, finding, lookback_days=7):
        """
        Check if finding is a duplicate of recent finding
        
        Args:
            finding: Dict with finding details
            lookback_days: How many days to look back
            
        Returns:
            Boolean indicating if duplicate
        """
        finding_hash = self.generate_finding_hash(finding)
        cutoff_date = datetime.now() - timedelta(days=lookback_days)
        
        query = text("""
            SELECT COUNT(*) as count
            FROM hunt_findings
            WHERE finding_type = :finding_type
                AND source_ip = :source_ip
                AND timestamp >= :cutoff_date
        """)
        
        with self.engine.connect() as conn:
            result = conn.execute(
                query,
                {
                    'finding_type': finding.get('finding_type'),
                    'source_ip': finding.get('source_ip'),
                    'cutoff_date': cutoff_date
                }
            ).fetchone()
            
            return result[0] > 0
        
    def filter_duplicates(self, findings, lookback_days=7):
        """
        Filter out duplicate findings
        
        Args:
            findings: List of finding dicts
            lookback_days: How many days to look back
            
        Returns:
            List of unique findings
        """
        unique_findings = []
        duplicate_count = 0
        
        for finding in findings:
            if not self.is_duplicate(finding, lookback_days):
                unique_findings.append(finding)
            else:
                duplicate_count += 1
        
        print(f"  Filtered {duplicate_count} duplicate findings")
        print(f"  Kept {len(unique_findings)} unique findings")
        
        return unique_findings
    
    def update_ioc_tracker(self, findings):
        """
        Update IOC tracking table with new findings
        
        Args:
            findings: List of finding dicts
        """
        for finding in findings:
            # Track source IPs
            if finding.get('source_ip'):
                self._update_ioc(
                    ioc_type='ip',
                    ioc_value=finding['source_ip'],
                    timestamp=finding['timestamp'],
                    hunt_name=finding.get('finding_type'),
                    threat_level=finding.get('severity', 'medium')
                )
            
            # Track destination IPs
            if finding.get('destination_ip'):
                self._update_ioc(
                    ioc_type='ip',
                    ioc_value=finding['destination_ip'],
                    timestamp=finding['timestamp'],
                    hunt_name=finding.get('finding_type'),
                    threat_level=finding.get('severity', 'medium')
                )
            
            # Track domains
            if finding.get('destination_hostname'):
                self._update_ioc(
                    ioc_type='domain',
                    ioc_value=finding['destination_hostname'],
                    timestamp=finding['timestamp'],
                    hunt_name=finding.get('finding_type'),
                    threat_level=finding.get('severity', 'medium')
                )
    
    def _update_ioc(self, ioc_type, ioc_value, timestamp, hunt_name, threat_level):
        """Update single IOC in tracking table"""

        # Convert hunt_name to JSON array for initial insert
        hunt_name_json = json.dumps([hunt_name])

        query = text("""
            INSERT INTO hunt_iocs
                (ioc_type, ioc_value, first_seen, last_seen, times_seen,
                 associated_hunts, threat_level)
            VALUES
                (:ioc_type, :ioc_value, :timestamp, :timestamp, 1,
                 :hunt_name_json, :threat_level)
            ON DUPLICATE KEY UPDATE
                last_seen = :timestamp,
                times_seen = times_seen + 1,
                associated_hunts = JSON_ARRAY_APPEND(
                    COALESCE(associated_hunts, JSON_ARRAY()),
                    '$',
                    :hunt_name
                ),
                threat_level = CASE
                    WHEN :threat_level = 'critical' THEN 'critical'
                    WHEN :threat_level = 'high' AND threat_level != 'critical' THEN 'high'
                    WHEN :threat_level = 'medium' AND threat_level NOT IN ('critical', 'high') THEN 'medium'
                    ELSE threat_level
                END
        """)

        with self.engine.connect() as conn:
            conn.execute(
                query,
                {
                    'ioc_type': ioc_type,
                    'ioc_value': ioc_value,
                    'timestamp': timestamp,
                    'hunt_name_json': hunt_name_json,  # JSON array for INSERT
                    'hunt_name': hunt_name,             # Plain string for ARRAY_APPEND
                    'threat_level': threat_level
                }
            )
            conn.commit()
            