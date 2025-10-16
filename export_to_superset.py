# export_to_superset.py
from sqlalchemy import create_engine, text
import pandas as pd
from datetime import datetime, timedelta
import json
import config

class SupersetExporter:
    """Export hunt results to MySQL for Superset dashboards"""

    def __init__(self, mysql_config=None):
        if mysql_config is None:
            mysql_config = config.MYSQL_CONFIG

        self.mysql_config = mysql_config
        self.engine = self._create_engine()

    def _create_engine(self):
        """Create SQLAlchemy engine"""
        connection_string = (
            f"mysql+pymysql://{self.mysql_config['user']}:{self.mysql_config['password']}"
            f"@{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}"
        )
        return create_engine(connection_string)
    
    def create_dashboard_views(self):
        """Create optimized views for dashboard queries"""
        
        print("Creating dashboard views...")
        
        views = {
            # Daily findings summary
            'vw_daily_findings': """
                CREATE OR REPLACE VIEW vw_daily_findings AS
                SELECT 
                    DATE(timestamp) as finding_date,
                    hunt_name,
                    severity,
                    COUNT(*) as finding_count,
                    AVG(confidence_score) as avg_confidence,
                    COUNT(DISTINCT source_ip) as unique_sources,
                    COUNT(DISTINCT destination_ip) as unique_destinations
                FROM hunt_findings
                WHERE status != 'false_positive'
                GROUP BY DATE(timestamp), hunt_name, severity
            """,
            
            # Hunt performance metrics
            'vw_hunt_performance': """
                CREATE OR REPLACE VIEW vw_hunt_performance AS
                SELECT 
                    hunt_name,
                    DATE(execution_start) as execution_date,
                    COUNT(*) as execution_count,
                    SUM(findings_count) as total_findings,
                    AVG(TIMESTAMPDIFF(SECOND, execution_start, execution_end)) as avg_execution_time,
                    SUM(records_analyzed) as total_records_analyzed,
                    SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as successful_runs,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_runs
                FROM hunt_executions
                GROUP BY hunt_name, DATE(execution_start)
            """,
            
            # Severity distribution
            'vw_severity_distribution': """
                CREATE OR REPLACE VIEW vw_severity_distribution AS
                SELECT 
                    severity,
                    finding_type,
                    COUNT(*) as count,
                    AVG(confidence_score) as avg_confidence
                FROM hunt_findings
                WHERE status != 'false_positive'
                GROUP BY severity, finding_type
            """,
            
            # Top sources by findings
            'vw_top_sources': """
                CREATE OR REPLACE VIEW vw_top_sources AS
                SELECT 
                    source_ip,
                    COUNT(*) as finding_count,
                    COUNT(DISTINCT finding_type) as unique_finding_types,
                    MAX(severity) as max_severity,
                    AVG(confidence_score) as avg_confidence,
                    MIN(timestamp) as first_seen,
                    MAX(timestamp) as last_seen
                FROM hunt_findings
                WHERE source_ip IS NOT NULL
                    AND status != 'false_positive'
                GROUP BY source_ip
                HAVING finding_count > 1
                ORDER BY finding_count DESC
            """,
            
            # Hunt timeline
            'vw_hunt_timeline': """
                CREATE OR REPLACE VIEW vw_hunt_timeline AS
                SELECT 
                    DATE(timestamp) as date,
                    HOUR(timestamp) as hour,
                    hunt_name,
                    COUNT(*) as finding_count,
                    COUNT(DISTINCT source_ip) as unique_sources
                FROM hunt_findings
                WHERE status != 'false_positive'
                GROUP BY DATE(timestamp), HOUR(timestamp), hunt_name
            """,
            
            # Investigation status
            'vw_investigation_status': """
                CREATE OR REPLACE VIEW vw_investigation_status AS
                SELECT 
                    status,
                    severity,
                    COUNT(*) as count,
                    MIN(created_at) as oldest_finding,
                    MAX(created_at) as newest_finding
                FROM hunt_findings
                GROUP BY status, severity
            """,
            
            # Hunt effectiveness
            'vw_hunt_effectiveness': """
                CREATE OR REPLACE VIEW vw_hunt_effectiveness AS
                SELECT 
                    hunt_name,
                    COUNT(*) as total_findings,
                    SUM(CASE WHEN status = 'confirmed' THEN 1 ELSE 0 END) as confirmed_threats,
                    SUM(CASE WHEN status = 'false_positive' THEN 1 ELSE 0 END) as false_positives,
                    ROUND(
                        SUM(CASE WHEN status = 'confirmed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*),
                        2
                    ) as true_positive_rate
                FROM hunt_findings
                GROUP BY hunt_name
            """
        }
        
        # Create each view
        with self.engine.begin() as conn:
            for view_name, view_sql in views.items():
                try:
                    conn.execute(text(view_sql))
                    print(f"  ✓ Created view: {view_name}")
                except Exception as e:
                    print(f"  ✗ Failed to create {view_name}: {e}")
        
        print(f"✓ Created {len(views)} dashboard views")


    def export_summary_statistics(self, days=30):
        """Export aggregated statistics for dashboards"""
        
        print(f"\nExporting summary statistics (last {days} days)...")
        
        cutoff_date = datetime.now() - timedelta(days=days)
        
        queries = {
            'hunt_summary': f"""
                SELECT 
                    hunt_name,
                    COUNT(DISTINCT execution_id) as total_executions,
                    SUM(findings_count) as total_findings,
                    AVG(TIMESTAMPDIFF(SECOND, execution_start, execution_end)) as avg_time_seconds,
                    MAX(execution_start) as last_run
                FROM hunt_executions
                WHERE execution_start >= '{cutoff_date.strftime('%Y-%m-%d')}'
                GROUP BY hunt_name
            """,
            
            'severity_trends': f"""
                SELECT 
                    DATE(timestamp) as date,
                    severity,
                    COUNT(*) as count
                FROM hunt_findings
                WHERE timestamp >= '{cutoff_date.strftime('%Y-%m-%d')}'
                    AND status != 'false_positive'
                GROUP BY DATE(timestamp), severity
                ORDER BY date, severity
            """,
            
            'finding_types': f"""
                SELECT 
                    finding_type,
                    COUNT(*) as count,
                    AVG(confidence_score) as avg_confidence
                FROM hunt_findings
                WHERE timestamp >= '{cutoff_date.strftime('%Y-%m-%d')}'
                    AND status != 'false_positive'
                GROUP BY finding_type
                ORDER BY count DESC
            """,
            
            'top_indicators': f"""
                SELECT 
                    source_ip,
                    destination_ip,
                    COUNT(*) as occurrence_count,
                    GROUP_CONCAT(DISTINCT finding_type) as finding_types,
                    MAX(severity) as max_severity
                FROM hunt_findings
                WHERE timestamp >= '{cutoff_date.strftime('%Y-%m-%d')}'
                    AND status != 'false_positive'
                GROUP BY source_ip, destination_ip
                HAVING occurrence_count > 5
                ORDER BY occurrence_count DESC
                LIMIT 100
            """
        }
        
        results = {}
        for name, query in queries.items():
            try:
                df = pd.read_sql(query, self.engine)
                
                # Export to dedicated table
                table_name = f"dashboard_{name}"
                df.to_sql(table_name, self.engine, if_exists='replace', index=False)
                
                results[name] = len(df)
                print(f"  ✓ Exported {name}: {len(df)} rows → {table_name}")
                
            except Exception as e:
                print(f"  ✗ Failed to export {name}: {e}")
        
        return results

    def create_ioc_feed(self):
        """Create IOC feed table for threat intelligence"""
        
        print("\nCreating IOC feed...")
        
        query = """
            SELECT DISTINCT
                source_ip as ioc_value,
                'ip' as ioc_type,
                MIN(timestamp) as first_seen,
                MAX(timestamp) as last_seen,
                COUNT(*) as times_seen,
                MAX(severity) as threat_level,
                GROUP_CONCAT(DISTINCT finding_type) as associated_hunts,
                MAX(confidence_score) as max_confidence
            FROM hunt_findings
            WHERE source_ip IS NOT NULL
                AND status IN ('confirmed', 'investigating')
            GROUP BY source_ip
            
            UNION ALL
            
            SELECT DISTINCT
                destination_ip as ioc_value,
                'ip' as ioc_type,
                MIN(timestamp) as first_seen,
                MAX(timestamp) as last_seen,
                COUNT(*) as times_seen,
                MAX(severity) as threat_level,
                GROUP_CONCAT(DISTINCT finding_type) as associated_hunts,
                MAX(confidence_score) as max_confidence
            FROM hunt_findings
            WHERE destination_ip IS NOT NULL
                AND status IN ('confirmed', 'investigating')
            GROUP BY destination_ip
            
            UNION ALL
            
            SELECT DISTINCT
                destination_hostname as ioc_value,
                'domain' as ioc_type,
                MIN(timestamp) as first_seen,
                MAX(timestamp) as last_seen,
                COUNT(*) as times_seen,
                MAX(severity) as threat_level,
                GROUP_CONCAT(DISTINCT finding_type) as associated_hunts,
                MAX(confidence_score) as max_confidence
            FROM hunt_findings
            WHERE destination_hostname IS NOT NULL
                AND status IN ('confirmed', 'investigating')
            GROUP BY destination_hostname
        """
        
        try:
            df = pd.read_sql(query, self.engine)
            df.to_sql('dashboard_ioc_feed', self.engine, if_exists='replace', index=False)
            print(f"  ✓ Created IOC feed: {len(df)} indicators")
            return len(df)
        except Exception as e:
            print(f"  ✗ Failed to create IOC feed: {e}")
            return 0
    
    def update_statistics_table(self):
        """Update the hunt_statistics aggregation table"""
        
        print("\nUpdating statistics table...")
        
        query = """
            INSERT INTO hunt_statistics 
            (hunt_name, execution_date, total_findings, critical_findings, high_findings, 
             medium_findings, low_findings, false_positives, confirmed_threats, 
             avg_confidence_score, records_analyzed, execution_time_seconds)
            SELECT 
                he.hunt_name,
                DATE(he.execution_start) as execution_date,
                COUNT(hf.finding_id) as total_findings,
                SUM(CASE WHEN hf.severity = 'critical' THEN 1 ELSE 0 END) as critical_findings,
                SUM(CASE WHEN hf.severity = 'high' THEN 1 ELSE 0 END) as high_findings,
                SUM(CASE WHEN hf.severity = 'medium' THEN 1 ELSE 0 END) as medium_findings,
                SUM(CASE WHEN hf.severity = 'low' THEN 1 ELSE 0 END) as low_findings,
                SUM(CASE WHEN hf.status = 'false_positive' THEN 1 ELSE 0 END) as false_positives,
                SUM(CASE WHEN hf.status = 'confirmed' THEN 1 ELSE 0 END) as confirmed_threats,
                AVG(hf.confidence_score) as avg_confidence_score,
                he.records_analyzed,
                TIMESTAMPDIFF(SECOND, he.execution_start, he.execution_end) as execution_time_seconds
            FROM hunt_executions he
            LEFT JOIN hunt_findings hf ON he.execution_id = hf.execution_id
            WHERE he.status = 'completed'
                AND DATE(he.execution_start) >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
            GROUP BY he.hunt_name, DATE(he.execution_start), he.records_analyzed, 
                     he.execution_start, he.execution_end
            ON DUPLICATE KEY UPDATE
                total_findings = VALUES(total_findings),
                critical_findings = VALUES(critical_findings),
                high_findings = VALUES(high_findings),
                medium_findings = VALUES(medium_findings),
                low_findings = VALUES(low_findings),
                false_positives = VALUES(false_positives),
                confirmed_threats = VALUES(confirmed_threats),
                avg_confidence_score = VALUES(avg_confidence_score),
                records_analyzed = VALUES(records_analyzed),
                execution_time_seconds = VALUES(execution_time_seconds)
        """
        
        try:
            with self.engine.begin() as conn:
                result = conn.execute(text(query))
                print(f"  ✓ Updated statistics table ({result.rowcount} rows)")
                return result.rowcount
        except Exception as e:
            print(f"  ✗ Failed to update statistics: {e}")
            return 0
    
    def run_full_export(self, days=30):
        """Run complete export process"""
        
        print("=" * 70)
        print("EXPORTING DATA FOR SUPERSET DASHBOARDS")
        print("=" * 70)
        
        # Create views
        self.create_dashboard_views()
        
        # Export summaries
        summary_results = self.export_summary_statistics(days=days)
        
        # Create IOC feed
        ioc_count = self.create_ioc_feed()
        
        # Update statistics
        stats_count = self.update_statistics_table()
        
        print("\n" + "=" * 70)
        print("EXPORT COMPLETE")
        print("=" * 70)
        print(f"  Dashboard views: 7 created")
        print(f"  Summary tables: {len(summary_results)} exported")
        print(f"  IOC indicators: {ioc_count}")
        print(f"  Statistics rows: {stats_count}")
        print("=" * 70)
        
        return {
            'views': 7,
            'summaries': summary_results,
            'iocs': ioc_count,
            'statistics': stats_count
        }


def main():
    """Run the export"""
    exporter = SupersetExporter()
    results = exporter.run_full_export(days=30)
    
    print("\n✓ Data ready for Superset dashboards!")
    print("\nNext steps:")
    print("  1. Access Superset: http://11.11.10.103:8088")
    print("  2. Login with: admin/admin")
    print("  3. Add database connection to hunt_results")
    print("  4. Create charts from the dashboard views")


if __name__ == "__main__":
    main()