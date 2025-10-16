# generate_hunt_report.py
from sqlalchemy import create_engine
import pandas as pd
from datetime import datetime, timedelta
from jinja2 import Template
import json

class HuntReportGenerator:
    """Generate executive reports from hunt results"""
    
    def __init__(self, mysql_config=None):
        if mysql_config is None:
            mysql_config = {
                'host': '172.25.41.34',
                'port': 3306,
                'database': 'hunt_results',
                'user': 'hunt_admin',
                'password': 'admin'
            }
        
        self.mysql_config = mysql_config
        self.engine = self._create_engine()
    
    def _create_engine(self):
        """Create SQLAlchemy engine"""
        connection_string = (
            f"mysql+pymysql://{self.mysql_config['user']}:{self.mysql_config['password']}"
            f"@{self.mysql_config['host']}:{self.mysql_config['port']}/{self.mysql_config['database']}"
        )
        return create_engine(connection_string)
    
    def generate_executive_summary(self, days=7):
        """Generate executive summary report"""
        
        print(f"Generating executive summary for last {days} days...")
        
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # Get overall statistics
        stats_query = f"""
            SELECT 
                COUNT(DISTINCT execution_id) as total_hunts,
                SUM(findings_count) as total_findings,
                SUM(CASE WHEN status = 'completed' THEN 1 ELSE 0 END) as successful_hunts,
                SUM(records_analyzed) as total_records_analyzed
            FROM hunt_executions
            WHERE execution_start >= '{cutoff_date.strftime('%Y-%m-%d')}'
        """

        stats = pd.read_sql(stats_query, self.engine).iloc[0]
        
        # Get severity breakdown
        severity_query = f"""
            SELECT 
                severity,
                COUNT(*) as count
            FROM hunt_findings
            WHERE timestamp >= '{cutoff_date.strftime('%Y-%m-%d')}'
                AND status != 'false_positive'
            GROUP BY severity
        """
        
        severity_df = pd.read_sql(severity_query, self.engine)
        
        # Get top threats
        threats_query = f"""
            SELECT 
                source_ip,
                COUNT(*) as finding_count,
                MAX(severity) as max_severity,
                GROUP_CONCAT(DISTINCT finding_type) as threat_types
            FROM hunt_findings
            WHERE timestamp >= '{cutoff_date.strftime('%Y-%m-%d')}'
                AND status IN ('confirmed', 'investigating')
            GROUP BY source_ip
            ORDER BY finding_count DESC
            LIMIT 10
        """
        
        threats_df = pd.read_sql(threats_query, self.engine)
        
        # Get hunt performance
        performance_query = f"""
            SELECT 
                hunt_name,
                COUNT(*) as execution_count,
                SUM(findings_count) as total_findings,
                AVG(TIMESTAMPDIFF(SECOND, execution_start, execution_end)) as avg_time
            FROM hunt_executions
            WHERE execution_start >= '{cutoff_date.strftime('%Y-%m-%d')}'
                AND status = 'completed'
            GROUP BY hunt_name
        """
        
        performance_df = pd.read_sql(performance_query, self.engine)
        
        # Generate report
        report = {
            'report_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'period_days': days,
            'period_start': cutoff_date.strftime('%Y-%m-%d'),
            'period_end': datetime.now().strftime('%Y-%m-%d'),
            'summary': {
                'total_hunts': int(stats['total_hunts']),
                'total_findings': int(stats['total_findings']),
                'successful_hunts': int(stats['successful_hunts']),
                'records_analyzed': int(stats['total_records_analyzed'])
            },
            'severity_breakdown': severity_df.to_dict('records'),
            'top_threats': threats_df.to_dict('records'),
            'hunt_performance': performance_df.to_dict('records')
        }
        
        return report
    
    def generate_html_report(self, report_data):
        """Generate HTML report from data"""
        
        template = Template("""
<!DOCTYPE html>
<html>
<head>
    <title>Hunt Report - {{ report_date }}</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 40px; }
        h1 { color: #2c3e50; }
        h2 { color: #34495e; margin-top: 30px; }
        table { border-collapse: collapse; width: 100%; margin: 20px 0; }
        th, td { border: 1px solid #ddd; padding: 12px; text-align: left; }
        th { background-color: #3498db; color: white; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        .summary-box { 
            background: #ecf0f1; 
            padding: 20px; 
            border-radius: 5px; 
            margin: 20px 0; 
        }
        .stat { 
            display: inline-block; 
            margin: 10px 20px; 
            font-size: 18px; 
        }
        .stat-value { 
            font-size: 32px; 
            font-weight: bold; 
            color: #2980b9; 
        }
        .critical { color: #e74c3c; font-weight: bold; }
        .high { color: #e67e22; font-weight: bold; }
        .medium { color: #f39c12; }
        .low { color: #27ae60; }
    </style>
</head>
<body>
    <h1>Retrospective Hunt Executive Summary</h1>
    <p><strong>Report Generated:</strong> {{ report_date }}</p>
    <p><strong>Period:</strong> {{ period_start }} to {{ period_end }} ({{ period_days }} days)</p>
    
    <div class="summary-box">
        <h2>Executive Summary</h2>
        <div class="stat">
            <div>Total Hunts</div>
            <div class="stat-value">{{ summary.total_hunts }}</div>
        </div>
        <div class="stat">
            <div>Total Findings</div>
            <div class="stat-value">{{ summary.total_findings }}</div>
        </div>
        <div class="stat">
            <div>Records Analyzed</div>
            <div class="stat-value">{{ "%.1f" | format(summary.records_analyzed / 1000000) }}M</div>
        </div>
        <div class="stat">
            <div>Success Rate</div>
            <div class="stat-value">{{ "%.1f" | format(summary.successful_hunts / summary.total_hunts * 100) }}%</div>
        </div>
    </div>
    
    <h2>Findings by Severity</h2>
    <table>
        <tr>
            <th>Severity</th>
            <th>Count</th>
        </tr>
        {% for row in severity_breakdown %}
        <tr>
            <td class="{{ row.severity }}">{{ row.severity|upper }}</td>
            <td>{{ row.count }}</td>
        </tr>
        {% endfor %}
    </table>
    
    <h2>Top 10 Threat Sources</h2>
    <table>
        <tr>
            <th>Source IP</th>
            <th>Findings</th>
            <th>Max Severity</th>
            <th>Threat Types</th>
        </tr>
        {% for row in top_threats %}
        <tr>
            <td>{{ row.source_ip }}</td>
            <td>{{ row.finding_count }}</td>
            <td class="{{ row.max_severity }}">{{ row.max_severity|upper }}</td>
            <td>{{ row.threat_types }}</td>
        </tr>
        {% endfor %}
    </table>
    
    <h2>Hunt Performance</h2>
    <table>
        <tr>
            <th>Hunt Name</th>
            <th>Executions</th>
            <th>Total Findings</th>
            <th>Avg Time (seconds)</th>
        </tr>
        {% for row in hunt_performance %}
        <tr>
            <td>{{ row.hunt_name }}</td>
            <td>{{ row.execution_count }}</td>
            <td>{{ row.total_findings }}</td>
            <td>{{ "%.1f" | format(row.avg_time) }}</td>
        </tr>
        {% endfor %}
    </table>
</body>
</html>
        """)
        
        html = template.render(**report_data)
        return html
    
    def save_report(self, days=7, output_path=None):
        """Generate and save report"""
        
        if output_path is None:
            output_path = f"/home/eric_s/dev_work/github.com/EricS-Arbitr/retro-hunt-lab/end_to_end/hunt_results/hunt_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html"
        
        # Generate report data
        report_data = self.generate_executive_summary(days=days)
        
        # Generate HTML
        html = self.generate_html_report(report_data)
        
        # Save to file
        with open(output_path, 'w') as f:
            f.write(html)
        
        print(f"✓ Report saved to: {output_path}")
        return output_path


def main():
    """Generate report"""
    generator = HuntReportGenerator()
    report_path = generator.save_report(days=7)
    
    print(f"\n✓ Executive report generated")
    print(f"  Open in browser: file://{report_path}")


if __name__ == "__main__":
    main()
