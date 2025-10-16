# Retrospex

A retrospective threat hunting framework built on Apache Spark and Delta Lake for detecting security threats in historical network data.

## Quick Start

```bash
# 1. Clone and setup
git clone <repository-url>
cd Retrospex
python3 -m venv env
source env/bin/activate

# 2. Install dependencies
pip install -r requirements.txt

# 3. Configure environment
cp .env.example .env
# Edit .env with your MySQL credentials and data paths

# 4. Setup database
mysql -u root -p < setup_hunt_database.sql

# 5. Run tests
python test_hunt_system.py

# 6. Execute hunts
python hunt_cli.py --all --lookback 7
```

## Overview

Retrospex is a scalable threat hunting platform designed to analyze large volumes of historical security logs (particularly Zeek network logs) to identify indicators of compromise and malicious activity patterns. The system uses PySpark for distributed processing and stores results in MySQL for visualization via Apache Superset dashboards.

## Features

- **Modular Hunt Library**: Pre-built detection hunts for common threat patterns:
  - C2 Beaconing Detection
  - DNS Tunneling Detection
  - Lateral Movement Detection

- **Scalable Architecture**: Built on Apache Spark with Delta Lake for efficient processing of large datasets

- **Result Deduplication**: Intelligent IOC tracking to prevent duplicate findings

- **MySQL Integration**: Stores hunt results and execution metadata for analysis

- **Dashboard Integration**: Export views and aggregations for Apache Superset visualization

- **Flexible Scheduling**: Built-in scheduler for automated hunt execution

- **CLI Interface**: Command-line tool for executing hunts manually

- **Report Generation**: Automated HTML report generation from hunt results

## Architecture

```
Retrospex/
├── config.py                  # Centralized configuration
├── .env                       # Environment variables (not in git)
├── .env.example              # Template for environment setup
├── hunt_library/              # Detection hunt modules
│   ├── hunt_base.py          # Base class for all hunts
│   ├── hunt_c2_beaconing.py  # C2 beaconing detection
│   ├── hunt_dns_tunneling.py # DNS tunneling detection
│   ├── hunt_lateral_movement.py # Lateral movement detection
│   └── hunt_deduplication.py # Deduplication logic
├── hunt_orchestrator.py       # Hunt execution coordinator
├── hunt_cli.py               # Command-line interface
├── scheduled_hunter.py       # Automated scheduling
├── export_to_superset.py     # Dashboard data export
├── configure_superset.py     # Superset configuration
├── generate_hunt_report.py   # HTML report generation
├── setup_environment.py      # Environment setup script
├── verify_data.py           # Data validation
├── test_hunt_system.py      # System testing
├── hunt_results/             # Generated reports (auto-created)
└── logs/                     # Application logs (auto-created)
```

## Prerequisites

- Python 3.8 or higher
- Java 8 or 11 (required for PySpark)
- MySQL 5.7+ (for storing hunt results)
- Apache Superset (optional, for dashboards)
- Access to Zeek network logs in Delta Lake format

## Installation

1. **Clone the repository:**
   ```bash
   git clone <repository-url>
   cd Retrospex
   ```

2. **Create and activate a virtual environment (recommended):**
   ```bash
   python3 -m venv env
   source env/bin/activate  # On Windows: env\Scripts\activate
   ```

3. **Install Python dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

4. **Set up MySQL database:**
   ```bash
   mysql -u root -p < setup_hunt_database.sql
   ```

5. **Configure environment variables:**
   ```bash
   cp .env.example .env
   # Edit .env with your actual credentials and paths
   ```

6. **Verify installation:**
   ```bash
   python test_hunt_system.py
   ```

## Configuration

### Environment Variables

All configuration is managed through the `.env` file for security and portability. Copy the example template and customize:

```bash
cp .env.example .env
```

Edit `.env` with your settings:

```bash
# MySQL Database Configuration
MYSQL_HOST=your_mysql_host
MYSQL_PORT=3306
MYSQL_DATABASE=hunt_results
MYSQL_USER=your_username
MYSQL_PASSWORD=your_secure_password

# Data Paths (optional - defaults to ../retro-hunt-lab/data2)
DATA_BASE=/path/to/your/data

# Apache Superset Configuration
SUPERSET_BASE_URL=http://localhost:8088
SUPERSET_USERNAME=admin
SUPERSET_PASSWORD=your_superset_password
```

**Important**: Never commit `.env` to version control. It's already in `.gitignore`.

### Centralized Configuration

The `config.py` module manages all paths and settings:

- **Project-relative paths** - Works anywhere without modification
- **Environment variable support** - Secure credential management
- **Auto-directory creation** - Creates `hunt_results/` and `logs/` automatically
- **Cross-platform compatibility** - Uses `pathlib` for Windows/Linux/Mac

### Data Paths

The system expects Zeek logs in Delta Lake format:
- `DATA_BASE/zeek_conn_logs` - Network connection logs
- `DATA_BASE/zeek_dns_logs` - DNS query logs
- `DATA_BASE/win_security` - Windows security event logs

Override the default data location in `.env`:
```bash
DATA_BASE=/your/custom/data/path
```

## Usage

### Command Line Interface

The CLI provides flexible hunt execution options:

**List available hunts:**
```bash
python hunt_cli.py --list
```

**Run a single hunt with lookback period:**
```bash
python hunt_cli.py --hunt c2_beaconing --lookback 30
```

**Run a hunt for a specific date range:**
```bash
python hunt_cli.py --hunt dns_tunneling --start 2021-01-01 --end 2021-01-31
```

**Run all hunts:**
```bash
python hunt_cli.py --all --lookback 7
```

**Run with custom parameters:**
```bash
python hunt_cli.py --hunt c2_beaconing --lookback 30 \
  --param min_connections=15 \
  --param jitter_threshold=300
```

### Programmatic Usage

```python
from hunt_orchestrator import HuntOrchestrator

# Initialize orchestrator
orchestrator = HuntOrchestrator()

# Run a single hunt
results = orchestrator.run_hunt(
    'c2_beaconing',
    start_date='2021-01-01',
    end_date='2021-01-31',
    min_connections=10,
    jitter_threshold=300
)

# Run all hunts
all_results = orchestrator.run_all_hunts(
    start_date='2021-01-01',
    end_date='2021-01-31'
)

# Run with lookback period
lookback_results = orchestrator.run_lookback_hunt(
    'dns_tunneling',
    lookback_days=90
)

# Clean up
orchestrator.cleanup()
```

### Scheduled Execution

The scheduled hunter runs hunts automatically:

```python
from scheduled_hunter import ScheduledHunter

scheduler = ScheduledHunter()
scheduler.start_scheduler()  # Runs as daemon
```

Default schedule:
- Daily hunts: 02:00 (previous day)
- Weekly hunts: Monday 03:00 (previous week)
- C2 hunts: Every 6 hours (last 3 days)

## Hunt Types

### 1. C2 Beaconing Detection

Identifies periodic network connections indicative of command-and-control (C2) communication.

**Detection Logic:**
- Regular connection intervals with low jitter
- Consistent byte patterns
- Sustained communication sessions

**Parameters:**
- `min_connections`: Minimum connection count (default: 10)
- `jitter_threshold`: Maximum time variance in seconds (default: 300)

**Example:**
```bash
python hunt_cli.py --hunt c2_beaconing --lookback 7 \
  --param min_connections=15 \
  --param jitter_threshold=200
```

### 2. DNS Tunneling Detection

Detects DNS-based data exfiltration and covert channels.

**Detection Logic:**
- Unusually long DNS queries
- High volume of queries to single domain
- Suspicious entropy in query strings

**Parameters:**
- `min_query_length`: Minimum query length to flag (default: 50)
- `min_query_count`: Minimum queries per domain (default: 100)

**Example:**
```bash
python hunt_cli.py --hunt dns_tunneling --lookback 30 \
  --param min_query_length=40 \
  --param min_query_count=50
```

### 3. Lateral Movement Detection

Identifies attempts to move laterally across network hosts.

**Detection Logic:**
- Multiple authentication attempts across hosts
- SMB/RDP connections from unusual sources
- Privilege escalation patterns

**Parameters:**
- `min_hosts`: Minimum number of target hosts (default: 3)
- `time_window`: Time window in hours (default: 24)

## Dashboard Integration

### Export Data to Superset

```bash
python export_to_superset.py
```

This creates:
- Dashboard views (vw_daily_findings, vw_hunt_performance, etc.)
- Summary statistics tables
- IOC feed for threat intelligence

### Configure Superset

```bash
python configure_superset.py
```

See `Sample_superset_dashboards.md` for dashboard configuration examples.

## Report Generation

Generate HTML reports from hunt results:

```python
from generate_hunt_report import HuntReportGenerator

generator = HuntReportGenerator()
report_path = generator.generate_report(days=7)
print(f"Report generated: {report_path}")
```

## Database Schema

### Key Tables

**hunt_executions**: Tracks hunt execution metadata
- `execution_id`: Unique execution identifier
- `hunt_name`: Name of executed hunt
- `execution_start`, `execution_end`: Timestamps
- `status`: Execution status (running, completed, failed)
- `records_analyzed`: Number of records processed
- `findings_count`: Number of findings discovered

**hunt_findings**: Stores detected threats
- `finding_id`: Unique finding identifier
- `execution_id`: Reference to hunt execution
- `hunt_name`: Hunt that generated finding
- `severity`: Severity level (critical, high, medium, low)
- `finding_type`: Type of threat detected
- `source_ip`, `destination_ip`: Network indicators
- `timestamp`: When threat occurred
- `confidence_score`: Detection confidence (0-100)
- `status`: Investigation status (new, investigating, confirmed, false_positive)
- `raw_data`: JSON with additional context

**hunt_ioc_tracker**: Tracks seen indicators
- Prevents duplicate findings
- Maintains IOC history

**hunt_statistics**: Aggregated metrics for dashboards

## Development

### Creating a New Hunt

1. Create a new file in `hunt_library/` (e.g., `hunt_new_detection.py`)
2. Inherit from `HuntBase`
3. Implement the `hunt_logic()` method
4. Register in `hunt_orchestrator.py`

Example:

```python
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
import config

from hunt_base import HuntBase
from pyspark.sql.functions import *
import json

class HuntNewDetection(HuntBase):
    def __init__(self, spark, mysql_config):
        super().__init__(spark, mysql_config)
        self.hunt_version = "1.0"
        self.description = "Detect new threat pattern"

    def hunt_logic(self, start_date, end_date, **kwargs):
        # Read data using config path
        delta_path = str(config.DATA_BASE / "your_data_source")
        df = self.read_delta_table(delta_path, start_date, end_date)

        # Apply detection logic
        suspicious = df.filter(...)

        # Format findings
        findings = []
        for row in suspicious.collect():
            finding = {
                'severity': 'high',
                'finding_type': 'new_detection',
                'source_ip': row['source'],
                'timestamp': row['timestamp'],
                'confidence_score': 85.0,
                'description': 'Description of finding',
                'raw_data': json.dumps({...})
            }
            findings.append(finding)

        return {
            'findings': findings,
            'records_analyzed': df.count()
        }
```

Then register in `hunt_orchestrator.py`:
```python
from hunt_new_detection import HuntNewDetection

self.hunts = {
    'c2_beaconing': HuntC2Beaconing(self.spark, self.mysql_config),
    'dns_tunneling': HuntDNSTunneling(self.spark, self.mysql_config),
    'lateral_movement': HuntLateralMovement(self.spark, self.mysql_config),
    'new_detection': HuntNewDetection(self.spark, self.mysql_config),  # Add here
}
```

### Testing

```bash
python test_hunt_system.py
```

## Troubleshooting

### PySpark/Delta Lake Issues

If you encounter Delta Lake compatibility errors:

1. Clear Maven cache:
   ```bash
   rm -rf ~/.ivy2/cache ~/.ivy2/jars
   ```

2. Verify versions:
   ```bash
   python -c "import pyspark; print(pyspark.__version__)"
   ```

3. Ensure PySpark 3.5.3 with Delta 3.1.0 (Scala 2.12)

### MySQL Connection Issues

- Verify MySQL is accessible from your host
- Check firewall rules
- Confirm credentials in configuration
- Ensure `hunt_results` database exists

### Data Path Issues

- Verify Delta Lake paths exist and are accessible
- Check that Delta tables have `_delta_log` directories
- Confirm date partitioning scheme matches code
- Ensure `DATA_BASE` environment variable is set correctly in `.env`
- Check file permissions on data directories

### Configuration Issues

If you encounter import errors or path issues:
- Ensure `.env` file exists (copy from `.env.example`)
- Verify `python-dotenv` is installed: `pip install python-dotenv`
- Check that all paths in `.env` are absolute paths
- Restart your Python session after changing `.env`

## Performance Tuning

### Spark Configuration

Adjust memory settings in `hunt_orchestrator.py`:

```python
.config("spark.driver.memory", "16g") \
.config("spark.executor.memory", "16g")
```

### Date Partitioning

For best performance, ensure Delta tables are partitioned by date:
- `year`, `month`, `day` columns
- Allows efficient time-range filtering

## Security Considerations

### Credential Management
- **Never commit `.env` to version control** - Already in `.gitignore`
- Use strong, unique passwords for MySQL and Superset
- Rotate credentials regularly, especially if previously hardcoded
- Consider using a secrets manager (AWS Secrets Manager, HashiCorp Vault) for production

### Access Controls
- Limit database user permissions to necessary operations only
- Use read-only Spark access to source data
- Implement network firewalls between components
- Review findings before actioning to prevent false positives

### Production Deployment
- Use environment variables instead of `.env` files in production
- Enable MySQL SSL/TLS connections
- Implement audit logging for hunt executions
- Restrict Superset dashboard access appropriately
- Regular security updates for all dependencies

## Contributing

This is a defensive security tool. Contributions should focus on:
- New threat detection hunts
- Performance improvements
- Documentation enhancements
- Bug fixes

## License

This project is for defensive security purposes only.

## Recent Updates

### Version 2.0 - Configuration Overhaul (October 2025)
- ✅ **Centralized configuration** via `config.py`
- ✅ **Environment variable support** for secure credential management
- ✅ **Project-relative paths** - portable across systems
- ✅ **SQLAlchemy 2.0+ compatibility** - updated to modern syntax
- ✅ **Auto-directory creation** - `hunt_results/` and `logs/` created automatically
- ✅ **Security improvements** - credentials no longer in source code
- ✅ **Cross-platform support** - uses `pathlib` for Windows/Linux/Mac

See `PATH_MIGRATION_SUMMARY.md` for detailed migration information.

## Support

For issues or questions, please review:
- `PATH_MIGRATION_SUMMARY.md` for configuration details
- `test_hunt_system.py` for end-to-end testing
- `.env.example` for configuration template
- `config.py` for available configuration options

## Acknowledgments

Built with:
- Apache Spark / PySpark
- Delta Lake
- Apache Superset
- MySQL
- Zeek Network Security Monitor
