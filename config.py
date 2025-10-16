"""
Centralized configuration for Retrospex project.
All file paths are relative to the project root.
Database credentials are loaded from environment variables.
"""
from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Project paths (relative to project root)
PROJECT_ROOT = Path(__file__).parent.resolve()
HUNT_LIBRARY_DIR = PROJECT_ROOT / "hunt_library"
HUNT_RESULTS_DIR = PROJECT_ROOT / "hunt_results"
LOGS_DIR = PROJECT_ROOT / "logs"

# Data paths (assuming data2 is in parent directory structure)
# Users can override this with DATA_BASE environment variable
DATA_BASE_DEFAULT = PROJECT_ROOT.parent / "retro-hunt-lab" / "data2"
DATA_BASE = Path(os.getenv('DATA_BASE', str(DATA_BASE_DEFAULT)))

# Specific data source paths
ZEEK_CONN_LOGS = DATA_BASE / "zeek_conn_logs"
ZEEK_DNS_LOGS = DATA_BASE / "zeek_dns_logs"
WIN_SECURITY_LOGS = DATA_BASE / "win_security"

# Database configuration
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', '172.25.41.34'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'database': os.getenv('MYSQL_DATABASE', 'hunt_results'),
    'user': os.getenv('MYSQL_USER', 'root'),
    'password': os.getenv('MYSQL_PASSWORD', '')
}

# Superset configuration
SUPERSET_BASE_URL = os.getenv('SUPERSET_BASE_URL', 'http://localhost:8088')
SUPERSET_USERNAME = os.getenv('SUPERSET_USERNAME', 'admin')
SUPERSET_PASSWORD = os.getenv('SUPERSET_PASSWORD', '')

# Ensure required directories exist
HUNT_RESULTS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
