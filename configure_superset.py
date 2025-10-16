# configure_superset.py
import requests
import json
from requests.auth import HTTPBasicAuth

class SupersetConfigurator:
    """Configure Superset dashboards programmatically"""
    
    def __init__(self, superset_url="http://172.25.41.34:8088"):
        self.base_url = superset_url
        self.username = "admin"
        self.password = "admin"
        self.session = requests.Session()
        self.csrf_token = None
        self.access_token = None

    def login(self):
        """Authenticate with Superset"""
        
        login_url = f"{self.base_url}/api/v1/security/login"
        payload = {
            "username": self.username,
            "password": self.password,
            "provider": "db",
            "refresh": True
        }


        print("Logging into Superset...")
        
        try:
            response = self.session.post(login_url, json=payload)
            response.raise_for_status()
            
            data = response.json()
            token = data.get('access_token')
            
            if not token:
                raise Exception(f"No access token in response: {data}")
            
            self.session.headers.update({
                'Authorization': f'Bearer {token}',
                'Content-Type': 'application/json'
            })
            
            print("Login successful!")
            
            # Get CSRF token
            csrf_response = self.session.get(f"{self.base_url}/api/v1/security/csrf_token/")
            csrf_token = csrf_response.json()['result']
            self.session.headers.update({'X-CSRFToken': csrf_token})
            
        except requests.exceptions.HTTPError as e:
            print(f"\n❌ Login failed!")
            print(f"Status code: {e.response.status_code}")
            print(f"Response: {e.response.text}")
            raise Exception(f"Authentication failed. Please check your credentials.")
    
    
    def add_database(self):
        """Add MySQL hunt_results database"""

        print("\nAdding database connection...")

        # Use structured parameters format (same as Superset UI)
        # This is required for proper validation
        database_config = {
            "database_name": "Hunt Results",
            "engine": "mysql",
            "driver": "pymysql",
            "configuration_method": "dynamic_form",
            "parameters": {
                "host": "172.25.41.34",
                "port": 3306,
                "database": "hunt_results",
                "username": "hunt_admin",
                "password": "admin"
            },
            "expose_in_sqllab": True,
            "allow_run_async": True,
            "allow_ctas": False,
            "allow_cvas": False,
            "allow_dml": False
        }

        url = f"{self.base_url}/api/v1/database/"
        response = self.session.post(url, json=database_config)

        if response.status_code in [200, 201]:
            data = response.json()
            db_id = data.get('id')
            print(f"  ✓ Database added (ID: {db_id})")
            return db_id
        elif response.status_code == 422:
            # 422 can mean connection failed OR database already exists
            response_data = response.json()
            error_message = response_data.get('message', '')

            # Check if it's a connection error
            if isinstance(error_message, str) and ('Connection failed' in error_message or 'connection settings' in error_message):
                print(f"  ✗ Connection failed: {error_message}")
                print("\n     Troubleshooting steps:")
                print("       1. Verify MySQL is running on 172.25.41.34:3306")
                print("       2. Verify database 'hunt_results' exists")
                print("       3. Verify user 'hunt_admin' has access")
                print("       4. Verify password is correct")
                return None

            # Check if it's a duplicate database error
            if isinstance(error_message, dict) or 'already exists' in str(error_message).lower():
                print(f"  ℹ Database already exists, searching for it...")
                # Try to find the existing database
                response = self.session.get(url + '?q=(page_size:100)')
                if response.status_code == 200:
                    databases = response.json().get('result', [])
                    for db in databases:
                        if db.get('database_name') == 'Hunt Results':
                            print(f"  ✓ Found existing database (ID: {db['id']})")
                            return db['id']
                    print("  ⚠ Database exists but couldn't find it in list")
                return None

            # Unknown 422 error
            print(f"  ✗ Error adding database: {error_message}")
            return None
        else:
            print(f"  ✗ Failed to add database: {response.status_code}")
            print(f"     {response.text}")
            return None

    def test_connection(self, database_id):
        """Verify database was created successfully"""

        print("\nVerifying database...")

        if not database_id:
            print("  ⚠ No database ID provided, skipping verification")
            return False

        try:
            # Get database info to verify it was created successfully
            url = f"{self.base_url}/api/v1/database/{database_id}"
            response = self.session.get(url)

            if response.status_code == 200:
                data = response.json()
                result = data.get('result', {})
                db_name = result.get('database_name', 'Unknown')
                backend = result.get('backend', 'Unknown')
                expose_sqllab = result.get('expose_in_sqllab', False)

                print(f"  ✓ Database configured successfully")
                print(f"    Name: {db_name}")
                print(f"    Backend: {backend}")
                print(f"    SQL Lab: {'Enabled' if expose_sqllab else 'Disabled'}")
                return True
            else:
                print(f"  ✗ Failed to verify database: {response.status_code}")
                return False
        except Exception as e:
            print(f"  ✗ Verification error: {e}")
            return False

def main():
    """Configure Superset"""

    print("=" * 70)
    print("SUPERSET CONFIGURATION")
    print("=" * 70)

    configurator = SupersetConfigurator()

    configurator.login()

    database_id = configurator.add_database()

    if database_id:
        configurator.test_connection(database_id)

    print("\n" + "=" * 70)
    print("CONFIGURATION SUMMARY")
    print("=" * 70)

    print("\nSuperset is now configured with hunt_results database.")

    print("\n📊 Available Views for Charting:")
    views = [
        "vw_daily_findings",
        "vw_hunt_performance",
        "vw_severity_distribution",
        "vw_top_sources",
        "vw_hunt_timeline",
        "vw_investigation_status",
        "vw_hunt_effectiveness"
    ]
    for view in views:
        print(f"   - {view}")

    print("\n📈 Dashboard Tables:")
    tables = [
        "dashboard_hunt_summary",
        "dashboard_severity_trends",
        "dashboard_finding_types",
        "dashboard_top_indicators",
        "dashboard_ioc_feed"
    ]
    for table in tables:
        print(f"   - {table}")

    print("\n🌐 Access Superset:")
    print("   URL: http://172.25.41.34:8088")
    print("   Username: admin")
    print("   Password: admin")

    print("\n✅ Next Steps:")
    print("   1. Log into Superset web interface")
    print("   2. Navigate to Data → Datasets")
    print("   3. Add datasets from the views listed above")
    print("   4. Create charts and dashboards from these datasets")
    print("=" * 70)


if __name__ == "__main__":
    main()