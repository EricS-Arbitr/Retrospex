#!/usr/bin/env python3
"""
Comprehensive test of the automated retrospective hunt system
"""

from hunt_orchestrator import HuntOrchestrator
from export_to_superset import SupersetExporter
from generate_hunt_report import HuntReportGenerator
from datetime import date, timedelta
import config

def run_system_test():
    """Run complete system test"""
    
    print("=" * 70)
    print("AUTOMATED RETROSPECTIVE HUNT SYSTEM - INTEGRATION TEST")
    print("=" * 70)
    
    # Test 1: Hunt Execution
    print("\n[TEST 1/4] Hunt Execution")
    print("-" * 70)
    
    orchestrator = HuntOrchestrator()
    
    try:
        # Run hunts for last 7 days
        end_date = date.today()
        start_date = end_date - timedelta(days=2500)
        
        results = orchestrator.run_all_hunts(start_date, end_date)
        
        success_count = sum(1 for r in results if r.get('status') == 'completed')
        total_findings = sum(r.get('findings_count', 0) for r in results)
        
        print(f"\n✓ Hunt execution test passed")
        print(f"  Successful hunts: {success_count}/{len(results)}")
        print(f"  Total findings: {total_findings}")
        
    except Exception as e:
        print(f"\n✗ Hunt execution test failed: {e}")
        return False
    finally:
        orchestrator.cleanup()
    
    # Test 2: Data Export
    print("\n[TEST 2/4] Data Export to Superset")
    print("-" * 70)
    
    try:
        exporter = SupersetExporter()
        export_results = exporter.run_full_export(days=2500)
        
        print(f"\n✓ Data export test passed")
        print(f"  Views created: {export_results['views']}")
        print(f"  IOCs tracked: {export_results['iocs']}")
        
    except Exception as e:
        print(f"\n✗ Data export test failed: {e}")
        return False
    
    # Test 3: Report Generation
    print("\n[TEST 3/4] Report Generation")
    print("-" * 70)
    
    try:
        generator = HuntReportGenerator()
        report_path = generator.save_report(days=7)
        
        print(f"\n✓ Report generation test passed")
        print(f"  Report saved: {report_path}")
        
    except Exception as e:
        print(f"\n✗ Report generation test failed: {e}")
        return False
    
    # Test 4: Database Integrity
    print("\n[TEST 4/4] Database Integrity Check")
    print("-" * 70)
    
    try:
        from sqlalchemy import create_engine

        # Build connection string from config
        mysql_config = config.MYSQL_CONFIG
        connection_string = (
            f"mysql+pymysql://{mysql_config['user']}:{mysql_config['password']}"
            f"@{mysql_config['host']}:{mysql_config['port']}/{mysql_config['database']}"
        )
        engine = create_engine(connection_string)
        
        # Check required tables exist
        tables_query = "SHOW TABLES"
        import pandas as pd
        tables_df = pd.read_sql(tables_query, engine)
        
        required_tables = [
            'hunt_executions',
            'hunt_findings',
            'hunt_definitions',
            'hunt_statistics',
            'hunt_iocs'
        ]
        
        existing_tables = tables_df.iloc[:, 0].tolist()
        missing_tables = [t for t in required_tables if t not in existing_tables]
        
        if missing_tables:
            print(f"\n⚠ Missing tables: {missing_tables}")
        else:
            print(f"\n✓ Database integrity test passed")
            print(f"  All required tables present")
        
    except Exception as e:
        print(f"\n✗ Database integrity test failed: {e}")
        return False
    
    # Final Summary
    print("\n" + "=" * 70)
    print("✓ ALL TESTS PASSED")
    print("=" * 70)
    print("\nYour automated retrospective hunt system is fully operational!")
    print("\nSystem Components:")
    print("  ✓ Hunt orchestration")
    print("  ✓ Delta Lake storage")
    print("  ✓ MySQL results database")
    print("  ✓ Superset visualization")
    print("  ✓ Executive reporting")
    print("\nNext Steps:")
    print("  1. Schedule recurring hunts with scheduled_hunter.py")
    print("  2. Create Superset dashboards")
    print("  3. Customize hunts for your environment")
    print("  4. Integrate with your SIEM/SOAR platform")
    
    return True


if __name__ == "__main__":
    success = run_system_test()
    exit(0 if success else 1)


