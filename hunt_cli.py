


# hunt_cli.py

import argparse
from datetime import datetime, date, timedelta
from hunt_orchestrator import HuntOrchestrator
import sys

def parse_date(date_string):
    """Parse date from string"""
    if date_string == 'today':
        return date.today()
    elif date_string == 'yesterday':
        return date.today() - timedelta(days=1)
    else:
        return datetime.strptime(date_string, '%Y-%m-%d').date()


def main():
    parser = argparse.ArgumentParser(
        description='Retrospective Threat Hunting CLI',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run single hunt for last 30 days
  %(prog)s --hunt c2_beaconing --lookback 30
  
  # Run all hunts for specific date range
  %(prog)s --all --start 2021-01-01 --end 2021-01-31
  
  # Run hunt with custom parameters
  %(prog)s --hunt dns_tunneling --start 2021-06-01 --end 2021-06-30 \\
           --param min_query_length=40 --param min_query_count=50
  
  # List available hunts
  %(prog)s --list
        """
    )

    parser.add_argument(
        '--hunt',
        choices=['c2_beaconing', 'dns_tunneling', 'lateral_movement'],
        help='Specific hunt to execute'
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='Execute all available hunts'
    )
    
    parser.add_argument(
        '--list',
        action='store_true',
        help='List available hunts and exit'
    )
    
    parser.add_argument(
        '--start',
        type=str,
        help='Start date (YYYY-MM-DD, "yesterday", or "today")'
    )

    parser.add_argument(
        '--end',
        type=str,
        help='End date (YYYY-MM-DD, "yesterday", or "today")'
    )

    parser.add_argument(
        '--lookback',
        type=int,
        metavar='DAYS',
        help='Hunt last N days (alternative to --start/--end)'
    )

    parser.add_argument(
        '--param',
        action='append',
        metavar='KEY=VALUE',
        help='Hunt-specific parameter (can be used multiple times)'
    )

    args = parser.parse_args()

    # List hunts
    if args.list:
        print("\nAvailable Hunts:")
        print("=" * 60)
        print("  c2_beaconing      - Detect periodic C2 communication")
        print("  dns_tunneling     - Detect DNS-based data exfiltration")
        print("  lateral_movement  - Detect lateral movement attempts")
        print("=" * 60)
        return 0
    
    if not args.hunt and not args.all:
        parser.error("Must specify --hunt or --all")

    if args.lookback:
        end_date = date.today()
        start_date = end_date - timedelta(days=args.lookback)
    elif args.start and args.end:
        start_date = parse_date(args.start)
        end_date = parse_date(args.end)
    else:
        parser.error("Must specify either --lookback or both --start and --end")

    # Parse parameters
    params = {}
    if args.param:
        for param in args.param:
            try:
                key, value = param.split('=')
                # Try to convert to appropriate type
                try:
                    value = int(value)
                except ValueError:
                    try:
                        value = float(value)
                    except ValueError:
                        pass  # Keep as string
                params[key] = value
            except ValueError:
                parser.error(f"Invalid parameter format: {param} (use KEY=VALUE)")
    
    # Execute hunt(s)
    orchestrator = HuntOrchestrator()

    try:
        if args.all:
            results = orchestrator.run_all_hunts(start_date, end_date, **params)
        else:
            results = orchestrator.run_hunt(args.hunt, start_date, end_date, **params)
            results = [results]  # Wrap in list for consistent handling
        
        # Print summary
        print("\n" + "=" * 70)
        print("HUNT EXECUTION SUMMARY")
        print("=" * 70)
        for result in results:
            status_symbol = "✓" if result.get('status') == 'completed' else "✗"
            print(f"{status_symbol} {result.get('hunt_name', 'unknown')}: "
                  f"{result.get('findings_count', 0)} findings")
        print("=" * 70)
        
        return 0
        
    except Exception as e:
        print(f"\n✗ Hunt execution failed: {e}", file=sys.stderr)
        return 1
        
    finally:
        orchestrator.cleanup()


if __name__ == "__main__":
    sys.exit(main())

