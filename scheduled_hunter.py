# scheduled_hunter.py
import schedule
import time
from datetime import datetime, date, timedelta
from hunt_orchestrator import HuntOrchestrator
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/home/eric_s/dev_work/github.com/EricS-Arbitr/retro-hunt-lab/end_to_end/hunt_scheduler.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

class ScheduledHunter:
    """Schedule and execute hunts automatically"""
    
    def __init__(self):
        self.orchestrator = None
    
    def daily_hunt_routine(self):
        """Run daily hunt for previous day"""
        logger.info("Starting daily hunt routine")
        
        try:
            if not self.orchestrator:
                self.orchestrator = HuntOrchestrator()
            
            # Hunt previous day
            yesterday = date.today() - timedelta(days=1)
            
            results = self.orchestrator.run_all_hunts(
                start_date=yesterday,
                end_date=yesterday
            )
            
            # Log summary
            total_findings = sum(r.get('findings_count', 0) for r in results)
            logger.info(f"Daily hunt complete: {total_findings} findings across {len(results)} hunts")
            
        except Exception as e:
            logger.error(f"Daily hunt failed: {e}", exc_info=True)

    def weekly_deep_hunt(self):
        """Run comprehensive hunt for past week"""
        logger.info("Starting weekly deep hunt")
        
        try:
            if not self.orchestrator:
                self.orchestrator = HuntOrchestrator()
            
            # Hunt previous week
            end_date = date.today() - timedelta(days=1)
            start_date = end_date - timedelta(days=7)
            
            results = self.orchestrator.run_all_hunts(
                start_date=start_date,
                end_date=end_date
            )
            
            total_findings = sum(r.get('findings_count', 0) for r in results)
            logger.info(f"Weekly hunt complete: {total_findings} findings")
            
        except Exception as e:
            logger.error(f"Weekly hunt failed: {e}", exc_info=True)

    def targeted_c2_hunt(self):
        """Run high-sensitivity C2 hunt"""
        logger.info("Starting targeted C2 beaconing hunt")
        
        try:
            if not self.orchestrator:
                self.orchestrator = HuntOrchestrator()
            
            # Last 3 days with stricter parameters
            results = self.orchestrator.run_lookback_hunt(
                'c2_beaconing',
                lookback_days=3,
                min_connections=5,  # More sensitive
                jitter_threshold=150
            )
            
            logger.info(f"C2 hunt complete: {results.get('findings_count', 0)} findings")
            
        except Exception as e:
            logger.error(f"C2 hunt failed: {e}", exc_info=True)

    def start_scheduler(self):
        """Start the scheduled hunt system"""
        logger.info("Starting hunt scheduler")
        
        # Schedule daily hunt at 2 AM
        schedule.every().day.at("02:00").do(self.daily_hunt_routine)
        
        # Schedule weekly hunt on Mondays at 3 AM
        schedule.every().monday.at("03:00").do(self.weekly_deep_hunt)
        
        # Schedule targeted C2 hunt every 6 hours
        schedule.every(6).hours.do(self.targeted_c2_hunt)
        
        logger.info("Hunt schedule configured:")
        logger.info("  - Daily hunts: 02:00")
        logger.info("  - Weekly hunts: Monday 03:00")
        logger.info("  - C2 hunts: Every 6 hours")
        
        # Run loop
        try:
            while True:
                schedule.run_pending()
                time.sleep(60)  # Check every minute
                
        except KeyboardInterrupt:
            logger.info("Scheduler stopped by user")
            if self.orchestrator:
                self.orchestrator.cleanup()


def main():
    """Run scheduler as daemon"""
    scheduler = ScheduledHunter()
    
    # For testing: run daily routine immediately
    print("Running test hunt...")
    scheduler.daily_hunt_routine()
    
    # Uncomment to run as daemon
    # scheduler.start_scheduler()


if __name__ == "__main__":
    main()