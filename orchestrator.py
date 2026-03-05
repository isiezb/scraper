"""Orchestrator that runs all scrapers sequentially.

Can be run as a one-shot job or scheduled to repeat periodically.
"""

import sys
import logging
import schedule
import time

from db import init_db
from klinik_scraper import KlinikScraper
from aerztekammer_scraper import AerztekammerScraper
from dgpraec_scraper import DGPRAECScraper

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger("orchestrator")

SCRAPERS = [
    KlinikScraper,
    AerztekammerScraper,
    DGPRAECScraper,
]


def run_all():
    logger.info("Starting scraper run")
    init_db()

    for scraper_cls in SCRAPERS:
        scraper = scraper_cls()
        try:
            logger.info(f"Running {scraper.name}...")
            scraper.run()
        except Exception as e:
            logger.error(f"{scraper.name} failed: {e}")
        finally:
            scraper.close()

    logger.info("Scraper run complete")


def main():
    if "--once" in sys.argv:
        run_all()
        return

    # Default: run once immediately, then weekly
    interval_hours = 168  # 7 days
    for arg in sys.argv[1:]:
        if arg.startswith("--interval="):
            interval_hours = int(arg.split("=")[1])

    logger.info(f"Scheduling scrapers every {interval_hours}h")
    run_all()

    schedule.every(interval_hours).hours.do(run_all)

    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    main()
