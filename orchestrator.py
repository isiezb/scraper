"""Orchestrator that runs all scrapers sequentially.

Order:
1. MedReg (CH) — best data quality, provides GLN numbers
2. Ärztekammer (DE) — largest DE coverage, state-verified Facharzttitel
3. KBV/116117 (DE) — GKV-Vertragsärzte, merges with Ärztekammer
4. OEGK/ÖÄK (AT) — dual-source cross-reference
5. DGPRÄC etc. (DACH) — enrichment only, never creates records
"""

import sys
import os
import logging
import schedule
import time

# Force unbuffered output so Railway sees logs in real time
os.environ["PYTHONUNBUFFERED"] = "1"

print("=== Orchestrator starting ===", flush=True)
print(f"Python {sys.version}", flush=True)
print(f"DATABASE_URL set: {bool(os.environ.get('DATABASE_URL'))}", flush=True)

from db import init_db
print("db module imported OK", flush=True)

from medreg_scraper import MedRegScraper
from aerztekammer_scraper import AerztekammerScraper
from kbv_scraper import KBVScraper
from oegk_scraper import OEGKScraper
from dgpraec_scraper import DGPRAECScraper
print("All scraper modules imported OK", flush=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("orchestrator")

# Order matters: verified official sources first, then enrichment
SCRAPERS = [
    MedRegScraper,        # CH: best data, GLN numbers (currently 503)
    AerztekammerScraper,  # DE: all 17 Landesärztekammern
    # KBVScraper,         # DE: DISABLED — API requires auth, returns HTML not JSON
    OEGKScraper,          # AT: OEGK + ÖÄK cross-reference
    DGPRAECScraper,       # DACH: society enrichment only
]


def run_all():
    logger.info("Starting scraper run")
    init_db()

    # Clean old data for fresh scrape (early dev — remove once data is stable)
    from db import get_conn
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("DELETE FROM spezialisierungen")
    cur.execute("DELETE FROM mitgliedschaften")
    cur.execute("DELETE FROM werdegang")
    cur.execute("DELETE FROM aerzte")
    conn.commit()
    cur.close()
    conn.close()
    logger.info("Cleared old data for fresh scrape")

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
