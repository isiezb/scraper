"""Orchestrator that runs scrapers in parallel where possible.

Phase 1 (parallel): MedReg (CH) + Ärztekammer (DE) + OEGK (AT)
Phase 2 (sequential): DGPRÄC enrichment — needs existing records from Phase 1
"""

import sys
import os
import logging
import schedule
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

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
from arztauskunft_scraper import ArztAuskunftScraper
print("All scraper modules imported OK", flush=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("orchestrator")

# Phase 1: these run in parallel (each creates its own DB connection)
PARALLEL_SCRAPERS = [
    # MedRegScraper,        # CH: disabled — medregom.admin.ch returning 503
    AerztekammerScraper,  # DE: all 17 Landesärztekammern + DGPRÄC nationwide
    KBVScraper,           # DE: 116117.de — nationwide GKV-zugelassene Ärzte
    ArztAuskunftScraper,  # DE: Stiftung Gesundheit — comprehensive (ALL doctors)
    # OEGKScraper,          # AT: disabled — returns 0 results, Playwright too slow
]

# Phase 2: enrichment only, must run after Phase 1
ENRICHMENT_SCRAPERS = [
    DGPRAECScraper,       # DACH: society membership enrichment
]


def _run_scraper(scraper_cls):
    """Run a single scraper (used by ThreadPoolExecutor)."""
    scraper = scraper_cls()
    try:
        logger.info(f"Starting {scraper.name}...")
        scraper.run()
        logger.info(f"Finished {scraper.name}")
        return scraper.name, True
    except Exception as e:
        logger.error(f"{scraper.name} failed: {e}")
        return scraper.name, False
    finally:
        scraper.close()


def _dedup_existing():
    """Remove duplicate doctor records, keeping the one with the most data."""
    from db import get_conn
    conn = get_conn()
    cur = conn.cursor()

    # Find duplicate name groups
    cur.execute("""
        SELECT LOWER(vorname), LOWER(nachname)
        FROM aerzte
        GROUP BY LOWER(vorname), LOWER(nachname)
        HAVING COUNT(*) > 1
    """)
    dupes = cur.fetchall()

    if not dupes:
        logger.info("No duplicates found")
        cur.close()
        conn.close()
        return

    total_removed = 0
    for vorname, nachname in dupes:
        # Get all records for this name, ordered by most data (non-null columns) desc
        cur.execute("""
            SELECT id, plz, telefon, website_url, verified,
                   (CASE WHEN plz IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN telefon IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN website_url IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN strasse IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN schwerpunkte IS NOT NULL THEN 1 ELSE 0 END +
                    CASE WHEN verified = TRUE THEN 5 ELSE 0 END) AS score
            FROM aerzte
            WHERE LOWER(vorname) = %s AND LOWER(nachname) = %s
            ORDER BY score DESC, id ASC
        """, (vorname, nachname))
        records = cur.fetchall()

        if len(records) <= 1:
            continue

        # Keep the best record (highest score), delete the rest
        keep_id = records[0][0]
        delete_ids = [r[0] for r in records[1:]]

        # Move child references to the kept record
        for del_id in delete_ids:
            cur.execute("UPDATE spezialisierungen SET arzt_id = %s WHERE arzt_id = %s", (keep_id, del_id))
            cur.execute("UPDATE mitgliedschaften SET arzt_id = %s WHERE arzt_id = %s", (keep_id, del_id))
            cur.execute("UPDATE werdegang SET arzt_id = %s WHERE arzt_id = %s", (keep_id, del_id))

        # Delete duplicate records
        cur.execute("DELETE FROM aerzte WHERE id = ANY(%s)", (delete_ids,))
        total_removed += len(delete_ids)

    conn.commit()
    cur.close()
    logger.info(f"Dedup: removed {total_removed} duplicate records from {len(dupes)} name groups")

    # Dedup spezialisierungen: remove duplicate (arzt_id, eingriff) keeping lowest id
    cur = conn.cursor()
    cur.execute("""
        DELETE FROM spezialisierungen
        WHERE id NOT IN (
            SELECT MIN(id) FROM spezialisierungen
            GROUP BY arzt_id, LOWER(eingriff)
        )
    """)
    spec_removed = cur.rowcount
    conn.commit()
    cur.close()
    if spec_removed:
        logger.info(f"Dedup: removed {spec_removed} duplicate spezialisierungen")
    conn.close()


def run_all():
    logger.info("Starting scraper run")
    init_db()

    # One-time dedup of existing records
    try:
        _dedup_existing()
    except Exception as e:
        logger.error(f"Dedup failed: {e}")

    # One-time cleanup v2: reset progress + delete corrupted "Mehr Details" record
    try:
        from db import get_conn
        conn = get_conn()
        cur = conn.cursor()
        # Reset progress for Berlin, Brandenburg, and ArztAuskunft
        cur.execute("""
            DELETE FROM scraper_progress
            WHERE (scraper = 'aerztekammer_de' AND source_key IN ('kammer_AEKB', 'kammer_LAEKB'))
               OR (scraper = 'arztauskunft_de')
        """)
        if cur.rowcount:
            logger.info(f"Reset {cur.rowcount} stale progress entries (Berlin/Brandenburg/ArztAuskunft)")
        # Delete corrupted record from old ArztAuskunft bug
        cur.execute("DELETE FROM aerzte WHERE vorname = 'Mehr' AND nachname = 'Details'")
        if cur.rowcount:
            logger.info(f"Deleted {cur.rowcount} corrupted 'Mehr Details' record(s)")
        # Delete institution records scraped as doctors
        cur.execute("""
            DELETE FROM aerzte WHERE source = 'arztauskunft_de'
            AND (LOWER(vorname) || ' ' || LOWER(nachname)) ~*
                '(klinik|kliniken|krankenhaus|hospital|clinic|clinicum|praxis|zentrum|center|centrum|institut|universit|berufsgen|gemeinschaftspraxis|mvz|gmbh|ggmbh|gbr|stiftung|akademie|ambulanz|abteilung|bergmannsheil|charit|asklepios|helios|vivantes|agaplesion|ameos|atos|sana |diakonie|diakovere|caritas|evangelisch|evang\.|kathol|residenz|campus|bundeswehr|fach.rzte|e\.v\.|co\. kg|.sthetik|aesthetic|surgery|chirurgia|esthetica|ethianum|medcenter|lubinus|dorow|beauty|lacomed|lipoedem)'
        """)
        if cur.rowcount:
            logger.info(f"Deleted {cur.rowcount} institution records mistakenly saved as doctors")
        # Fix mojibake in city names (UTF-8 decoded as Latin-1)
        cur.execute("UPDATE aerzte SET stadt = REPLACE(stadt, 'Ã¶', 'ö') WHERE stadt LIKE '%Ã¶%'")
        cur.execute("UPDATE aerzte SET stadt = REPLACE(stadt, 'Ã¼', 'ü') WHERE stadt LIKE '%Ã¼%'")
        cur.execute("UPDATE aerzte SET stadt = REPLACE(stadt, 'Ã¤', 'ä') WHERE stadt LIKE '%Ã¤%'")
        cur.execute("UPDATE aerzte SET stadt = REPLACE(stadt, 'Ã\x9f', 'ß') WHERE stadt LIKE '%Ã\x9f%'")
        cur.execute("UPDATE aerzte SET stadt = REPLACE(stadt, 'Ã', 'Ü') WHERE stadt LIKE '%Ã%'")
        # Also fix in vorname/nachname
        for col in ('vorname', 'nachname', 'stadt', 'strasse'):
            cur.execute(f"UPDATE aerzte SET {col} = REPLACE({col}, 'Ã¶', 'ö') WHERE {col} LIKE '%Ã¶%'")
            cur.execute(f"UPDATE aerzte SET {col} = REPLACE({col}, 'Ã¼', 'ü') WHERE {col} LIKE '%Ã¼%'")
            cur.execute(f"UPDATE aerzte SET {col} = REPLACE({col}, 'Ã¤', 'ä') WHERE {col} LIKE '%Ã¤%'")
        logger.info("Fixed mojibake encoding in city/name fields")
        conn.commit()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"Cleanup failed: {e}")

    # Phase 1: Run main scrapers in parallel
    logger.info("Phase 1: Running main scrapers in parallel...")
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = {executor.submit(_run_scraper, cls): cls for cls in PARALLEL_SCRAPERS}
        for future in as_completed(futures):
            name, success = future.result()
            status = "OK" if success else "FAILED"
            logger.info(f"  {name}: {status}")

    # Phase 2: Enrichment scrapers (sequential, need Phase 1 data)
    logger.info("Phase 2: Running enrichment scrapers...")
    for scraper_cls in ENRICHMENT_SCRAPERS:
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
