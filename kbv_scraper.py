"""Scraper for KBV / 116117.de (Germany — GKV-Zulassung).

Uses the internal JSON API at arztsuche.116117.de to find GKV-zugelassene Ärzte
with plastic/aesthetic surgery specializations.
After scraping, merges with Ärztekammer data: doctors found only in
Ärztekammer (not in KBV) get gkv_zugelassen = false.
"""

import base64
import random
import time
from datetime import datetime, timezone
from base_scraper import BaseScraper, generate_slug

API_URL = "https://arztsuche.116117.de/api/data"
API_USER = "bdps"
API_PASS = "fkr493mvg_f"

# Filter definitions matching the 116117 API's internal codes
# fgf = Fachgebiet (specific specialty), zbk = Zusatzqualifikation
SEARCH_FILTERS = [
    {
        "label": "Plastische Chirurgie",
        "kategorie": "koerper",
        "filters": [{"title": "Fachgebiet", "fieldName": "fgf", "selectedCodes": ["322"]}],
    },
]

# Search grid: German cities spaced ~100km apart for full coverage
SEARCH_LOCATIONS = [
    {"name": "Berlin", "lat": 52.520, "lon": 13.405},
    {"name": "Hamburg", "lat": 53.551, "lon": 9.994},
    {"name": "München", "lat": 48.137, "lon": 11.576},
    {"name": "Köln", "lat": 50.938, "lon": 6.960},
    {"name": "Frankfurt", "lat": 50.110, "lon": 8.682},
    {"name": "Stuttgart", "lat": 48.776, "lon": 9.183},
    {"name": "Düsseldorf", "lat": 51.228, "lon": 6.774},
    {"name": "Leipzig", "lat": 51.340, "lon": 12.375},
    {"name": "Dresden", "lat": 51.051, "lon": 13.738},
    {"name": "Hannover", "lat": 52.376, "lon": 9.732},
    {"name": "Nürnberg", "lat": 49.452, "lon": 11.077},
    {"name": "Dortmund", "lat": 51.514, "lon": 7.468},
    {"name": "Bremen", "lat": 53.080, "lon": 8.801},
    {"name": "Essen", "lat": 51.457, "lon": 7.012},
    {"name": "Freiburg", "lat": 47.999, "lon": 7.842},
    {"name": "Rostock", "lat": 54.092, "lon": 12.099},
    {"name": "Kiel", "lat": 54.323, "lon": 10.123},
    {"name": "Saarbrücken", "lat": 49.234, "lon": 6.997},
    {"name": "Erfurt", "lat": 50.978, "lon": 11.029},
    {"name": "Magdeburg", "lat": 52.121, "lon": 11.628},
    {"name": "Potsdam", "lat": 52.391, "lon": 13.064},
    {"name": "Mainz", "lat": 49.993, "lon": 8.247},
    {"name": "Kassel", "lat": 51.313, "lon": 9.497},
    {"name": "Augsburg", "lat": 48.366, "lon": 10.899},
    {"name": "Regensburg", "lat": 49.013, "lon": 12.102},
    {"name": "Würzburg", "lat": 49.794, "lon": 9.929},
    {"name": "Mannheim", "lat": 49.489, "lon": 8.467},
    {"name": "Ulm", "lat": 48.402, "lon": 9.988},
    {"name": "Oldenburg", "lat": 53.143, "lon": 8.214},
    {"name": "Göttingen", "lat": 51.534, "lon": 9.935},
]


def _gen_req_val(lat: float, lon: float) -> str:
    """Generate the req-val header (anti-bot token) for LATLON requests."""
    lat2 = lat + 1.1
    lon2 = lon + 2.3
    ts = str(int(time.time() * 1000))
    int_lat = str(lat2).split(".")[0]
    frac_lat = str(lat2).split(".")[1][0] if "." in str(lat2) else "0"
    int_lon = str(lon2).split(".")[0]
    frac_lon = str(lon2).split(".")[1][0] if "." in str(lon2) else "0"
    token = (
        int_lat[-1] + ts[-1] + int_lon[-1] + ts[-2] + frac_lat + ts[-3] + frac_lon
    )
    return base64.b64encode(token.encode()).decode()


class KBVScraper(BaseScraper):
    name = "kbv_116117"
    min_delay = 2.0
    max_delay = 4.0

    def __init__(self):
        super().__init__()
        self.seen_slugs = set()
        self._consecutive_errors = 0
        self._init_session()

    def _init_session(self):
        """Visit the main page to obtain session cookies before API calls."""
        try:
            self.session.headers.update({
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            })
            self.session.get("https://arztsuche.116117.de", timeout=15)
            self.logger.info("Session initialized with cookies from main page")
        except Exception as e:
            self.logger.warning(f"Could not initialize session cookies: {e}")

    def run(self):
        total_before = self.stats["neu"]

        for search_def in SEARCH_FILTERS:
            label = search_def["label"]
            self.logger.info(f"Searching 116117 for: {label}")

            for loc in SEARCH_LOCATIONS:
                progress_key = f"{label}_{loc['name']}"
                _, completed = self.get_progress(progress_key)
                if completed:
                    continue

                try:
                    count = self._search(search_def, loc)
                    if count is not None:
                        self.save_progress(progress_key, count, completed=True)
                except Exception as e:
                    self.logger.error(f"Failed {label} in {loc['name']}: {e}")

                self.wait()

            self.logger.info(f"  {label}: {self.stats['neu'] - total_before} new so far")

        # Mark Ärztekammer-only doctors as not GKV-zugelassen
        if self.stats["neu"] > 0 or self.stats["aktualisiert"] > 0:
            self._mark_non_gkv_doctors()

        self.finalize()

    def _api_post(self, location: dict, search_def: dict, max_retries: int = 3):
        """POST to 116117 API with retry/backoff for 500 errors."""
        for attempt in range(max_retries):
            lat, lon = location["lat"], location["lon"]
            req_val = _gen_req_val(lat, lon)

            body = {
                "r": 100,
                "locType": "LATLON",
                "lat": lat,
                "lon": lon,
                "plz": None,
                "osmId": None,
                "osmType": None,
                "filterSelections": search_def["filters"],
                "locOrigin": "USER_INPUT",
                "searchTrigger": "INITIAL",
                "viaDeeplink": False,
            }

            resp = self.session.post(
                API_URL,
                json=body,
                headers={
                    "req-val": req_val,
                    "Content-Type": "application/json",
                    "Origin": "https://arztsuche.116117.de",
                    "Referer": "https://arztsuche.116117.de/",
                },
                auth=(API_USER, API_PASS),
                timeout=30,
            )

            if resp.status_code == 200:
                self._consecutive_errors = 0
                return resp

            self._consecutive_errors += 1
            backoff = min(120, 10 * (2 ** attempt)) + random.uniform(0, 5)

            if self._consecutive_errors >= 5:
                # Likely WAF-blocked — reset session and wait longer
                self.logger.warning(
                    f"  {location['name']}: HTTP {resp.status_code}, "
                    f"{self._consecutive_errors} consecutive errors — "
                    f"resetting session, waiting {backoff:.0f}s"
                )
                self.session.close()
                import requests as req_lib
                self.session = req_lib.Session()
                from base_scraper import USER_AGENTS
                self.session.headers.update({
                    "User-Agent": random.choice(USER_AGENTS),
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "de-DE,de;q=0.9,en;q=0.5",
                })
                self._init_session()
                self._consecutive_errors = 0
                backoff = max(backoff, 60)
            else:
                self.logger.warning(
                    f"  {location['name']}: HTTP {resp.status_code}, "
                    f"retry {attempt+1}/{max_retries} in {backoff:.0f}s"
                )

            time.sleep(backoff)

        return None

    def _search(self, search_def: dict, location: dict) -> int | None:
        """Search for doctors of a specialty near a location. Returns result count."""
        resp = self._api_post(location, search_def)
        if resp is None:
            self.logger.error(f"  {location['name']}: all retries failed")
            return None

        content_type = resp.headers.get("content-type", "")
        if "json" not in content_type:
            self.logger.error(f"  {location['name']}: non-JSON response: {content_type}")
            return None

        data = resp.json()
        praxen = data.get("arztPraxisDatas", [])
        if not praxen:
            return 0

        more = data.get("moreResults", False)
        if more:
            self.logger.warning(
                f"  {location['name']}: {len(praxen)} results but moreResults=True "
                f"(some doctors may be missed)"
            )

        count = 0
        for praxis in praxen:
            try:
                if self._process_doctor(praxis, search_def):
                    count += 1
            except Exception as e:
                self.logger.error(f"  Failed processing: {e}")

        if count > 0:
            self.logger.info(
                f"  {location['name']}: {len(praxen)} results, {count} new/updated"
            )
        return len(praxen)

    def _process_doctor(self, data: dict, search_def: dict) -> bool:
        """Process a single doctor/practice from the 116117 API. Returns True if inserted/updated."""
        # The API returns one record per practice, with doctor info at top level
        # arzt=true means it's a doctor (vs. a Praxis entry)
        vorname = data.get("vorname", "")
        nachname = data.get("name", "")
        if not vorname or not nachname:
            return False

        titel = data.get("titel", "")
        slug = generate_slug(titel, vorname, nachname)
        if slug in self.seen_slugs:
            return False
        self.seen_slugs.add(slug)

        anrede = data.get("anrede", "")
        geschlecht = self._map_gender(data.get("geschlecht", anrede))

        # Build address from components
        strasse = data.get("strasse", "")
        hausnummer = data.get("hausnummer", "")
        if strasse and hausnummer:
            strasse = f"{strasse} {hausnummer}"

        # Use the search filter label as facharzttitel — the ag (Arztgruppen)
        # list contains the broad category (e.g. "Chirurgie und Orthopädie")
        # which is too generic for our directory.
        facharzttitel = "Plastische und Ästhetische Chirurgie"

        arzt_data = {
            "vorname": vorname,
            "nachname": nachname,
            "titel": titel,
            "geschlecht": geschlecht,
            "ist_facharzt": True,
            "facharzttitel": facharzttitel,
            "selbstbezeichnung": facharzttitel,
            "land": "DE",
            "stadt": data.get("ort"),
            "plz": data.get("plz"),
            "strasse": strasse or None,
            "seo_slug": slug,
            "datenquelle": "kbv_116117",
            "quelle_url": "https://arztsuche.116117.de",
            "telefon": data.get("tel") or None,
            "fax": data.get("fax") or None,
            "email": data.get("email") or None,
            "website_url": data.get("web") or None,
            "gkv_zugelassen": True,
            "verified": True,
            "source": "kbv",
            "source_type": "official",
            "last_verified_at": datetime.now(timezone.utc).isoformat(),
        }

        arzt_id = self.upsert_arzt(arzt_data)
        if arzt_id:
            self.insert_spezialisierungen(arzt_id, [{
                "kategorie": search_def["kategorie"],
                "eingriff": facharzttitel,
                "erfahrungslevel": "spezialist",
            }])
            return True
        return False

    def _mark_non_gkv_doctors(self):
        """Mark Ärztekammer doctors NOT found in KBV as gkv_zugelassen = false."""
        cur = self.conn.cursor()
        cur.execute(
            """UPDATE aerzte SET gkv_zugelassen = FALSE
               WHERE land = 'DE'
               AND source = 'aerztekammer_de'
               AND gkv_zugelassen IS NULL
               AND id NOT IN (
                   SELECT id FROM aerzte WHERE source = 'kbv' OR gkv_zugelassen = TRUE
               )"""
        )
        affected = cur.rowcount
        self.conn.commit()
        cur.close()
        if affected:
            self.logger.info(f"Marked {affected} Ärztekammer-only doctors as gkv_zugelassen=false")

    def _map_gender(self, gender) -> str | None:
        if not gender:
            return None
        g = str(gender).lower()
        if g in ("m", "male", "männlich", "1", "herr"):
            return "m"
        if g in ("f", "w", "female", "weiblich", "2", "frau"):
            return "w"
        return "d"


if __name__ == "__main__":
    scraper = KBVScraper()
    try:
        scraper.run()
    finally:
        scraper.close()
