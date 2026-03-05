"""Scraper for KBV / 116117.de (Germany — GKV-Zulassung).

Uses the JSON API at arztsuche.116117.de to find GKV-zugelassene Ärzte.
After scraping, merges with Ärztekammer data: doctors found only in
Ärztekammer (not in KBV) get gkv_zugelassen = false.
"""

import re
from datetime import datetime, timezone
from base_scraper import BaseScraper, generate_slug

# 116117 Arztsuche API endpoint
API_BASE = "https://arztsuche.116117.de/api"
LOCATION_URL = f"{API_BASE}/location"

# Relevant Fachgruppen codes for aesthetic/plastic surgery
FACHGRUPPEN = [
    {"code": "51", "name": "Plastische Chirurgie", "kategorie": "koerper"},
    {"code": "52", "name": "Plastische und Ästhetische Chirurgie", "kategorie": "koerper"},
    {"code": "11", "name": "Chirurgie", "kategorie": "koerper"},
    {"code": "14", "name": "Dermatologie", "kategorie": "minimal_invasiv"},
    {"code": "34", "name": "Mund-Kiefer-Gesichtschirurgie", "kategorie": "gesicht"},
    {"code": "16", "name": "Hals-Nasen-Ohren-Heilkunde", "kategorie": "gesicht"},
]

# PLZ ranges to iterate for full DE coverage (first 2 digits)
# Ensures no gaps between city-based radius searches
PLZ_PREFIXES = [f"{i:02d}" for i in range(1, 100)]

# Major German cities with coordinates for radius search
SEARCH_LOCATIONS = [
    {"name": "Berlin", "lat": 52.520, "lon": 13.405, "bundesland": "Berlin"},
    {"name": "Hamburg", "lat": 53.551, "lon": 9.994, "bundesland": "Hamburg"},
    {"name": "München", "lat": 48.137, "lon": 11.576, "bundesland": "Bayern"},
    {"name": "Köln", "lat": 50.938, "lon": 6.960, "bundesland": "Nordrhein-Westfalen"},
    {"name": "Frankfurt", "lat": 50.110, "lon": 8.682, "bundesland": "Hessen"},
    {"name": "Stuttgart", "lat": 48.776, "lon": 9.183, "bundesland": "Baden-Württemberg"},
    {"name": "Düsseldorf", "lat": 51.228, "lon": 6.774, "bundesland": "Nordrhein-Westfalen"},
    {"name": "Leipzig", "lat": 51.340, "lon": 12.375, "bundesland": "Sachsen"},
    {"name": "Dresden", "lat": 51.051, "lon": 13.738, "bundesland": "Sachsen"},
    {"name": "Hannover", "lat": 52.376, "lon": 9.732, "bundesland": "Niedersachsen"},
    {"name": "Nürnberg", "lat": 49.452, "lon": 11.077, "bundesland": "Bayern"},
    {"name": "Dortmund", "lat": 51.514, "lon": 7.468, "bundesland": "Nordrhein-Westfalen"},
    {"name": "Bremen", "lat": 53.080, "lon": 8.801, "bundesland": "Bremen"},
    {"name": "Essen", "lat": 51.457, "lon": 7.012, "bundesland": "Nordrhein-Westfalen"},
    {"name": "Freiburg", "lat": 47.999, "lon": 7.842, "bundesland": "Baden-Württemberg"},
    {"name": "Rostock", "lat": 54.092, "lon": 12.099, "bundesland": "Mecklenburg-Vorpommern"},
    {"name": "Kiel", "lat": 54.323, "lon": 10.123, "bundesland": "Schleswig-Holstein"},
    {"name": "Saarbrücken", "lat": 49.234, "lon": 6.997, "bundesland": "Saarland"},
    {"name": "Erfurt", "lat": 50.978, "lon": 11.029, "bundesland": "Thüringen"},
    {"name": "Magdeburg", "lat": 52.121, "lon": 11.628, "bundesland": "Sachsen-Anhalt"},
    {"name": "Potsdam", "lat": 52.391, "lon": 13.064, "bundesland": "Brandenburg"},
    {"name": "Mainz", "lat": 49.993, "lon": 8.247, "bundesland": "Rheinland-Pfalz"},
]


class KBVScraper(BaseScraper):
    name = "kbv_116117"
    min_delay = 0.5
    max_delay = 1.5

    def __init__(self):
        super().__init__()
        self.seen_slugs = set()
        self.kbv_doctor_ids = set()  # Track all doctor IDs found in KBV

    def run(self):
        # Phase 1: Search by location
        location_found = 0
        for fachgruppe in FACHGRUPPEN:
            self.logger.info(f"Searching 116117 for: {fachgruppe['name']}")
            for location in SEARCH_LOCATIONS:
                try:
                    self._search_location(fachgruppe, location)
                except Exception as e:
                    self.logger.error(f"Failed {fachgruppe['name']} in {location['name']}: {e}")
                self.wait()
            location_found += self.stats["neu"]

        # Phase 2: PLZ-based search for gaps (skip if API is not working)
        if location_found > 0:
            self.logger.info("Phase 2: PLZ-based gap search")
            for fachgruppe in FACHGRUPPEN:
                for prefix in PLZ_PREFIXES:
                    try:
                        self._search_plz(fachgruppe, f"{prefix}000")
                    except Exception as e:
                        self.logger.error(f"PLZ {prefix}xxx failed: {e}")
                    self.wait()
        else:
            self.logger.warning("Skipping PLZ phase — location phase found 0 doctors (API may be unavailable)")

        # Phase 3: Mark Ärztekammer-only doctors as not GKV-zugelassen
        self._mark_non_gkv_doctors()

        self.finalize()

    def _search_location(self, fachgruppe: dict, location: dict):
        """Search for doctors of a Fachgruppe near a location."""
        params = {
            "lat": location["lat"],
            "lon": location["lon"],
            "radius": 80,
            "fachgruppe": fachgruppe["code"],
            "pageSize": 200,
            "page": 0,
        }

        resp = self.fetch(LOCATION_URL, params=params)
        if not resp:
            return

        content_type = resp.headers.get("content-type", "")
        if "json" not in content_type:
            self.logger.error(f"  Non-JSON response for {location['name']}: {content_type} (first 200 chars: {resp.text[:200]})")
            return

        try:
            data = resp.json()
        except Exception:
            self.logger.error(f"  Failed to parse JSON for {location['name']}")
            return

        results = data if isinstance(data, list) else data.get("results", data.get("data", data.get("doctors", data.get("arztPraxen", []))))
        if not results:
            return

        self.logger.info(f"  {location['name']}: {len(results)} results for {fachgruppe['name']}")

        for doctor in results:
            try:
                self._process_doctor(doctor, fachgruppe, location)
            except Exception as e:
                self.logger.error(f"  Failed processing doctor: {e}")

    def _search_plz(self, fachgruppe: dict, plz: str):
        """Search by PLZ for coverage gaps."""
        params = {
            "plz": plz,
            "fachgruppe": fachgruppe["code"],
            "pageSize": 200,
            "page": 0,
        }

        resp = self.fetch(LOCATION_URL, params=params)
        if not resp:
            return

        try:
            data = resp.json()
        except Exception:
            return

        results = data if isinstance(data, list) else data.get("results", data.get("data", data.get("doctors", [])))
        if not results:
            return

        for doctor in results:
            try:
                self._process_doctor(doctor, fachgruppe, {"name": plz, "bundesland": None})
            except Exception:
                pass

    def _process_doctor(self, data: dict, fachgruppe: dict, location: dict):
        """Process a single doctor from 116117 API response."""
        vorname = data.get("vorname", data.get("firstName", ""))
        nachname = data.get("nachname", data.get("name", data.get("lastName", "")))
        if not vorname or not nachname:
            return

        titel = data.get("titel", data.get("title", ""))

        slug = generate_slug(titel or "", vorname, nachname)
        if slug in self.seen_slugs:
            return
        self.seen_slugs.add(slug)

        stadt = data.get("ort", data.get("city", location["name"]))
        plz = data.get("plz", data.get("zipCode", ""))
        bundesland = data.get("bundesland", location.get("bundesland"))
        facharzttitel = data.get("fachgebiet", data.get("fachgruppe", fachgruppe["name"]))
        telefon = data.get("telefon", data.get("phone", data.get("tel")))

        # Store internal source ID if available
        arztsuche_id = data.get("id", data.get("arztId", data.get("arzt_id")))

        arzt_data = {
            "vorname": vorname,
            "nachname": nachname,
            "titel": titel or "",
            "geschlecht": self._map_gender(data.get("geschlecht", data.get("gender"))),
            "ist_facharzt": True,
            "facharzttitel": facharzttitel,
            "selbstbezeichnung": facharzttitel,
            "land": "DE",
            "stadt": stadt,
            "bundesland": bundesland,
            "plz": str(plz) if plz else None,
            "seo_slug": slug,
            "datenquelle": "kbv_116117",
            "quelle_url": LOCATION_URL,
            "telefon": str(telefon) if telefon else None,
            "arztsuche_id": str(arztsuche_id) if arztsuche_id else None,
            "gkv_zugelassen": True,
            "verified": True,
            "source": "kbv",
            "source_type": "official",
            "last_verified_at": datetime.now(timezone.utc).isoformat(),
        }

        lat = data.get("lat", data.get("latitude"))
        lon = data.get("lon", data.get("longitude"))
        if lat and lon:
            arzt_data["latitude"] = float(lat)
            arzt_data["longitude"] = float(lon)

        arzt_id = self.upsert_arzt(arzt_data)
        if arzt_id:
            self.kbv_doctor_ids.add(arzt_id)
            self.insert_spezialisierungen(arzt_id, [{
                "kategorie": fachgruppe["kategorie"],
                "eingriff": facharzttitel,
                "erfahrungslevel": "spezialist",
            }])

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
