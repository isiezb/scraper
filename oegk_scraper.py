"""Scraper for Austrian OEGK / ÖÄK doctor search.

Two sources combined:
1. gesundheitskasse.at/arztsuche — Kassenstatus, Name, Adresse, Fachgebiet
2. arztsuche.at (ÖÄK) — Approbation, Diplome

Both sources use JS rendering → requires Playwright.
Cross-references by name to merge into single records.
verified: true only when both sources confirm the same doctor.
"""

import re
from datetime import datetime, timezone

from base_scraper import BaseScraper, generate_slug, normalize_name

# Relevant Sonderfächer
SONDERFAECHER = [
    "Plastische, Ästhetische und Rekonstruktive Chirurgie",
    "Chirurgie",
    "Haut- und Geschlechtskrankheiten",
    "Mund-, Kiefer- und Gesichtschirurgie",
    "Hals-, Nasen- und Ohrenheilkunde",
    "Augenheilkunde und Optometrie",
]

BUNDESLAND_MAP = {
    "W": "Wien", "NÖ": "Niederösterreich", "OÖ": "Oberösterreich",
    "S": "Salzburg", "T": "Tirol", "V": "Vorarlberg",
    "K": "Kärnten", "ST": "Steiermark", "B": "Burgenland",
}

SEARCH_CITIES = [
    # Bundesland capitals
    {"name": "Wien", "bundesland": "Wien"},
    {"name": "Graz", "bundesland": "Steiermark"},
    {"name": "Linz", "bundesland": "Oberösterreich"},
    {"name": "Salzburg", "bundesland": "Salzburg"},
    {"name": "Innsbruck", "bundesland": "Tirol"},
    {"name": "Klagenfurt", "bundesland": "Kärnten"},
    {"name": "Bregenz", "bundesland": "Vorarlberg"},
    {"name": "St. Pölten", "bundesland": "Niederösterreich"},
    {"name": "Eisenstadt", "bundesland": "Burgenland"},
    # Additional cities
    {"name": "Villach", "bundesland": "Kärnten"},
    {"name": "Wels", "bundesland": "Oberösterreich"},
    {"name": "Dornbirn", "bundesland": "Vorarlberg"},
    {"name": "Wiener Neustadt", "bundesland": "Niederösterreich"},
    {"name": "Steyr", "bundesland": "Oberösterreich"},
    {"name": "Feldkirch", "bundesland": "Vorarlberg"},
    {"name": "Baden", "bundesland": "Niederösterreich"},
    {"name": "Leoben", "bundesland": "Steiermark"},
    {"name": "Leonding", "bundesland": "Oberösterreich"},
    {"name": "Klosterneuburg", "bundesland": "Niederösterreich"},
    {"name": "Krems", "bundesland": "Niederösterreich"},
    {"name": "Traun", "bundesland": "Oberösterreich"},
    {"name": "Amstetten", "bundesland": "Niederösterreich"},
    {"name": "Lustenau", "bundesland": "Vorarlberg"},
    {"name": "Kapfenberg", "bundesland": "Steiermark"},
    {"name": "Mödling", "bundesland": "Niederösterreich"},
    {"name": "Hallein", "bundesland": "Salzburg"},
    {"name": "Kufstein", "bundesland": "Tirol"},
    {"name": "Braunau am Inn", "bundesland": "Oberösterreich"},
    {"name": "Schwechat", "bundesland": "Niederösterreich"},
    {"name": "Spittal an der Drau", "bundesland": "Kärnten"},
    {"name": "Saalfelden", "bundesland": "Salzburg"},
    {"name": "Hall in Tirol", "bundesland": "Tirol"},
    {"name": "Bruck an der Mur", "bundesland": "Steiermark"},
    {"name": "Tulln", "bundesland": "Niederösterreich"},
    {"name": "Wolfsberg", "bundesland": "Kärnten"},
    {"name": "Ried im Innkreis", "bundesland": "Oberösterreich"},
    {"name": "Vöcklabruck", "bundesland": "Oberösterreich"},
]


class OEGKScraper(BaseScraper):
    name = "oegk_at"
    min_delay = 1.5
    max_delay = 3.0

    def __init__(self):
        super().__init__()
        self.seen_slugs = set()
        self._browser = None
        self._page = None
        # Collect doctors from both sources for cross-referencing
        self.oegk_doctors = {}   # normalized_name -> data dict
        self.oeak_doctors = {}   # normalized_name -> data dict

    def _init_browser(self):
        """Initialize Playwright browser (lazy loading)."""
        try:
            from playwright.sync_api import sync_playwright
            self._playwright = sync_playwright().start()
            self._browser = self._playwright.chromium.launch(headless=True)
            self._page = self._browser.new_page()
            self._page.set_extra_http_headers({
                "Accept-Language": "de-DE,de;q=0.9",
            })
            self.logger.info("Playwright browser initialized")
        except ImportError:
            self.logger.warning("Playwright not available, falling back to requests-only mode")
            self._browser = None
        except Exception as e:
            self.logger.error(f"Failed to init Playwright: {e}")
            self._browser = None

    def close(self):
        if self._browser:
            self._browser.close()
            self._playwright.stop()
        super().close()

    def run(self):
        self._init_browser()

        if not self._browser:
            self.logger.error("Playwright required for Austrian sources. Skipping.")
            return

        # Phase 1: Scrape OEGK (gesundheitskasse.at)
        self.logger.info("Phase 1: Scraping OEGK (gesundheitskasse.at)...")
        for city in SEARCH_CITIES:
            for sonderfach in SONDERFAECHER:
                try:
                    self._scrape_oegk(sonderfach, city)
                except Exception as e:
                    self.logger.error(f"OEGK failed for {sonderfach} in {city['name']}: {e}")
                self.wait()

        self.logger.info(f"  OEGK collected: {len(self.oegk_doctors)} doctors")

        # Phase 2: Scrape ÖÄK (arztsuche.at)
        self.logger.info("Phase 2: Scraping ÖÄK (arztsuche.at)...")
        for city in SEARCH_CITIES:
            for sonderfach in SONDERFAECHER:
                try:
                    self._scrape_oeak(sonderfach, city)
                except Exception as e:
                    self.logger.error(f"ÖÄK failed for {sonderfach} in {city['name']}: {e}")
                self.wait()

        self.logger.info(f"  ÖÄK collected: {len(self.oeak_doctors)} doctors")

        # Phase 3: Cross-reference and upsert
        self.logger.info("Phase 3: Cross-referencing and upserting...")
        self._cross_reference_and_upsert()

        self.finalize()

    def _scrape_oegk(self, sonderfach: str, city: dict):
        """Scrape gesundheitskasse.at Arztsuche via Playwright."""
        search_url = f"https://www.gesundheitskasse.at/arztsuche?fach={sonderfach.replace(' ', '+')}&ort={city['name']}"

        try:
            self._page.goto(search_url, timeout=30000)
            self._page.wait_for_load_state("networkidle", timeout=15000)
        except Exception as e:
            self.logger.error(f"  OEGK page load failed: {e}")
            return

        cards = self._page.query_selector_all("[class*='doctor'], [class*='arzt'], [class*='result'], .card, article, .list-item")
        self.logger.info(f"  OEGK {city['name']}/{sonderfach}: {len(cards)} cards")

        for card in cards:
            try:
                text = card.inner_text()
                doctor = self._parse_card_text(text)
                if doctor:
                    doctor["sonderfach"] = sonderfach
                    doctor["bundesland"] = city["bundesland"]
                    doctor["quelle"] = "oegk"
                    doctor["quelle_url"] = search_url
                    # Extract Kassenstatus
                    kassenstatus = self._extract_kassenstatus(text)
                    doctor["kassenstatus"] = kassenstatus
                    norm = normalize_name(doctor["vorname"], doctor["nachname"])
                    self.oegk_doctors[norm] = doctor
            except Exception as e:
                self.logger.error(f"  Failed parsing OEGK card: {e}")

    def _scrape_oeak(self, sonderfach: str, city: dict):
        """Scrape ÖÄK Arztsuche via Playwright."""
        search_url = f"https://www.arztsuche.at/suche?fach={sonderfach.replace(' ', '+')}&ort={city['name']}"

        try:
            self._page.goto(search_url, timeout=30000)
            self._page.wait_for_load_state("networkidle", timeout=15000)
        except Exception as e:
            self.logger.error(f"  ÖÄK page load failed: {e}")
            return

        cards = self._page.query_selector_all("[class*='doctor'], [class*='arzt'], [class*='result'], .card, article, .list-item")
        self.logger.info(f"  ÖÄK {city['name']}/{sonderfach}: {len(cards)} cards")

        for card in cards:
            try:
                text = card.inner_text()
                doctor = self._parse_card_text(text)
                if doctor:
                    doctor["sonderfach"] = sonderfach
                    doctor["bundesland"] = city["bundesland"]
                    doctor["quelle"] = "oeak"
                    doctor["quelle_url"] = search_url
                    doctor["approbiert"] = True
                    norm = normalize_name(doctor["vorname"], doctor["nachname"])
                    self.oeak_doctors[norm] = doctor
            except Exception as e:
                self.logger.error(f"  Failed parsing ÖÄK card: {e}")

    def _cross_reference_and_upsert(self):
        """Merge OEGK and ÖÄK data, then upsert to database."""
        all_names = set(self.oegk_doctors.keys()) | set(self.oeak_doctors.keys())

        for norm_name in all_names:
            oegk = self.oegk_doctors.get(norm_name)
            oeak = self.oeak_doctors.get(norm_name)

            # Determine if both sources confirm this doctor
            both_confirmed = oegk is not None and oeak is not None

            # Use whichever source has data, preferring OEGK for address/Kassenstatus
            primary = oegk or oeak

            vorname = primary["vorname"]
            nachname = primary["nachname"]
            titel = primary.get("titel", "")
            sonderfach = primary.get("sonderfach", "")
            bundesland = primary.get("bundesland", "")
            stadt = primary.get("stadt")
            plz = primary.get("plz")

            slug = generate_slug(titel, vorname, nachname)
            if slug in self.seen_slugs:
                continue
            self.seen_slugs.add(slug)

            kassenstatus = oegk.get("kassenstatus") if oegk else None

            arzt_data = {
                "vorname": vorname,
                "nachname": nachname,
                "titel": titel,
                "geschlecht": None,
                "ist_facharzt": True,
                "facharzttitel": sonderfach,
                "selbstbezeichnung": sonderfach,
                "approbation_verifiziert": bool(oeak),
                "land": "AT",
                "stadt": stadt,
                "bundesland": bundesland,
                "plz": str(plz) if plz else None,
                "seo_slug": slug,
                "datenquelle": "oegk_oaek" if both_confirmed else ("oegk" if oegk else "oeak"),
                "quelle_url": primary.get("quelle_url"),
                "kassenstatus_at": kassenstatus,
                "verified": both_confirmed,
                "source": "oegk_oaek",
                "source_type": "official",
                "last_verified_at": datetime.now(timezone.utc).isoformat() if both_confirmed else None,
            }

            arzt_id = self.upsert_arzt(arzt_data)
            if not arzt_id:
                continue

            kategorie = self._map_kategorie(sonderfach)
            if kategorie:
                self.insert_spezialisierungen(arzt_id, [{
                    "kategorie": kategorie,
                    "eingriff": sonderfach,
                    "erfahrungslevel": "spezialist",
                }])

    def _parse_card_text(self, text: str) -> dict | None:
        """Parse doctor info from card text."""
        lines = [l.strip() for l in text.split("\n") if l.strip()]
        if not lines:
            return None

        name_data = self._extract_name(lines[0])
        if not name_data:
            return None

        for line in lines[1:]:
            plz_match = re.search(r"\b(\d{4})\s+([A-ZÄÖÜa-zäöüß]+)", line)
            if plz_match:
                name_data["plz"] = plz_match.group(1)
                name_data["stadt"] = plz_match.group(2)
                break

        return name_data

    def _extract_name(self, text: str) -> dict | None:
        """Extract name components from text."""
        text = text.strip()
        if not text or len(text) < 4:
            return None

        titel_parts = []
        name_parts = []
        titel_kw = {"prof", "prof.", "dr", "dr.", "med", "med.", "univ", "univ.", "dent", "dent.", "priv.-doz", "priv.-doz."}

        words = text.split()
        in_name = False
        for word in words:
            clean = word.lower().rstrip(".,")
            if not in_name and clean in titel_kw:
                titel_parts.append(word)
            else:
                in_name = True
                if any(c.isdigit() for c in word) or word in (",", "|", "-", "–"):
                    break
                name_parts.append(word)

        if len(name_parts) < 2:
            return None

        return {
            "vorname": name_parts[0],
            "nachname": " ".join(name_parts[1:]),
            "titel": " ".join(titel_parts),
        }

    def _extract_kassenstatus(self, text: str) -> str | None:
        """Extract Kassenstatus from card text."""
        text_lower = text.lower()
        if "kassenarzt" in text_lower or "vertragsarzt" in text_lower or "alle kassen" in text_lower:
            return "Kassenarzt"
        if "wahlarzt" in text_lower or "privatarzt" in text_lower:
            return "Wahlarzt"
        return None

    def _map_kategorie(self, sonderfach: str) -> str | None:
        s = sonderfach.lower()
        if "plastisch" in s or "ästhetisch" in s:
            return "koerper"
        if "haut" in s or "geschlechtskrankheit" in s:
            return "minimal_invasiv"
        if "mund" in s or "kiefer" in s or "gesicht" in s:
            return "gesicht"
        if "hals" in s or "nasen" in s or "ohren" in s:
            return "gesicht"
        if "augen" in s:
            return "gesicht"
        return None


if __name__ == "__main__":
    scraper = OEGKScraper()
    try:
        scraper.run()
    finally:
        scraper.close()
