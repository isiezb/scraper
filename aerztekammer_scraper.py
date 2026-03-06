"""Scraper for all 17 German Landesaerztekammern.

Each Kammer has its own website structure and search form.
Uses kammer_config.py for per-Kammer configuration.
Kammern with needs_js=True use Playwright for JS-rendered pages.
Extracts all available fields: name, Facharzttitel, address, phone, email, website.
All entries are verified: true, source: 'aerztekammer_de'.
"""

import random
import re
import time
from datetime import datetime, timezone
from urllib.parse import urljoin

from bs4 import BeautifulSoup
from base_scraper import BaseScraper
from kammer_config import KAMMERN


class AerztekammerScraper(BaseScraper):
    name = "aerztekammer_de"
    min_delay = 1.0
    max_delay = 2.0
    MAX_PAGES = 20  # Safety limit -- plastic surgeons per Kammer should be <100

    RELEVANT_KEYWORDS = {
        "plastisch", "ästhetisch", "aesthetisch", "rekonstruktiv",
        "dermatolog", "haut", "gesichtschirurg", "mund-kiefer",
        "hno", "hals-nasen", "augen", "ophthalmol",
    }

    def __init__(self):
        super().__init__()
        self.seen_slugs = set()
        self._browser = None
        self._page = None

    def _init_browser(self):
        """Initialize Playwright browser (lazy loading)."""
        if self._browser:
            return True
        try:
            from playwright.sync_api import sync_playwright
            self._playwright = sync_playwright().start()
            self._browser = self._playwright.chromium.launch(headless=True)
            self._page = self._browser.new_page()
            self._page.set_extra_http_headers({
                "Accept-Language": "de-DE,de;q=0.9",
            })
            self.logger.info("Playwright browser initialized for Aerztekammer")
            return True
        except ImportError:
            self.logger.warning("Playwright not available")
            return False
        except Exception as e:
            self.logger.error(f"Failed to init Playwright: {e}")
            return False

    def close(self):
        if self._browser:
            self._browser.close()
            self._playwright.stop()
        super().close()

    # Kammern that use direct JSON APIs (no HTML parsing needed)
    API_KAMMERN = {"AEKNO", "AEKHESSEN", "AEKHH", "AEKSL"}
    # Kammern with custom scrapers
    CUSTOM_KAMMERN = {"AEKBW"}
    # Kammern known to have NO public Arztsuche (skip silently)
    SKIP_KAMMERN = {
        "BLAEK",     # Bayern — no public Arztsuche, use 116117.de
        "AEKB",      # Berlin — no working Arztsuche URL
        "AEKHB",     # Bremen — no public Arztsuche
        "AEKMV",     # Mecklenburg-VP — no public Arztsuche
        "AEKN",      # Niedersachsen — no public Arztsuche, use 116117.de
        "LAEKRLP",   # Rheinland-Pfalz — no public Arztsuche, use 116117.de
        "SLAEK",     # Sachsen — Arztsuche URL broken (404)
        "AEKSA",     # Sachsen-Anhalt — no direct Arztsuche
        "AEKSH",     # Schleswig-Holstein — no public Arztsuche
        "LAEKTH",    # Thüringen — no public Arztsuche
        "LAEKB",     # Brandenburg — no Kammer Arztsuche, KV portal only
        "AEKWL",     # Westfalen-Lippe — ExtJS app too complex
    }

    def run(self):
        for kammer in KAMMERN:
            kuerzel = kammer["kuerzel"]
            if kuerzel in self.SKIP_KAMMERN:
                self.logger.info(f"Skipping: {kammer['name']} (no public Arztsuche)")
                continue

            # Check if this Kammer was already completed
            _, completed = self.get_progress(f"kammer_{kuerzel}")
            if completed:
                self.logger.info(f"Skipping: {kammer['name']} (already completed)")
                continue

            self.logger.info(f"Scraping: {kammer['name']} ({kuerzel})")
            try:
                completed = True
                if kuerzel in self.API_KAMMERN:
                    self._scrape_kammer_api(kammer)
                elif kuerzel in self.CUSTOM_KAMMERN:
                    completed = self._scrape_custom(kammer) is not False
                elif kammer.get("needs_js"):
                    self._scrape_kammer_js(kammer)
                else:
                    self._scrape_kammer(kammer)
                # Mark this Kammer as completed (unless scraper signaled incomplete)
                if completed:
                    self.save_progress(f"kammer_{kuerzel}", 0, completed=True)
            except Exception as e:
                self.logger.error(f"Failed {kammer['name']}: {e}")
            self.wait()

        # Scrape DGPRÄC for nationwide coverage (replaces 116117.de)
        self._scrape_dgpraec()

        self.finalize()

    def _scrape_custom(self, kammer: dict):
        """Route to custom scrapers. Returns False if scraper didn't complete."""
        if kammer["kuerzel"] == "AEKBW":
            return self._scrape_bw(kammer)
        return True

    # ── API-based Kammern ────────────────────────────────────────────

    def _scrape_kammer_api(self, kammer: dict):
        """Scrape Kammern that expose JSON APIs."""
        if kammer["kuerzel"] == "AEKNO":
            self._scrape_kvno(kammer)
        elif kammer["kuerzel"] == "AEKHESSEN":
            self._scrape_hessen(kammer)
        elif kammer["kuerzel"] == "AEKHH":
            self._scrape_hamburg(kammer)
        elif kammer["kuerzel"] == "AEKSL":
            self._scrape_saarland(kammer)

    def _scrape_kvno(self, kammer: dict):
        """Scrape Nordrhein via KVNO JSON API (arztsuche.kvno.de)."""
        api_url = "https://arztsuche.kvno.de/api/api/places"
        headers = {
            "Content-Type": "application/json",
            "Accept": "application/json",
            "Origin": "https://arztsuche.kvno.de",
            "Referer": "https://arztsuche.kvno.de/",
        }
        payload = {
            "searchText": "Plastische Chirurgie",
            "near": 500000,
            "address": "",
            "page": 1,
            "pageSize": 200,
            "UserLocation": {"Lat": 51.0, "Lng": 7.0},
        }

        try:
            import requests
            resp = requests.post(api_url, json=payload, headers=headers, timeout=30)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            self.logger.error(f"  KVNO API failed: {e}")
            return

        person_list = data.get("personList", [])
        self.logger.info(f"  KVNO API returned {len(person_list)} doctors (total: {data.get('totalCount', '?')})")

        for person in person_list:
            try:
                doctor = self._parse_kvno_person(person)
                if doctor:
                    self._process_doctor(doctor, kammer)
            except Exception as e:
                self.logger.error(f"  Failed parsing KVNO person: {e}")

    def _parse_kvno_person(self, person: dict) -> dict | None:
        """Parse a KVNO API person entry."""
        vorname = (person.get("vorname") or "").strip()
        nachname = (person.get("nachname") or "").strip()
        if not vorname or not nachname:
            return None

        titel = (person.get("title") or "").strip()
        if titel == "None":
            titel = ""

        doctor = {
            "vorname": vorname,
            "nachname": nachname,
            "titel": titel,
            "facharzttitel": person.get("fachgebiet"),
            "schwerpunkte": ", ".join(person.get("bereiche", [])),
            "plz": person.get("plz"),
            "stadt": person.get("ort"),
            "strasse": person.get("strasse"),
        }

        # Add hausnummer to strasse
        if person.get("hausnummer") and doctor["strasse"]:
            doctor["strasse"] = f"{doctor['strasse']} {person['hausnummer']}"

        # Phone
        if person.get("phone"):
            ph = person["phone"][0]
            vw = ph.get("telefonvorwahl", "")
            nr = ph.get("telefonnummer", "")
            if vw or nr:
                doctor["telefon"] = f"{vw} {nr}".strip()

        # Email
        if person.get("email"):
            doctor["email"] = person["email"][0].get("emailAddress")

        # Website
        if person.get("homePage"):
            web = person["homePage"][0].get("webSite", "")
            if web and not web.startswith("http"):
                web = f"https://{web}"
            if web:
                doctor["website_url"] = web

        # Gender
        geschlecht = person.get("geschlect")
        if geschlecht == 2:
            doctor["geschlecht"] = "m"
        elif geschlecht == 1:
            doctor["geschlecht"] = "w"

        # Coordinates
        place = person.get("place", {})
        if place.get("latitude"):
            doctor["latitude"] = place["latitude"]
            doctor["longitude"] = place.get("longitute")  # typo in API

        return doctor

    # ── Hessen (arztsuchehessen.de — REST API) ──────────────────────

    def _scrape_hessen(self, kammer: dict):
        """Scrape Hessen via arztsuchehessen.de REST API (/api/suche).

        Uses professionDoctor[] filter values:
        - 267 = Plastische(r) und Ästhetische(r) Chirurg/-in
        - 266 = Plastische(r) Chirurg/-in
        """
        import requests

        api_url = "https://arztsuchehessen.de/api/suche"
        try:
            resp = requests.post(
                api_url,
                data={"doctorType": "F", "professionDoctor[]": ["267", "266"]},
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            self.logger.error(f"  Hessen API failed: {e}")
            return

        items = data.get("items", [])
        self.logger.info(f"  Hessen API returned {len(items)} doctors")

        for item in items:
            try:
                doctor = self._parse_hessen_item(item)
                if doctor:
                    self._process_doctor(doctor, kammer)
            except Exception as e:
                self.logger.error(f"  Failed parsing Hessen item: {e}")

    def _parse_hessen_item(self, item: dict) -> dict | None:
        """Parse a Hessen API result item."""
        headline = (item.get("headline") or "").strip()
        if not headline or len(headline) < 4:
            return None

        # headline format: "Frau Dr. med. Stefanie Adili" or "Herr Prof. Dr. med. ..."
        # Strip Herr/Frau prefix and extract gender
        geschlecht = None
        if headline.startswith("Frau "):
            geschlecht = "w"
            headline = headline[5:]
        elif headline.startswith("Herr "):
            geschlecht = "m"
            headline = headline[5:]

        name_data = self._extract_name_from_text(headline)
        if not name_data:
            return None

        doctor = {**name_data}
        if geschlecht:
            doctor["geschlecht"] = geschlecht

        # Specialty from description
        desc = (item.get("description") or "").strip()
        if desc:
            doctor["facharzttitel"] = desc

        # Address
        addr = item.get("address", {})
        if addr:
            doctor["strasse"] = addr.get("street")
            doctor["plz"] = addr.get("zip")
            doctor["stadt"] = addr.get("place")
            if addr.get("phone"):
                doctor["telefon"] = addr["phone"]
            if addr.get("fax"):
                doctor["fax"] = addr["fax"]

        return doctor

    # ── Hamburg (aerztekammer-hamburg.org — JSON API) ───────────────

    def _scrape_hamburg(self, kammer: dict):
        """Scrape Hamburg via aerztekammer-hamburg.org JSON API.

        Single GET returns ALL ~3,700 doctors as JSON. We filter for
        plastic surgery branches locally.
        """
        import requests

        api_url = "https://aerztekammer-hamburg.org/search_api/arztsuche.php"
        try:
            resp = requests.get(api_url, timeout=60)
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            self.logger.error(f"  Hamburg API failed: {e}")
            return

        results = data.get("results", [])
        self.logger.info(f"  Hamburg API returned {len(results)} total doctors")

        count = 0
        for entry in results:
            try:
                doctor = self._parse_hamburg_entry(entry)
                if doctor:
                    self._process_doctor(doctor, kammer)
                    count += 1
            except Exception as e:
                self.logger.error(f"  Failed parsing Hamburg entry: {e}")

        self.logger.info(f"  Hamburg: processed {count} plastic surgeons")

    def _parse_hamburg_entry(self, entry: dict) -> dict | None:
        """Parse a Hamburg API entry. Returns None if not a plastic surgeon."""
        branches = entry.get("branch", [])
        branch_str = " ".join(branches).lower()
        if not any(kw in branch_str for kw in ("plastisch", "ästhetisch", "aesthetisch")):
            return None

        vorname = (entry.get("first_name") or "").strip()
        nachname = (entry.get("last_name") or "").strip()
        if not vorname or not nachname:
            return None

        titel = (entry.get("degree") or "").strip()

        doctor = {
            "vorname": vorname,
            "nachname": nachname,
            "titel": titel,
            "facharzttitel": ", ".join(branches),
            "plz": str(entry.get("zip", "")).strip() or None,
            "stadt": "Hamburg",
            "strasse": (entry.get("street") or "").strip() or None,
        }

        # Contact info
        phone = (entry.get("phone") or "").strip()
        if phone:
            doctor["telefon"] = phone
        fax = (entry.get("fax") or "").strip()
        if fax:
            doctor["fax"] = fax
        email = (entry.get("email") or "").strip()
        if email:
            doctor["email"] = email
        web = (entry.get("web") or "").strip()
        if web:
            if not web.startswith("http"):
                web = f"https://{web}"
            doctor["website_url"] = web

        # Focus areas
        focus = entry.get("focus", [])
        if focus:
            doctor["schwerpunkte"] = ", ".join(focus)

        return doctor

    # ── Saarland (aerztekammer-saarland.de — AJAX HTML) ───────────

    def _scrape_saarland(self, kammer: dict):
        """Scrape Saarland via aerztekammer-saarland.de AJAX endpoint.

        GET request returns HTML fragments with doctor entries.
        """
        import requests

        url = "https://www.aerztekammer-saarland.de/aerzte/informationenfueraerzte/arztsuche/results.inc"
        params = {"f": "Plastische und Ästhetische Chirurgie"}
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            html = resp.text
        except Exception as e:
            self.logger.error(f"  Saarland API failed: {e}")
            return

        soup = BeautifulSoup(html, "html.parser")
        entries = soup.select("article.entry.leftpad.single")

        self.logger.info(f"  Saarland returned {len(entries)} entries")

        count = 0
        for entry in entries:
            try:
                doctor = self._parse_saarland_entry(entry)
                if doctor:
                    self._process_doctor(doctor, kammer)
                    count += 1
            except Exception as e:
                self.logger.error(f"  Failed parsing Saarland entry: {e}")

        self.logger.info(f"  Saarland: processed {count} plastic surgeons")

    def _parse_saarland_entry(self, entry) -> dict | None:
        """Parse a Saarland HTML entry (article.entry.leftpad.single)."""
        # Name from h2.entry-title
        name_el = entry.select_one("h2.entry-title")
        if not name_el:
            return None

        name_text = name_el.get_text(strip=True)
        if not name_text or len(name_text) < 4:
            return None

        name_data = self._extract_name_from_text(name_text)
        if not name_data:
            return None

        doctor = {**name_data, "facharzttitel": "Plastische und Ästhetische Chirurgie"}

        # Address from first col-md-6
        addr_col = entry.select_one(".col-md-6")
        if addr_col:
            addr_text = addr_col.decode_contents()
            addr_lines = [l.strip() for l in re.split(r"<br\s*/?>", addr_text) if l.strip()]
            for line in addr_lines:
                # Skip bold lines (practice name)
                if "<b>" in line:
                    continue
                clean = BeautifulSoup(line, "html.parser").get_text(strip=True)
                plz_match = re.match(r"(\d{5})\s+(.+)", clean)
                if plz_match:
                    doctor["plz"] = plz_match.group(1)
                    doctor["stadt"] = plz_match.group(2).strip()
                elif clean and len(clean) > 3 and not doctor.get("strasse"):
                    doctor["strasse"] = clean

        # Contact from second col-md-6
        contact_col = entry.select(".col-md-6")
        if len(contact_col) > 1:
            contact_text = contact_col[1].decode_contents()
            contact_lines = [l.strip() for l in re.split(r"<br\s*/?>", contact_text) if l.strip()]
            for line in contact_lines:
                clean = BeautifulSoup(line, "html.parser").get_text(strip=True)
                if clean.startswith("Telefon:"):
                    doctor["telefon"] = clean.replace("Telefon:", "").strip()
                elif clean.startswith("Fax:"):
                    doctor["fax"] = clean.replace("Fax:", "").strip()

            # Email from mailto link
            email_link = contact_col[1].select_one("a[href^='mailto:']")
            if email_link:
                doctor["email"] = email_link.get_text(strip=True)

            # Website from non-mailto link
            for a in contact_col[1].select("a[href]"):
                href = a.get("href", "")
                if href and not href.startswith("mailto:"):
                    if not href.startswith("http"):
                        href = f"https://{href}"
                    doctor["website_url"] = href
                    break

        # Schwerpunkte from entry-cats
        cats = entry.select_one("span.entry-cats")
        if cats:
            schwerpunkte_spans = cats.select("span.abovefooter")
            extras = []
            for sp in schwerpunkte_spans:
                text = sp.get_text(strip=True)
                if "Plastische" not in text and text:
                    extras.append(text)
            if extras:
                doctor["schwerpunkte"] = ", ".join(extras)

        return doctor

    # ── BW (arztsuche-bw.de — static HTML GET) ──────────────────────

    def _scrape_bw(self, kammer: dict):
        """Scrape Baden-Württemberg via arztsuche-bw.de — paginated GET form.

        Uses id_fachgruppe=425 (Plastische und Ästhetische Chirurgie) — ~51 results
        in only 3 pages, much faster than the old approach of paging through all
        1821 Chirurgie (420) results.
        """
        base_url = "https://www.arztsuche-bw.de/index.php"
        MAX_BW_PAGES = 10  # Safety limit (51 results / 20 per page = 3 pages)
        consecutive_failures = 0

        # Resume from last successful offset
        # Use new progress key since we switched from fachgruppe 420 to 425
        saved_offset, completed = self.get_progress("bw_425")
        if completed:
            self.logger.info("  BW: already completed, skipping")
            return
        offset = saved_offset
        if offset > 0:
            self.logger.info(f"  BW: resuming from offset {offset} (page {offset // 20 + 1})")

        while offset < MAX_BW_PAGES * 20:
            params = {
                "suchen": "1",
                "arztgruppe": "facharzt",
                "id_fachgruppe": "425",  # Plastische und Ästhetische Chirurgie
                "offset": offset,
            }

            resp = self.fetch(base_url, params=params)
            if not resp:
                consecutive_failures += 1
                if consecutive_failures >= 3:
                    self.logger.warning(f"  BW: 3 consecutive failures at offset {offset}, stopping (will retry next run)")
                    return False  # Don't mark as completed
                # Wait longer before retry
                self.logger.info(f"  BW: retry after failure (attempt {consecutive_failures}/3), waiting 10s...")
                time.sleep(10)
                continue

            consecutive_failures = 0
            soup = BeautifulSoup(resp.text, "lxml")
            rows = soup.select("li.resultrow")
            if not rows:
                break

            self.logger.info(f"  BW page {offset // 20 + 1}: {len(rows)} rows")

            for row in rows:
                doctor = self._parse_bw_row(row)
                if doctor:
                    self._process_doctor(doctor, kammer)

            # Save progress after each successful page
            offset += 20
            self.save_progress("bw_425", offset)

            if len(rows) < 20:
                break
            # Delay between BW pages (only ~3 pages with fachgruppe 425)
            time.sleep(3 + random.random() * 2)

        # Mark BW as completed
        self.save_progress("bw_425", offset, completed=True)
        self.logger.info(f"  BW: completed (final offset {offset})")
        return True

    def _parse_bw_row(self, row) -> dict | None:
        """Parse a BW arztsuche result row."""
        # Check qualifications for Plastische/Ästhetische
        qual_el = row.select_one("dd.qualifikation")
        if not qual_el:
            return None
        qual_text = qual_el.get_text(" ", strip=True)
        if not any(kw in qual_text.lower() for kw in ["plastisch", "ästhetisch"]):
            return None

        # Extract name
        name_el = row.select_one("dd.name dt")
        if not name_el:
            return None
        name_text = name_el.get_text(strip=True)
        name_data = self._extract_name_from_text(name_text)
        if not name_data:
            return None

        doctor = {**name_data}

        # Extract Facharzttitel from qualifications
        fach_dt = qual_el.select_one("dt")
        fach_dd = qual_el.select_one("dd")
        if fach_dd:
            doctor["facharzttitel"] = fach_dd.get_text(strip=True)

        # Address
        addr_el = row.select_one("dd.adresse p.anschrift-arzt")
        if addr_el:
            addr_text = addr_el.get_text("\n", strip=True)
            lines = [l.strip() for l in addr_text.split("\n") if l.strip()]
            # Last line before "Landkreis:" is PLZ Stadt
            for line in lines:
                plz_match = re.search(r"(\d{5})\s+(.+)", line)
                if plz_match:
                    doctor["plz"] = plz_match.group(1)
                    doctor["stadt"] = plz_match.group(2).strip()
            if len(lines) >= 3:
                doctor["strasse"] = lines[-2] if "Landkreis" not in lines[-2] else lines[-3] if len(lines) >= 4 else None

        # Phone
        phone_el = row.select_one("dd.adresse dd")
        if phone_el:
            phone_text = phone_el.get_text(strip=True)
            tel_match = re.search(r"Telefon:\s*([\d/\-\s]+)", phone_text)
            if tel_match:
                doctor["telefon"] = tel_match.group(1).strip()

        return doctor

    # ── DGPRÄC (nationwide) ─────────────────────────────────────────

    # PLZ prefix → Bundesland mapping (approximate)
    PLZ_TO_BUNDESLAND = {
        "01": "Sachsen", "02": "Sachsen", "03": "Brandenburg", "04": "Sachsen",
        "06": "Sachsen-Anhalt", "07": "Thüringen", "08": "Sachsen", "09": "Sachsen",
        "10": "Berlin", "12": "Berlin", "13": "Berlin", "14": "Brandenburg",
        "15": "Brandenburg", "16": "Brandenburg", "17": "Mecklenburg-Vorpommern",
        "18": "Mecklenburg-Vorpommern", "19": "Mecklenburg-Vorpommern",
        "20": "Hamburg", "21": "Niedersachsen", "22": "Hamburg", "23": "Schleswig-Holstein",
        "24": "Schleswig-Holstein", "25": "Schleswig-Holstein", "26": "Niedersachsen",
        "27": "Niedersachsen", "28": "Bremen", "29": "Niedersachsen",
        "30": "Niedersachsen", "31": "Niedersachsen", "32": "Nordrhein-Westfalen",
        "33": "Nordrhein-Westfalen", "34": "Hessen", "35": "Hessen",
        "36": "Hessen", "37": "Niedersachsen", "38": "Niedersachsen",
        "39": "Sachsen-Anhalt",
        "40": "Nordrhein-Westfalen", "41": "Nordrhein-Westfalen",
        "42": "Nordrhein-Westfalen", "44": "Nordrhein-Westfalen",
        "45": "Nordrhein-Westfalen", "46": "Nordrhein-Westfalen",
        "47": "Nordrhein-Westfalen", "48": "Nordrhein-Westfalen",
        "49": "Niedersachsen",
        "50": "Nordrhein-Westfalen", "51": "Nordrhein-Westfalen",
        "52": "Nordrhein-Westfalen", "53": "Nordrhein-Westfalen",
        "54": "Rheinland-Pfalz", "55": "Rheinland-Pfalz",
        "56": "Rheinland-Pfalz", "57": "Nordrhein-Westfalen",
        "58": "Nordrhein-Westfalen", "59": "Nordrhein-Westfalen",
        "60": "Hessen", "61": "Hessen", "63": "Hessen", "64": "Hessen",
        "65": "Hessen", "66": "Saarland", "67": "Rheinland-Pfalz",
        "68": "Baden-Württemberg", "69": "Baden-Württemberg",
        "70": "Baden-Württemberg", "71": "Baden-Württemberg",
        "72": "Baden-Württemberg", "73": "Baden-Württemberg",
        "74": "Baden-Württemberg", "75": "Baden-Württemberg",
        "76": "Baden-Württemberg", "77": "Baden-Württemberg",
        "78": "Baden-Württemberg", "79": "Baden-Württemberg",
        "80": "Bayern", "81": "Bayern", "82": "Bayern", "83": "Bayern",
        "84": "Bayern", "85": "Bayern", "86": "Bayern", "87": "Bayern",
        "88": "Baden-Württemberg", "89": "Bayern",
        "90": "Bayern", "91": "Bayern", "92": "Bayern", "93": "Bayern",
        "94": "Bayern", "95": "Bayern", "96": "Bayern", "97": "Bayern",
        "98": "Thüringen", "99": "Thüringen",
    }

    def _scrape_dgpraec(self):
        """Scrape DGPRÄC Arztsuche — 400+ plastic surgeons across all of Germany."""
        import requests as _requests

        _, completed = self.get_progress("dgpraec")
        if completed:
            self.logger.info("DGPRÄC: already completed, skipping")
            return

        self.logger.info("Scraping DGPRÄC Arztsuche (nationwide)...")

        try:
            resp = _requests.post(
                "https://www.dgpraec.de/patienten/arztsuche/",
                data={"submit": "1", "doctor": "", "place": "", "radius": ""},
                headers={"User-Agent": "Mozilla/5.0"},
                timeout=30,
            )
            resp.raise_for_status()
        except Exception as e:
            self.logger.error(f"  DGPRÄC failed: {e}")
            return

        soup = BeautifulSoup(resp.text, "lxml")
        rows = soup.select(".content_row")
        self.logger.info(f"  DGPRÄC returned {len(rows)} rows")

        count = 0
        for row in rows:
            try:
                doctor = self._parse_dgpraec_row(row)
                if doctor:
                    plz = doctor.get("plz", "")
                    bundesland = self.PLZ_TO_BUNDESLAND.get(plz[:2], "") if plz else ""
                    kammer_info = {
                        "name": "DGPRÄC",
                        "bundesland": bundesland,
                        "search_url": "https://www.dgpraec.de/patienten/arztsuche/",
                    }
                    self._process_doctor(doctor, kammer_info)
                    count += 1
            except Exception as e:
                self.logger.error(f"  Failed parsing DGPRÄC row: {e}")

        self.save_progress("dgpraec", count, completed=True)
        self.logger.info(f"  DGPRÄC: processed {count} doctors")

    def _parse_dgpraec_row(self, row) -> dict | None:
        """Parse a DGPRÄC content_row element."""
        strong = row.select_one("strong")
        if not strong:
            return None

        name_text = re.sub(r"\s+", " ", strong.get_text(strip=True)).strip()
        if not name_text or len(name_text) < 4:
            return None

        name_data = self._extract_name_from_text(name_text)
        if not name_data:
            return None

        doctor = {**name_data}
        doctor["facharzttitel"] = "Plastische und Ästhetische Chirurgie"

        # Address from second col-sm-5
        cols = row.select(".col-sm-5")
        if len(cols) > 1:
            addr_parts = [l.strip() for l in cols[1].get_text("\n").split("\n") if l.strip()]
            if addr_parts:
                doctor["strasse"] = addr_parts[0]
            if len(addr_parts) > 1:
                plz_match = re.match(r"(\d{5})\s*(.*)", addr_parts[1])
                if plz_match:
                    doctor["plz"] = plz_match.group(1)
                    doctor["stadt"] = plz_match.group(2).strip()
                else:
                    doctor["stadt"] = addr_parts[1]

        # Skip non-German entries (no 5-digit PLZ)
        if not doctor.get("plz") or not re.match(r"\d{5}$", doctor.get("plz", "")):
            return None

        return doctor

    # ── JS-rendered Kammern (Playwright) ──────────────────────────────

    def _scrape_kammer_js(self, kammer: dict):
        """Scrape a JS-rendered Kammer using Playwright."""
        if not self._init_browser():
            self.logger.warning(f"  Skipping {kammer['name']} (Playwright unavailable)")
            return

        search_url = kammer["search_url"]
        self.logger.info(f"  Loading {search_url} with Playwright...")

        try:
            self._page.goto(search_url, timeout=30000)
            self._page.wait_for_load_state("networkidle", timeout=15000)
        except Exception as e:
            self.logger.error(f"  Page load failed: {e}")
            return

        # Kammer-specific JS scrapers (legacy — AEKHH and AEKSL now use API)
        if kammer["kuerzel"] == "AEKHH":
            self._scrape_hamburg_js(kammer)
        elif kammer["kuerzel"] == "AEKSL":
            self._scrape_saarland_js(kammer)
        else:
            # Generic JS scraping
            self._scrape_js_generic(kammer)

    def _scrape_hamburg_js(self, kammer: dict):
        """Scrape Hamburg Aerztekammer via Playwright (legacy — now using API)."""
        # Find all dropdown options matching plastic/aesthetic surgery
        plastisch_options = self._page.evaluate("""() => {
            const sel = document.querySelector("select[name='l-area'], #l-area");
            if (!sel) return [];
            const seen = new Set();
            return Array.from(sel.options)
                .filter(o => o.textContent.toLowerCase().includes('plastisch'))
                .filter(o => { if (seen.has(o.value)) return false; seen.add(o.value); return true; })
                .map(o => ({value: o.value, text: o.textContent.trim()}));
        }""")

        if not plastisch_options:
            self.logger.warning("  No 'Plastisch' dropdown options found in Hamburg")
            return

        for opt in plastisch_options:
            self.logger.info(f"  Searching Hamburg: {opt['text']} ({opt['value']})")
            try:
                self._hamburg_search_and_parse(opt["value"], kammer)
            except Exception as e:
                self.logger.error(f"  Hamburg search failed for {opt['text']}: {e}")
            self.wait()

    def _hamburg_search_and_parse(self, fachgebiet_value: str, kammer: dict):
        """Select a Fachgebiet, submit, and parse results."""
        # Set dropdown and trigger change
        self._page.evaluate("""(value) => {
            const sel = document.querySelector("select[name='l-area'], #l-area");
            if (sel) {
                sel.value = value;
                sel.dispatchEvent(new Event('change', {bubbles: true}));
            }
        }""", fachgebiet_value)
        self._page.wait_for_timeout(2000)

        # Click the submit button in the arztsuche form (not the WP search form)
        self._page.evaluate("""() => {
            const sel = document.querySelector("select[name='l-area'], #l-area");
            if (sel) {
                const form = sel.closest('form');
                if (form) {
                    const btn = form.querySelector("button[type='submit'], button");
                    if (btn) btn.click();
                }
            }
        }""")

        self._page.wait_for_timeout(4000)
        try:
            self._page.wait_for_load_state("networkidle", timeout=10000)
        except Exception:
            pass

        # Parse results from #results--doctors
        items = self._page.query_selector_all("#results--doctors .item.margin-bottom")
        self.logger.info(f"  Found {len(items)} doctors")

        for item in items:
            try:
                doctor = self._parse_hamburg_item(item)
                if doctor:
                    self._process_doctor(doctor, kammer)
            except Exception as e:
                self.logger.error(f"  Failed parsing Hamburg item: {e}")

    def _parse_hamburg_item(self, item) -> dict | None:
        """Parse a Hamburg Aerztekammer structured doctor entry."""
        # Extract name from header, excluding <small> tags
        header_text = item.evaluate("""(el) => {
            const flex1 = el.querySelector('.item__header .flex1');
            if (!flex1) return '';
            const clone = flex1.cloneNode(true);
            clone.querySelectorAll('small').forEach(s => s.remove());
            return clone.textContent.trim();
        }""")
        if not header_text or len(header_text) < 4:
            return None
        # Remove type designations like (P), (D) at end
        header_text = re.sub(r"\s*\(?[PD]\)?\s*$", "", header_text).strip()

        name_data = self._parse_hamburg_name(header_text)
        if not name_data:
            name_data = self._extract_name_from_text(header_text)
        if not name_data:
            return None

        doctor = {**name_data}

        # Parse the 4 columns in item__inner
        columns = item.query_selector_all(".flex__item, .item__inner .flex > div")

        for col in columns:
            strong = col.query_selector("strong")
            if not strong:
                continue
            label = strong.inner_text().strip().lower()
            content_text = col.inner_text().replace(strong.inner_text(), "").strip()

            if "fachgebiet" in label:
                # "Ärztliche Fachgebiete & Schwerpunkte" — extract the <p> text
                p_el = col.query_selector("p")
                if p_el:
                    doctor["facharzttitel"] = p_el.inner_text().strip()
                else:
                    doctor["facharzttitel"] = content_text.strip()
            elif "schwerpunkt" in label or "zusätz" in label or "zusatz" in label:
                p_el = col.query_selector("p")
                doctor["schwerpunkte"] = (p_el.inner_text().strip() if p_el else content_text.strip())
            elif "anschrift" in label:
                # Address parsing
                lines = [l.strip() for l in content_text.split("\n") if l.strip()]
                if lines:
                    doctor["strasse"] = lines[0] if len(lines) > 1 else None
                    # Last line should be "PLZ Stadt"
                    addr_line = lines[-1] if lines else ""
                    plz_match = re.search(r"(\d{5})\s+(.+)", addr_line)
                    if plz_match:
                        doctor["plz"] = plz_match.group(1)
                        doctor["stadt"] = plz_match.group(2).strip()
            elif "kontakt" in label:
                # Extract phone, fax, email, website from contact section
                tel_el = col.query_selector("a[href^='tel:']")
                if tel_el:
                    doctor["telefon"] = tel_el.inner_text().strip()

                email_el = col.query_selector("a[href^='mailto:']")
                if email_el:
                    doctor["email"] = email_el.inner_text().strip()

                web_el = col.query_selector("a[target='_blank']")
                if web_el:
                    href = web_el.get_attribute("href") or ""
                    if href and "mailto:" not in href and "tel:" not in href:
                        doctor["website_url"] = href

                # Fax from text
                fax_match = re.search(r"Fax:\s*([\d\s/\-]+)", col.inner_text())
                if fax_match:
                    doctor["fax"] = fax_match.group(1).strip()

        return doctor

    # ── Saarland (legacy JS) ────────────────────────────────────────

    def _scrape_saarland_js(self, kammer: dict):
        """Scrape Saarland Aerztekammer via Playwright (legacy — now using API)."""
        # Select "Plastische und Ästhetische Chirurgie" from Fachgebiet dropdown
        try:
            self._page.select_option("select[name='s']", label="Plastische und Ästhetische Chirurgie")
            self._page.wait_for_timeout(1000)
            self._page.click("#searchbtn")
            self._page.wait_for_timeout(3000)
            try:
                self._page.wait_for_load_state("networkidle", timeout=10000)
            except Exception:
                pass
        except Exception as e:
            self.logger.error(f"  Saarland form submission failed: {e}")
            return

        # Parse results — each doctor is article.entry.leftpad.single
        items = self._page.query_selector_all("article.entry.leftpad.single")
        self.logger.info(f"  Found {len(items)} doctors in Saarland")

        for item in items:
            try:
                doctor = self._parse_saarland_item(item)
                if doctor:
                    self._process_doctor(doctor, kammer)
            except Exception as e:
                self.logger.error(f"  Failed parsing Saarland item: {e}")

    def _parse_saarland_item(self, item) -> dict | None:
        """Parse a Saarland article.entry doctor element."""
        # Name from h2.entry-title (may include "Dr. med." prefix)
        title_el = item.query_selector("h2.entry-title")
        if not title_el:
            return None
        name_text = title_el.inner_text().strip()
        if not name_text or len(name_text) < 4:
            return None

        name_data = self._extract_name_from_text(name_text)
        if not name_data:
            return None

        doctor = {**name_data}

        # Practice name from <b> tag
        # Address from first col-md-6
        addr_col = item.query_selector(".entry-content .col-md-6:first-child")
        if addr_col:
            lines = [l.strip() for l in addr_col.inner_text().split("\n") if l.strip()]
            # First line is practice name (bold), then street, then PLZ+city
            if len(lines) >= 2:
                doctor["strasse"] = lines[-2] if len(lines) >= 3 else None
                import re as _re
                plz_match = _re.search(r"(\d{5})\s+(.+)", lines[-1])
                if plz_match:
                    doctor["plz"] = plz_match.group(1)
                    doctor["stadt"] = plz_match.group(2).strip()

        # Contact from second col-md-6
        contact_col = item.query_selector(".entry-content .col-md-6:nth-child(2)")
        if contact_col:
            text = contact_col.inner_text()
            tel_match = re.search(r"Telefon:\s*([\d\-/\s]+)", text)
            if tel_match:
                doctor["telefon"] = tel_match.group(1).strip()

            email_el = contact_col.query_selector("a[href^='mailto:']")
            if email_el:
                doctor["email"] = email_el.inner_text().strip()

            web_el = contact_col.query_selector("a[target='_blank']")
            if web_el:
                href = web_el.get_attribute("href") or ""
                if href and "mailto:" not in href:
                    doctor["website_url"] = href

        # Fachgebiet and Schwerpunkte from span.entry-cats
        cats = item.query_selector("span.entry-cats")
        if cats:
            cats_text = cats.inner_text()
            fach_match = re.search(r"Fachgebiet:\s*(.+?)(?:\n|$)", cats_text)
            if fach_match:
                doctor["facharzttitel"] = fach_match.group(1).strip()
            schwer_match = re.search(r"Zusätze und Schwerpunkte:\s*(.+?)(?:\n|$)", cats_text)
            if schwer_match:
                doctor["schwerpunkte"] = schwer_match.group(1).strip()

        return doctor

    def _scrape_js_generic(self, kammer: dict):
        """Generic Playwright scraping for other JS-rendered Kammern."""
        html = self._page.content()
        soup = BeautifulSoup(html, "lxml")
        doctors = self._extract_doctors(soup, kammer)
        self.logger.info(f"  Found {len(doctors)} doctors via Playwright")
        for doctor in doctors:
            self._process_doctor(doctor, kammer)

    # ── Static HTML Kammern (requests) ────────────────────────────────

    def _scrape_kammer(self, kammer: dict):
        """Scrape a single Landesaerztekammer's Arztsuche (static HTML)."""
        if kammer["method"] == "POST":
            resp = self._post_search(kammer)
        else:
            resp = self.fetch(kammer["search_url"], params=kammer["params"])

        if not resp:
            self.logger.warning(f"  No response from {kammer['name']}")
            return

        soup = BeautifulSoup(resp.text, "lxml")
        doctors = self._extract_doctors(soup, kammer)
        self.logger.info(f"  Found {len(doctors)} doctors from {kammer['name']}")

        for doctor in doctors:
            self._process_doctor(doctor, kammer)

        # Handle pagination
        if kammer.get("pagination"):
            page = 2
            while page <= self.MAX_PAGES:
                params = {**kammer["params"], kammer["pagination"]: page}
                resp = self.fetch(kammer["search_url"], params=params)
                if not resp:
                    break
                soup = BeautifulSoup(resp.text, "lxml")
                next_doctors = self._extract_doctors(soup, kammer)
                if not next_doctors:
                    break
                self.logger.info(f"  Page {page}: {len(next_doctors)} doctors")
                for doctor in next_doctors:
                    self._process_doctor(doctor, kammer)
                page += 1
                self.wait()
            if page > self.MAX_PAGES:
                self.logger.warning(f"  Hit max pages ({self.MAX_PAGES}) for {kammer['name']} -- likely unfiltered results")

    def _post_search(self, kammer: dict):
        """Submit a POST search form."""
        try:
            self.session.headers["Content-Type"] = "application/x-www-form-urlencoded"
            resp = self.session.post(
                kammer["search_url"],
                data=kammer["params"],
                timeout=30,
            )
            resp.raise_for_status()
            return resp
        except Exception as e:
            self.logger.error(f"  POST failed for {kammer['name']}: {e}")
            return None

    # ── HTML parsing (static pages) ───────────────────────────────────

    def _extract_doctors(self, soup: BeautifulSoup, kammer: dict) -> list[dict]:
        """Extract doctor entries from a Kammer search results page."""
        doctors = []

        # Try configured selectors
        selectors = kammer["result_selector"].split(", ")
        result_elements = []
        for sel in selectors:
            result_elements = soup.select(sel)
            if result_elements:
                break

        if not result_elements:
            result_elements = self._find_doctor_elements(soup)

        for element in result_elements:
            doctor = self._parse_doctor_element(element, kammer)
            if doctor:
                doctors.append(doctor)

        return doctors

    def _find_doctor_elements(self, soup: BeautifulSoup) -> list:
        """Fallback: find doctor entries by heuristic patterns."""
        elements = []

        for el in soup.find_all(["div", "article", "li"], class_=re.compile(
            r"arzt|doctor|result|member|entry|item", re.I
        )):
            text = el.get_text(" ", strip=True)
            if any(kw in text.lower() for kw in ["dr.", "prof.", "facharzt", "plastisch"]):
                elements.append(el)

        if not elements:
            for row in soup.find_all("tr"):
                text = row.get_text(" ", strip=True)
                if any(kw in text.lower() for kw in ["dr.", "prof.", "plastisch"]):
                    elements.append(row)

        return elements

    def _parse_doctor_element(self, element, kammer: dict) -> dict | None:
        """Parse a single doctor entry from an HTML element."""
        text = element.get_text(" ", strip=True)
        if not text or len(text) < 8:
            return None

        name_data = self._extract_name_from_text(text)
        if not name_data:
            return None

        doctor = {
            **name_data,
            "facharzttitel": self._extract_facharzttitel(text),
            "kammer_mitgliedsnr": self._extract_mitgliedsnr(text),
        }

        plz, stadt = self._extract_address(text)
        doctor["plz"] = plz
        doctor["stadt"] = stadt

        # Extract contact info from text
        phone = self._extract_phone(text)
        if phone:
            doctor["telefon"] = phone

        email = self._extract_email(text)
        if email:
            doctor["email"] = email

        # Extract website from links
        website = self._extract_website(element)
        if website:
            doctor["website_url"] = website

        return doctor

    def _extract_phone(self, text: str) -> str | None:
        """Extract German phone number from text."""
        m = re.search(r"(?:Tel(?:efon)?|Fon)[.:]\s*([\d\s/\-()]+\d)", text, re.I)
        if m:
            return m.group(1).strip()
        # Generic German phone pattern
        m = re.search(r"\b(0\d{2,4}[\s/\-]?\d{3,8})\b", text)
        if m:
            return m.group(1).strip()
        return None

    def _extract_email(self, text: str) -> str | None:
        """Extract email address from text."""
        m = re.search(r"[\w.+-]+@[\w-]+\.[\w.-]+", text)
        if m:
            return m.group(0)
        return None

    def _extract_website(self, element) -> str | None:
        """Extract website URL from HTML links."""
        if hasattr(element, "find_all"):
            for a in element.find_all("a", href=True):
                href = a["href"]
                if href.startswith("http") and "mailto:" not in href and "tel:" not in href:
                    if any(kw in href for kw in [".de", ".com", ".org", ".net", ".eu"]):
                        return href
        return None

    # ── Name parsing ──────────────────────────────────────────────────

    SKIP_WORDS = {
        "name", "ärztin", "arzt", "ärztliche", "fachgebiete", "gefundene",
        "ärztinnen", "suche", "ergebnis", "ergebnisse", "treffer", "seite",
        "sortierung", "filter", "zurück", "weiter", "anzeigen",
    }

    def _parse_hamburg_name(self, text: str) -> dict | None:
        """Parse Hamburg-format name: 'Nachname, Vorname , Titel' with edge cases.

        Handles: 'von Wild, Tobias , Dr. med.', 'Kolios, MBA, Georgios , Dr. med.',
                 'Orellana Oropin De Castro, Greysy', 'Tobbia, M.D., Dalia'
        """
        if "," not in text:
            return None

        # Split on the LAST comma that precedes a title (Dr/Prof) or end of string
        # Strategy: everything before first comma = nachname, then skip qualifications,
        # find the vorname, and collect the titel
        parts = [p.strip() for p in text.split(",")]
        if len(parts) < 2:
            return None

        nachname = parts[0].strip()
        # Skip parts that are qualifications (MBA, M.D., etc.) to find vorname
        titel_kw = {"dr", "dr.", "med", "med.", "prof", "prof.", "pd", "priv.-doz",
                     "priv.-doz.", "univ", "univ.", "dent", "dent.", "habil", "habil.",
                     "mba", "m.d.", "m.d", "ph.d.", "ph.d", "msc", "m.sc."}
        vorname = None
        titel_parts = []
        for part in parts[1:]:
            part = part.strip()
            if not part:
                continue
            # Check if this part is a qualification/title
            if part.lower().rstrip(".") in titel_kw or part.lower() in titel_kw:
                titel_parts.append(part)
            elif re.match(r"^(?:Prof|Dr|med|PD|Priv)[.\s-]", part):
                titel_parts.append(part)
            elif vorname is None and len(part) >= 2 and part[0].isalpha():
                vorname = part
            else:
                titel_parts.append(part)

        if not vorname or len(vorname) < 2 or len(nachname) < 2:
            return None

        return {
            "vorname": vorname,
            "nachname": nachname,
            "titel": " ".join(titel_parts).strip().rstrip(",. "),
        }

    # University names that appear after "Univ." and should be part of the title
    UNIVERSITY_NAMES = {
        "semmelweis", "budapest", "wien", "graz", "innsbruck", "zürich", "zurich",
        "basel", "bern", "heidelberg", "münchen", "berlin", "hamburg", "köln",
        "freiburg", "tübingen", "göttingen", "erlangen", "würzburg", "mainz",
        "bonn", "marburg", "giessen", "rostock", "jena", "leipzig", "dresden",
        "pécs", "szeged", "debrecen", "bratislava", "prag", "praha", "brno",
    }

    def _extract_name_from_text(self, text: str) -> dict | None:
        """Extract name components from text."""
        text = text.strip()
        if len(text) < 4:
            return None

        # Normalize compound title tokens: "Dr.med." -> "Dr. med."
        text = re.sub(r"(?i)\bDr\.med\.", "Dr. med.", text)
        text = re.sub(r"(?i)\bDr\.dent\.", "Dr. dent.", text)
        text = re.sub(r"(?i)\bUniv\.-Prof\.", "Univ.-Prof.", text)
        # Strip stray parentheses and their content before name (e.g. "Semmelweis)")
        text = re.sub(r"[()]+", " ", text)
        text = re.sub(r"\s+", " ", text).strip()

        first_words = {w.lower().rstrip(".,:/") for w in text.split()[:4]}
        if first_words & self.SKIP_WORDS:
            return None

        titel_kw = {
            "prof", "prof.", "dr", "dr.", "med", "med.", "pd",
            "priv.-doz", "priv.-doz.", "priv", "priv.",
            "univ.-prof", "univ.-prof.", "univ", "univ.",
            "doz", "doz.", "dent", "dent.",
            "habil", "habil.", "dipl.-med", "dipl.-med.",
        }

        # "Nachname, Vorname, Titel" pattern
        comma_match = re.match(
            r"^([A-ZÄÖÜ][a-zäöüß]+(?:-[A-ZÄÖÜ][a-zäöüß]+)?),\s*"
            r"([A-ZÄÖÜ][a-zäöüß]+(?:\s+[A-ZÄÖÜ][a-zäöüß]+)?),?\s*"
            r"((?:(?:Prof|Dr|med|PD|Priv)[.\s-]*)*)?",
            text,
        )
        if comma_match:
            nachname = comma_match.group(1).strip()
            vorname = comma_match.group(2).strip()
            titel = (comma_match.group(3) or "").strip().rstrip(",. ")
            if len(vorname) >= 2 and len(nachname) >= 2:
                return {"vorname": vorname, "nachname": nachname, "titel": titel}

        # "Titel Vorname Nachname" format
        titel_parts = []
        name_parts = []

        words = text.split()
        in_name = False
        last_was_univ = False  # Track if previous word was "Univ." to catch university names
        for word in words:
            clean = word.lower().rstrip(".,")
            if not in_name and clean in titel_kw:
                titel_parts.append(word)
                last_was_univ = clean in ("univ", "univ.")
            elif not in_name and last_was_univ and clean in self.UNIVERSITY_NAMES:
                # University name after "Univ." — treat as part of title
                titel_parts.append(word)
                last_was_univ = False
            elif not in_name and word == ",":
                last_was_univ = False
                continue
            else:
                in_name = True
                last_was_univ = False
                if any(c.isdigit() for c in word) or word in (",", "|", "-", "\u2013", "\u2022", "/"):
                    break
                if len(word) > 1 and word[0].isupper():
                    name_parts.append(word)
                elif name_parts:
                    break

        if len(name_parts) < 2:
            return None

        # Reject if name parts are too short (likely parsing artifacts)
        vorname = name_parts[0]
        nachname = " ".join(name_parts[1:])
        if len(vorname) < 2 or len(nachname) < 2:
            return None

        return {
            "vorname": vorname,
            "nachname": nachname,
            "titel": " ".join(titel_parts),
        }

    def _extract_facharzttitel(self, text: str) -> str | None:
        """Extract Facharzttitel from text."""
        patterns = [
            r"Facharzt\s+f[uü]r\s+([^,\n]+)",
            r"Fach[aä]rztin\s+f[uü]r\s+([^,\n]+)",
            r"FA\s+f[uü]r\s+([^,\n]+)",
            r"F[AÄ]\s+f[uü]r\s+([^,\n]+)",
            r"(Plastische\s+(?:und\s+)?(?:[AÄ]sthetische\s+)?(?:und\s+Rekonstruktive\s+)?Chirurgie)",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, re.I)
            if m:
                return m.group(1).strip().rstrip(".")
        return None

    def _extract_address(self, text: str) -> tuple[str | None, str | None]:
        """Extract PLZ and city from text."""
        m = re.search(r"\b(\d{5})\s+([A-ZÄÖÜ][a-zäöüß]+(?:\s+[a-zäöüß]+)?)", text)
        if m:
            return m.group(1), m.group(2).strip()
        return None, None

    def _extract_mitgliedsnr(self, text: str) -> str | None:
        """Try to extract a Kammer membership number."""
        m = re.search(r"(?:Mitgl|Nr|ID)[.:]\s*(\d{4,10})", text, re.I)
        if m:
            return m.group(1)
        return None

    # ── Processing / upsert ───────────────────────────────────────────

    GARBAGE_NAMES = {
        "name", "ärztin", "arzt", "ärztliche", "fachgebiete", "gefundene",
        "ärztinnen", "ergebnis", "ergebnisse", "suche", "treffer",
        "sortierung", "filter", "anzeigen", "seite",
    }

    def _process_doctor(self, doctor: dict, kammer: dict):
        """Upsert a doctor from an Aerztekammer source."""
        # Reject garbage names
        all_name_words = {w.lower().rstrip(".,:/") for w in
                         f"{doctor['vorname']} {doctor['nachname']}".split()}
        if all_name_words & self.GARBAGE_NAMES:
            self.logger.debug(f"  Skipped garbage name: {doctor['vorname']} {doctor['nachname']}")
            return

        # Only keep doctors with relevant specialty
        fach = (doctor.get("facharzttitel") or "").lower()
        schwerpunkte = (doctor.get("schwerpunkte") or "").lower()
        combined = fach + " " + schwerpunkte
        if not any(kw in combined for kw in self.RELEVANT_KEYWORDS):
            self.logger.debug(f"  Skipped (no relevant Facharzttitel): {doctor['vorname']} {doctor['nachname']}")
            return

        # Dedup by name only (ignore title differences between sources)
        dedup_key = f"{doctor['vorname'].lower().strip()}|{doctor['nachname'].lower().strip()}"
        if dedup_key in self.seen_slugs:
            return
        self.seen_slugs.add(dedup_key)

        arzt_data = {
            "vorname": doctor["vorname"],
            "nachname": doctor["nachname"],
            "titel": doctor.get("titel", ""),
            "geschlecht": doctor.get("geschlecht"),
            "ist_facharzt": True,
            "facharzttitel": doctor.get("facharzttitel"),
            "selbstbezeichnung": doctor.get("facharzttitel") or "Facharzt",
            "approbation_verifiziert": True,
            "land": "DE",
            "stadt": doctor.get("stadt"),
            "bundesland": kammer["bundesland"],
            "plz": doctor.get("plz"),
            "strasse": doctor.get("strasse"),
            "telefon": doctor.get("telefon"),
            "email": doctor.get("email"),
            "fax": doctor.get("fax"),
            "website_url": doctor.get("website_url"),
            "schwerpunkte": doctor.get("schwerpunkte"),
            "latitude": doctor.get("latitude"),
            "longitude": doctor.get("longitude"),
            "datenquelle": "aerztekammer_de",
            "quelle_url": kammer["search_url"],
            "kammer_region": kammer["name"],
            "kammer_mitgliedsnr": doctor.get("kammer_mitgliedsnr"),
            "verified": True,
            "source": "aerztekammer_de",
            "source_type": "official",
            "last_verified_at": datetime.now(timezone.utc).isoformat(),
        }

        arzt_id = self.upsert_arzt(arzt_data)
        if not arzt_id:
            return

        if doctor.get("facharzttitel"):
            kategorie = self._map_kategorie(doctor["facharzttitel"])
            self.insert_spezialisierungen(arzt_id, [{
                "kategorie": kategorie,
                "eingriff": doctor["facharzttitel"],
                "erfahrungslevel": "spezialist",
            }])

        self.logger.info(
            f"  Processed: {doctor.get('titel', '')} {doctor['vorname']} {doctor['nachname']}"
            f" | {doctor.get('facharzttitel', 'N/A')}"
            f" | Tel: {doctor.get('telefon', '-')}"
            f" | Web: {doctor.get('website_url', '-')}"
        )

    def _map_kategorie(self, facharzttitel: str) -> str:
        """Map Facharzttitel to spezialisierungen kategorie."""
        s = facharzttitel.lower()
        if "plastisch" in s or "ästhetisch" in s or "aesthetisch" in s:
            return "koerper"
        if "haut" in s or "dermatolog" in s:
            return "minimal_invasiv"
        if "mund" in s or "kiefer" in s or "gesicht" in s:
            return "gesicht"
        if "hals" in s or "nasen" in s or "ohren" in s or "hno" in s:
            return "gesicht"
        if "augen" in s or "ophthalmol" in s:
            return "gesicht"
        return "koerper"


if __name__ == "__main__":
    scraper = AerztekammerScraper()
    try:
        scraper.run()
    finally:
        scraper.close()
