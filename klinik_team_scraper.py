"""Scraper for clinic team pages — finds employed plastic surgeons.

Visits clinic team/doctor pages, extracts individual doctor profile URLs,
then scrapes each profile for name, title, and Facharzttitel.
Uses Playwright for JS-rendered pages.
Only creates records for doctors with a verified Plastische Chirurgie Facharzttitel.
"""

import re
import time
import random
from urllib.parse import urljoin, urlparse
from base_scraper import BaseScraper

# Clinics to scrape — team_url is the page listing all doctors
KLINIKEN = [
    {"name": "Dorow Clinic", "team_url": "https://dorow-clinic.de/aerzte/", "stadt": "Waldshut-Tiengen", "bundesland": "Baden-Württemberg"},
    {"name": "Arteo Klinik", "team_url": "https://arteo-klinik.de/unser-team/fachaerzte/", "stadt": "Düsseldorf", "bundesland": "Nordrhein-Westfalen"},
    {"name": "ATOS Klinik München", "team_url": "https://atos-kliniken.com/de/muenchen/unsere-aerzte/", "stadt": "München", "bundesland": "Bayern"},
    {"name": "ATOS Klinik Heidelberg", "team_url": "https://atos-kliniken.com/de/heidelberg/unsere-aerzte/", "stadt": "Heidelberg", "bundesland": "Baden-Württemberg"},
    {"name": "Noahklinik", "team_url": "https://noahklinik.de/en/about-us/our-specialists/", "stadt": "Kassel", "bundesland": "Hessen"},
    {"name": "Clinic im Centrum", "team_url": "https://clinic-im-centrum.de/concept/fachaerzte/", "stadt": "Berlin", "bundesland": "Berlin"},
    {"name": "KÖ-Klinik", "team_url": "https://www.koe-klinik.de/koe-klinik/team/fachaerzte-der-koe-klinik.html", "stadt": "Düsseldorf", "bundesland": "Nordrhein-Westfalen"},
    {"name": "Lanuwa Aesthetik", "team_url": "https://lanuwa-klinik.de/aerzteteam/", "stadt": "Leipzig", "bundesland": "Sachsen"},
    {"name": "Fort Malakoff Klinik", "team_url": "https://www.malakoff-klinik.de/ueber-uns/unsere-fachaerzte/", "stadt": "Mainz", "bundesland": "Rheinland-Pfalz"},
    {"name": "Rosenpark Klinik", "team_url": "https://www.rosenparkklinik.de/en/clinic-team/physicians/", "stadt": "Darmstadt", "bundesland": "Hessen"},
    {"name": "Kaiserberg Klinik", "team_url": "https://kaiserberg-klinik.de/team/", "stadt": "Duisburg", "bundesland": "Nordrhein-Westfalen"},
    {"name": "Proaesthetic", "team_url": "https://www.proaesthetic.de/team/", "stadt": "Heidelberg", "bundesland": "Baden-Württemberg"},
    {"name": "Medical One", "team_url": "https://www.medical-one.de/fachaerzte/", "stadt": "Hamburg", "bundesland": "Hamburg"},
    {"name": "Alster-Klinik", "team_url": "https://alster-klinik.de/aerzte-team/", "stadt": "Hamburg", "bundesland": "Hamburg"},
    {"name": "Sinis Aesthetics", "team_url": "https://www.sinis-aesthetics.de/die-klinik/", "stadt": "Berlin", "bundesland": "Berlin"},
    {"name": "Bodenseeklinik", "team_url": "https://www.bodenseeklinik.de/schoenheitschirurg", "stadt": "Lindau", "bundesland": "Bayern"},
    {"name": "Sophienklinik Stuttgart", "team_url": "https://www.sophienklinik-stuttgart.de/klinik/aerzte/", "stadt": "Stuttgart", "bundesland": "Baden-Württemberg"},
    {"name": "ISAR Klinikum", "team_url": "https://plastische-chirurgie.isarklinikum.de/team/", "stadt": "München", "bundesland": "Bayern"},
    {"name": "Schlosspark Klinik", "team_url": "https://www.schlosspark-klinik.com/schlossparkklinik/team", "stadt": "Ludwigsburg", "bundesland": "Baden-Württemberg"},
    {"name": "Mannheimer Klinik", "team_url": "https://www.beautyclinic.de/das-team/", "stadt": "Mannheim", "bundesland": "Baden-Württemberg"},
]

# Patterns indicating a link points to a doctor profile
DOCTOR_LINK_PATTERNS = re.compile(
    r"(dr[.-]|prof[.-]|facharzt|fach.rztin|arzt|.rztin|/team/|/aerzte/|/doctors/|/physicians/|/specialists/)",
    re.IGNORECASE,
)

# Patterns for Facharzttitel in plastic surgery
FACHARZT_PATTERNS = [
    re.compile(r"Fach(?:arzt|ärztin)\s+für\s+Plastische", re.IGNORECASE),
    re.compile(r"Plastische\s+und\s+Ästhetische\s+Chirurgie", re.IGNORECASE),
    re.compile(r"Plastische\s+(?:&|und)\s+Ästhetische", re.IGNORECASE),
    re.compile(r"Plastic\s+(?:and|&)\s+Aesthetic\s+Surg", re.IGNORECASE),
    re.compile(r"Facharzt.*Plastische.*Chirurgie", re.IGNORECASE),
    re.compile(r"Plastische\s+Chirurgie", re.IGNORECASE),
]

# Title keywords
TITEL_KEYWORDS = {
    "prof", "prof.", "dr", "dr.", "med", "med.", "pd", "priv.-doz",
    "priv.-doz.", "univ", "univ.", "dent", "dent.", "dipl", "dipl.",
    "habil", "habil.", "msc", "m.sc.", "ph.d", "ph.d.", "mba",
}

# Domains to never follow
BLOCKED_DOMAINS = {
    "facebook.com", "instagram.com", "twitter.com", "x.com",
    "youtube.com", "tiktok.com", "linkedin.com", "pinterest.com",
    "google.com", "wa.me", "whatsapp.com", "mailto:",
}


def _parse_name(text: str) -> dict | None:
    """Parse a doctor name from heading text."""
    text = text.strip()
    # Remove common prefixes
    text = re.sub(r"^(Herr|Frau|Unser\w*|Meet|About)\s+", "", text, flags=re.IGNORECASE)
    # Remove trailing roles
    text = re.sub(r"\s*[-–|,]\s*(Oberarzt|Chefarzt|Leitend|Fach.rzt|Specialist|Director).*$", "", text, flags=re.IGNORECASE)

    words = text.split()
    titel_parts = []
    name_parts = []
    in_name = False

    for word in words:
        clean = word.lower().rstrip(".,")
        if not in_name and clean in TITEL_KEYWORDS:
            titel_parts.append(word)
        else:
            in_name = True
            # Stop at non-name content
            if any(c.isdigit() for c in word) or word in (",", "|", "–", "•", "/"):
                break
            name_parts.append(word)

    if len(name_parts) < 2:
        return None

    # Handle nobility particles
    nobility = {"von", "van", "de", "zu", "vom", "zum", "zur", "ten", "ter"}
    nachname_start = len(name_parts) - 1
    for i, part in enumerate(name_parts):
        if i > 0 and part.lower() in nobility:
            nachname_start = i
            break

    if nachname_start == len(name_parts) - 1:
        vorname = " ".join(name_parts[:-1])
        nachname = name_parts[-1]
    else:
        vorname = " ".join(name_parts[:nachname_start])
        nachname = " ".join(name_parts[nachname_start:])

    if not vorname or not nachname:
        return None

    return {
        "titel": " ".join(titel_parts) if titel_parts else None,
        "vorname": vorname,
        "nachname": nachname,
    }


class KlinikTeamScraper(BaseScraper):
    name = "klinik_team"
    min_delay = 3.0
    max_delay = 6.0

    def __init__(self):
        super().__init__()
        self._browser = None
        self._page = None

    def _init_browser(self):
        if self._browser:
            return True
        try:
            from playwright.sync_api import sync_playwright
            self._pw = sync_playwright().start()
            self._browser = self._pw.chromium.launch(headless=True)
            self._page = self._browser.new_page()
            self._page.set_extra_http_headers({"Accept-Language": "de-DE,de;q=0.9"})
            return True
        except Exception as e:
            self.logger.error(f"Failed to init Playwright: {e}")
            return False

    def close(self):
        if self._browser:
            try:
                self._browser.close()
                self._pw.stop()
            except Exception:
                pass
        super().close()

    def run(self):
        if not self._init_browser():
            self.logger.error("Cannot run without Playwright")
            return

        for klinik in KLINIKEN:
            progress_key = f"klinik_{klinik['name'].lower().replace(' ', '_')}"
            _, completed = self.get_progress(progress_key)
            if completed:
                self.logger.info(f"  {klinik['name']}: already completed, skipping")
                continue

            self.logger.info(f"Scraping: {klinik['name']}")
            try:
                count = self._scrape_klinik(klinik)
                self.save_progress(progress_key, count or 0, completed=True)
            except Exception as e:
                self.logger.error(f"  {klinik['name']} failed: {e}")
            self.wait()

        self.finalize()

    def _scrape_klinik(self, klinik: dict) -> int:
        """Scrape a clinic's team page for doctor profiles."""
        team_url = klinik["team_url"]
        base_domain = urlparse(team_url).netloc

        # Load team page with Playwright (handles JS rendering)
        try:
            self._page.goto(team_url, timeout=30000, wait_until="networkidle")
            time.sleep(2)  # Let lazy content load
        except Exception as e:
            self.logger.error(f"  Failed to load {team_url}: {e}")
            return 0

        html = self._page.content()

        # Extract doctor profile links from team page
        doctor_urls = self._find_doctor_links(html, team_url, base_domain)
        self.logger.info(f"  {klinik['name']}: found {len(doctor_urls)} doctor links")

        if not doctor_urls:
            # Try parsing doctors directly from the team page
            count = self._parse_doctors_from_page(html, klinik)
            return count

        count = 0
        for url in doctor_urls:
            time.sleep(2 + random.random() * 3)
            try:
                self._page.goto(url, timeout=30000, wait_until="networkidle")
                time.sleep(1)
                profile_html = self._page.content()
                if self._process_doctor_page(profile_html, url, klinik):
                    count += 1
            except Exception as e:
                self.logger.error(f"  Failed to load {url}: {e}")

        return count

    def _find_doctor_links(self, html: str, base_url: str, base_domain: str) -> list[str]:
        """Extract links that likely point to doctor profile pages."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        links = set()

        for a in soup.find_all("a", href=True):
            href = a["href"]
            full_url = urljoin(base_url, href)
            parsed = urlparse(full_url)

            # Must be same domain
            if parsed.netloc != base_domain:
                continue
            # Skip blocked
            if any(blocked in full_url.lower() for blocked in BLOCKED_DOMAINS):
                continue
            # Skip non-http
            if not full_url.startswith("http"):
                continue
            # Skip same page anchors
            if full_url.rstrip("/") == base_url.rstrip("/"):
                continue

            href_lower = href.lower()
            text = a.get_text(strip=True).lower()

            # Check if link looks like a doctor profile
            is_doctor_link = False

            # URL contains doctor-like patterns
            if re.search(r"(dr[.-]|prof[.-])", href_lower):
                is_doctor_link = True
            # Link text contains doctor name patterns
            elif re.search(r"(dr\.|prof\.|med\.)", text):
                is_doctor_link = True
            # URL path suggests a team member page
            elif re.search(r"/(team|aerzte|arzt|doctors?|physicians?|specialists?)/[^/]+/?$", href_lower):
                is_doctor_link = True

            if is_doctor_link:
                links.add(full_url)

        return sorted(links)

    def _process_doctor_page(self, html: str, url: str, klinik: dict) -> bool:
        """Parse a doctor profile page and save if they're a plastic surgeon."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        page_text = soup.get_text(" ", strip=True)

        # Check for Facharzt für Plastische Chirurgie
        is_plastic = any(pat.search(page_text) for pat in FACHARZT_PATTERNS)
        if not is_plastic:
            return False

        # Extract name from h1 or h2
        name_data = None
        for tag in soup.find_all(["h1", "h2"]):
            text = tag.get_text(strip=True)
            if re.search(r"(dr|prof|med)", text, re.IGNORECASE) or len(text.split()) >= 2:
                name_data = _parse_name(text)
                if name_data:
                    break

        if not name_data:
            self.logger.warning(f"  Could not parse name from {url}")
            return False

        # Build doctor record
        data = {
            "vorname": name_data["vorname"],
            "nachname": name_data["nachname"],
            "titel": name_data["titel"],
            "ist_facharzt": True,
            "facharzttitel": "Facharzt für Plastische und Ästhetische Chirurgie",
            "stadt": klinik["stadt"],
            "bundesland": klinik["bundesland"],
            "land": "DE",
            "verified": False,  # Not from official registry
            "source": "klinik_team",
            "source_type": "clinic_website",
            "quelle_url": url,
        }

        # Try to extract more specific Facharzttitel
        for pat in FACHARZT_PATTERNS:
            m = pat.search(page_text)
            if m:
                data["facharzttitel"] = m.group(0)
                break

        # Try to extract phone
        phone_match = re.search(r"(?:Tel|Telefon|Phone)[.:\s]*(\+?[\d\s/()-]{8,})", page_text)
        if phone_match:
            data["telefon"] = phone_match.group(1).strip()

        # Try to extract email
        email_match = re.search(r"[\w.+-]+@[\w.-]+\.\w{2,}", page_text)
        if email_match:
            data["email"] = email_match.group(0)

        arzt_id = self.upsert_arzt(data)
        name_str = f"{name_data.get('titel', '')} {name_data['vorname']} {name_data['nachname']}".strip()
        self.logger.info(f"  {'Saved' if arzt_id else 'Skipped'}: {name_str} ({klinik['name']})")
        return bool(arzt_id)

    def _parse_doctors_from_page(self, html: str, klinik: dict) -> int:
        """Fallback: try to extract doctors directly from the team listing page."""
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")
        page_text = soup.get_text(" ", strip=True)
        count = 0

        # Look for headings that contain doctor names
        for tag in soup.find_all(["h1", "h2", "h3", "h4"]):
            text = tag.get_text(strip=True)
            if not re.search(r"(dr\.|prof\.)", text, re.IGNORECASE):
                continue

            name_data = _parse_name(text)
            if not name_data:
                continue

            # Check surrounding text for Facharzt pattern
            parent = tag.parent
            if parent:
                context = parent.get_text(" ", strip=True)
            else:
                context = page_text

            is_plastic = any(pat.search(context) for pat in FACHARZT_PATTERNS)
            if not is_plastic:
                continue

            data = {
                "vorname": name_data["vorname"],
                "nachname": name_data["nachname"],
                "titel": name_data["titel"],
                "ist_facharzt": True,
                "facharzttitel": "Facharzt für Plastische und Ästhetische Chirurgie",
                "stadt": klinik["stadt"],
                "bundesland": klinik["bundesland"],
                "land": "DE",
                "verified": False,
                "source": "klinik_team",
                "source_type": "clinic_website",
                "quelle_url": klinik["team_url"],
            }

            arzt_id = self.upsert_arzt(data)
            name_str = f"{name_data.get('titel', '')} {name_data['vorname']} {name_data['nachname']}".strip()
            self.logger.info(f"  {'Saved' if arzt_id else 'Skipped'}: {name_str} ({klinik['name']})")
            if arzt_id:
                count += 1

        return count


if __name__ == "__main__":
    scraper = KlinikTeamScraper()
    try:
        scraper.run()
    finally:
        scraper.close()
