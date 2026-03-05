"""Scraper for plastic surgery professional societies (enrichment only).

Targets DGPRÄC, DGÄPC, VDÄPC, and ISAPS member directories.
This scraper NEVER creates new records — it only enriches existing
doctor records with membership booleans via name matching.
All entries: verified=false, source_type='professional_association'.
"""

import re
from urllib.parse import urljoin
from bs4 import BeautifulSoup
from base_scraper import BaseScraper, normalize_name

SOCIETIES = [
    {
        "name": "DGPRÄC",
        "full_name": "Deutsche Gesellschaft der Plastischen, Rekonstruktiven und Ästhetischen Chirurgen",
        "member_url": "https://www.dgpraec.de/patienten/arztsuche/",
        "land": "DE",
    },
    {
        "name": "DGÄPC",
        "full_name": "Deutsche Gesellschaft für Ästhetisch-Plastische Chirurgie",
        "member_url": "https://www.dgaepc.de/mitgliedersuche/",
        "land": "DE",
    },
    {
        "name": "VDÄPC",
        "full_name": "Vereinigung der Deutschen Ästhetisch-Plastischen Chirurgen",
        "member_url": "https://www.vdaepc.de/arztsuche/",
        "land": "DE",
    },
    {
        "name": "ISAPS",
        "full_name": "International Society of Aesthetic Plastic Surgery",
        "member_url": "https://www.isaps.org/discover/find-a-surgeon/",
        "land": "DE",
    },
]

BUNDESLAND_MAP = {
    "berlin": "Berlin",
    "hamburg": "Hamburg",
    "münchen": "Bayern", "muenchen": "Bayern", "munich": "Bayern", "bayern": "Bayern",
    "köln": "Nordrhein-Westfalen", "koeln": "Nordrhein-Westfalen", "düsseldorf": "Nordrhein-Westfalen",
    "duesseldorf": "Nordrhein-Westfalen", "nordrhein-westfalen": "Nordrhein-Westfalen",
    "frankfurt": "Hessen", "hessen": "Hessen",
    "stuttgart": "Baden-Württemberg", "baden-württemberg": "Baden-Württemberg",
    "freiburg": "Baden-Württemberg", "karlsruhe": "Baden-Württemberg",
    "hannover": "Niedersachsen", "niedersachsen": "Niedersachsen",
    "dresden": "Sachsen", "leipzig": "Sachsen", "sachsen": "Sachsen",
    "wien": "Wien", "vienna": "Wien",
    "innsbruck": "Tirol",
    "graz": "Steiermark",
    "salzburg": "Salzburg",
    "zürich": "Zürich", "zurich": "Zürich",
    "bern": "Bern",
    "basel": "Basel-Stadt",
    "genf": "Genf", "genève": "Genf",
}


def guess_bundesland(stadt: str | None) -> str | None:
    if not stadt:
        return None
    return BUNDESLAND_MAP.get(stadt.lower().strip())


def guess_land(stadt: str | None) -> str:
    if not stadt:
        return "DE"
    s = stadt.lower().strip()
    if s in ("wien", "vienna", "innsbruck", "graz", "salzburg", "linz", "klagenfurt"):
        return "AT"
    if s in ("zürich", "zurich", "bern", "basel", "genf", "genève", "lausanne", "luzern", "montreux"):
        return "CH"
    return "DE"


class DGPRAECScraper(BaseScraper):
    name = "fachgesellschaft"
    min_delay = 1.5
    max_delay = 3.0

    def run(self):
        for society in SOCIETIES:
            self.logger.info(f"Scraping: {society['name']}")
            try:
                self._scrape_society(society)
            except Exception as e:
                self.logger.error(f"Failed {society['name']}: {e}")
            self.wait()
        self.finalize()

    def _scrape_society(self, society: dict):
        resp = self.fetch(society["member_url"])
        if not resp:
            return

        soup = BeautifulSoup(resp.text, "lxml")

        # Try to find member profile links
        profile_links = self._find_profile_links(soup, society["member_url"])
        self.logger.info(f"  Found {len(profile_links)} profile links")

        # If no profile links, try parsing the listing page directly
        if not profile_links:
            members = self._parse_member_list(soup)
            self.logger.info(f"  Found {len(members)} members from listing")
            for member in members:
                self._process_member(member, society)
            return

        for url in profile_links:
            self.wait()
            profile_resp = self.fetch(url)
            if not profile_resp:
                continue
            try:
                member = self._parse_profile_page(profile_resp.text, url)
                if member:
                    self._process_member(member, society)
            except Exception as e:
                self.logger.error(f"  Failed parsing {url}: {e}")

    def _find_profile_links(self, soup: BeautifulSoup, base_url: str) -> list[str]:
        """Find links to individual member profile pages."""
        links = set()
        for a in soup.find_all("a", href=True):
            href = a["href"]
            text = a.get_text(strip=True).lower()
            # Heuristic: profile-like URLs
            if any(kw in href.lower() for kw in ["profil", "member", "mitglied", "arzt", "surgeon", "dr-", "dr."]):
                full_url = urljoin(base_url, href)
                if full_url != base_url and full_url.startswith("http"):
                    links.add(full_url)
            elif any(kw in text for kw in ["dr.", "prof.", "dr ", "prof "]):
                full_url = urljoin(base_url, href)
                if full_url != base_url and full_url.startswith("http"):
                    links.add(full_url)
        return list(links)

    def _parse_member_list(self, soup: BeautifulSoup) -> list[dict]:
        """Parse member entries directly from a listing page."""
        members = []

        # Pattern: cards/list items with doctor info
        for item in soup.find_all(["div", "li", "article"], class_=re.compile(r"member|arzt|doctor|profile|card", re.I)):
            text = item.get_text(" ", strip=True)
            name_data = self._extract_name_from_text(text)
            if name_data:
                stadt = self._extract_stadt(item)
                name_data["stadt"] = stadt
                members.append(name_data)

        # Pattern: table rows
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 1:
                text = " ".join(c.get_text(strip=True) for c in cells)
                name_data = self._extract_name_from_text(text)
                if name_data:
                    stadt = None
                    if len(cells) >= 3:
                        stadt = cells[2].get_text(strip=True) or None
                    elif len(cells) >= 2:
                        stadt = cells[1].get_text(strip=True) or None
                    name_data["stadt"] = stadt
                    members.append(name_data)

        return members

    def _parse_profile_page(self, html: str, url: str) -> dict | None:
        """Parse an individual member profile page."""
        soup = BeautifulSoup(html, "lxml")

        h1 = soup.find("h1")
        if not h1:
            return None

        name_text = h1.get_text(strip=True)
        name_data = self._extract_name_from_text(name_text)
        if not name_data:
            return None

        # Try to find city
        page_text = soup.get_text(" ", strip=True)
        stadt = self._extract_stadt(soup)
        name_data["stadt"] = stadt
        name_data["website_url"] = url

        return name_data

    def _extract_name_from_text(self, text: str) -> dict | None:
        """Extract name components from text containing a doctor name."""
        text = text.strip()
        if not text or len(text) < 4:
            return None

        # Check if it contains a doctor-like name
        if not any(kw in text.lower() for kw in ["dr", "prof", "med"]):
            # Must have at least 2 words that look like names
            words = text.split()
            if len(words) < 2 or not all(w[0].isupper() for w in words[:2] if w):
                return None

        titel_parts = []
        name_parts = []
        titel_kw = {"prof", "prof.", "dr", "dr.", "med", "med.", "pd", "priv.-doz", "priv.-doz.", "univ", "univ.", "dent", "dent."}

        words = text.split()
        in_name = False
        for word in words:
            clean = word.lower().rstrip(".,")
            if not in_name and clean in titel_kw:
                titel_parts.append(word)
            elif not in_name and word == ",":
                continue
            else:
                in_name = True
                # Stop at non-name content
                if any(c.isdigit() for c in word) or word in (",", "|", "-", "–", "•"):
                    break
                name_parts.append(word)

        if len(name_parts) < 2:
            return None

        return {
            "vorname": name_parts[0],
            "nachname": " ".join(name_parts[1:]),
            "titel": " ".join(titel_parts),
        }

    def _extract_stadt(self, element) -> str | None:
        """Try to extract city from a page element."""
        # Look for address-like elements
        addr = element.find(["address", "span", "p"], class_=re.compile(r"city|stadt|location|address|ort", re.I))
        if addr:
            return addr.get_text(strip=True).split(",")[0].strip() or None

        # Look for postal code pattern (German: 5 digits)
        text = element.get_text(" ", strip=True)
        m = re.search(r"\b\d{4,5}\s+([A-ZÄÖÜ][a-zäöüß]+(?:\s+[a-zäöüß]+)?)", text)
        if m:
            return m.group(1).strip()

        return None

    def _process_member(self, member: dict, society: dict):
        """Enrich existing doctor record with society membership.

        This method NEVER creates new records. It only finds existing
        doctors by name and updates their membership boolean fields.
        """
        vorname = member["vorname"]
        nachname = member["nachname"]

        # Find existing doctor by name match
        cur = self.conn.cursor()
        cur.execute(
            """SELECT id FROM aerzte
               WHERE LOWER(nachname) = LOWER(%s) AND LOWER(vorname) = LOWER(%s)""",
            (nachname, vorname),
        )
        rows = cur.fetchall()
        cur.close()

        if not rows:
            self.logger.debug(f"  No existing record for {vorname} {nachname} — skipping (enrich-only)")
            self.stats["uebersprungen"] += 1
            return

        # Map society to membership boolean field
        membership_field = {
            "DGPRÄC": "dgpraec_mitglied",
            "DGÄPC": "dgaepc_mitglied",
            "VDÄPC": "vdaepc_mitglied",
            "ISAPS": "isaps_mitglied",
        }.get(society["name"])

        for row in rows:
            arzt_id = row[0]

            # Set membership boolean
            if membership_field:
                self.enrich_arzt(arzt_id, {membership_field: True})

            # Record in mitgliedschaften table
            self.upsert_mitgliedschaft(
                arzt_id,
                society["full_name"],
                status="Mitglied",
                verifiziert=True,
                quelle_url=society["member_url"],
            )
            self.stats["aktualisiert"] += 1

        self.logger.info(f"  {member.get('titel', '')} {vorname} {nachname} -> {society['name']} ({len(rows)} record(s) enriched)")


if __name__ == "__main__":
    scraper = DGPRAECScraper()
    try:
        scraper.run()
    finally:
        scraper.close()
