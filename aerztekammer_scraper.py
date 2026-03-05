"""Scraper for Landesärztekammer doctor registries.

Queries public search portals of regional medical chambers to verify
doctor registrations and extract Facharzt qualifications.
"""

import re
from urllib.parse import urlencode
from bs4 import BeautifulSoup
from base_scraper import BaseScraper

# Landesärztekammern with public search portals
KAMMERN = [
    {
        "name": "Ärztekammer Nordrhein",
        "search_url": "https://www.aekno.de/arztsuche",
        "land": "DE",
        "bundesland": "Nordrhein-Westfalen",
    },
    {
        "name": "Bayerische Landesärztekammer",
        "search_url": "https://www.blaek.de/arztsuche",
        "land": "DE",
        "bundesland": "Bayern",
    },
    {
        "name": "Ärztekammer Berlin",
        "search_url": "https://www.aerztekammer-berlin.de/arztsuche",
        "land": "DE",
        "bundesland": "Berlin",
    },
    {
        "name": "Ärztekammer Hamburg",
        "search_url": "https://www.aerztekammer-hamburg.org/arztsuche",
        "land": "DE",
        "bundesland": "Hamburg",
    },
    {
        "name": "Landesärztekammer Baden-Württemberg",
        "search_url": "https://www.aerztekammer-bw.de/arztsuche",
        "land": "DE",
        "bundesland": "Baden-Württemberg",
    },
    {
        "name": "Landesärztekammer Hessen",
        "search_url": "https://www.laekh.de/arztsuche",
        "land": "DE",
        "bundesland": "Hessen",
    },
]

PLASTISCHE_KEYWORDS = [
    "plastische",
    "ästhetische chirurgie",
    "aesthetische chirurgie",
    "plastisch",
]


class AerztekammerScraper(BaseScraper):
    name = "aerztekammer"
    min_delay = 5.0
    max_delay = 10.0

    def run(self):
        """Search each Kammer for plastic/aesthetic surgery specialists."""
        for kammer in KAMMERN:
            self.logger.info(f"Querying: {kammer['name']}")
            try:
                self._scrape_kammer(kammer)
            except Exception as e:
                self.logger.error(f"Failed {kammer['name']}: {e}")
            self.wait()
        self.finalize()

    def _scrape_kammer(self, kammer: dict):
        """Search a single Kammer for relevant doctors."""
        search_terms = ["Plastische Chirurgie", "Ästhetische Chirurgie"]

        for term in search_terms:
            self.wait()
            resp = self.fetch(kammer["search_url"], params={"q": term})
            if not resp:
                continue

            soup = BeautifulSoup(resp.text, "lxml")
            results = self._parse_search_results(soup)
            self.logger.info(f"  '{term}' -> {len(results)} results")

            for result in results:
                result["land"] = kammer["land"]
                result["bundesland"] = kammer["bundesland"]
                result["kammer_name"] = kammer["name"]
                self._process_result(result)

    def _parse_search_results(self, soup: BeautifulSoup) -> list[dict]:
        """Extract doctor entries from search results page.

        Tries common patterns across Ärztekammer websites:
        - Table rows with doctor info
        - List items / cards with structured data
        """
        results = []

        # Pattern 1: Table-based results
        for row in soup.find_all("tr"):
            cells = row.find_all("td")
            if len(cells) >= 2:
                text = " ".join(c.get_text(strip=True) for c in cells)
                if any(kw in text.lower() for kw in PLASTISCHE_KEYWORDS):
                    name_cell = cells[0].get_text(strip=True)
                    fach_cell = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    parsed = self._parse_result_entry(name_cell, fach_cell)
                    if parsed:
                        results.append(parsed)

        # Pattern 2: List/card-based results
        for item in soup.find_all(["li", "div"], class_=re.compile(r"result|arzt|doctor|entry", re.I)):
            text = item.get_text(" ", strip=True)
            if any(kw in text.lower() for kw in PLASTISCHE_KEYWORDS):
                parsed = self._parse_result_entry(text, "")
                if parsed:
                    results.append(parsed)

        return results

    def _parse_result_entry(self, name_text: str, fach_text: str) -> dict | None:
        """Parse a single search result into structured data."""
        name_text = name_text.strip()
        if not name_text or len(name_text) < 4:
            return None

        # Try to extract name components
        titel_parts = []
        name_parts = []
        titel_kw = {"prof", "prof.", "dr", "dr.", "med", "med.", "pd", "priv.-doz", "priv.-doz.", "univ", "univ."}

        words = name_text.split()
        in_name = False
        for word in words:
            if not in_name and word.lower().rstrip(".,") in titel_kw:
                titel_parts.append(word)
            else:
                in_name = True
                name_parts.append(word)

        if len(name_parts) < 2:
            return None

        # Detect Facharzt from fach_text or name_text
        combined = f"{name_text} {fach_text}".lower()
        ist_facharzt = any(kw in combined for kw in PLASTISCHE_KEYWORDS)

        facharzttitel = None
        if ist_facharzt:
            fa_match = re.search(
                r"(Fach(?:arzt|ärztin)\s+für\s+[^,\n]+)",
                f"{name_text} {fach_text}",
                re.IGNORECASE,
            )
            facharzttitel = fa_match.group(1).strip()[:200] if fa_match else "Facharzt für Plastische und Ästhetische Chirurgie"

        return {
            "vorname": name_parts[0],
            "nachname": " ".join(name_parts[1:]),
            "titel": " ".join(titel_parts),
            "ist_facharzt": ist_facharzt,
            "facharzttitel": facharzttitel,
        }

    def _process_result(self, result: dict):
        """Upsert a doctor from Ärztekammer search results."""
        arzt_data = {
            "vorname": result["vorname"],
            "nachname": result["nachname"],
            "titel": result.get("titel", ""),
            "geschlecht": None,
            "ist_facharzt": result["ist_facharzt"],
            "facharzttitel": result.get("facharzttitel"),
            "selbstbezeichnung": result.get("facharzttitel"),
            "approbation_verifiziert": True,
            "kammer_id": result.get("kammer_name"),
            "land": result["land"],
            "bundesland": result["bundesland"],
            "stadt": result.get("stadt"),
            "datenquelle": "aerztekammer",
        }

        arzt_id = self.upsert_arzt(arzt_data)
        if arzt_id:
            self.logger.info(f"  Processed: {result.get('titel', '')} {result['vorname']} {result['nachname']}")


if __name__ == "__main__":
    scraper = AerztekammerScraper()
    try:
        scraper.run()
    finally:
        scraper.close()
