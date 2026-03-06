"""Scraper for Arzt-Auskunft (Stiftung Gesundheit) — comprehensive DE doctor database.

Uses server-rendered listing pages at arzt-auskunft.de/plastische-chirurgie/.
2,275+ plastic surgeons nationwide. No Playwright needed — plain HTML.
"""

import re
import time
from bs4 import BeautifulSoup
from base_scraper import BaseScraper


# Specialty listing paths on arzt-auskunft.de
SPECIALTY_PATHS = [
    {"path": "plastische-chirurgie", "label": "Plastische Chirurgie"},
    {"path": "plastische-und-aesthetische-chirurgie", "label": "Plastische und Ästhetische Chirurgie"},
]

BASE_URL = "https://www.arzt-auskunft.de"


class ArztAuskunftScraper(BaseScraper):
    name = "arztauskunft_de"
    min_delay = 2.0
    max_delay = 4.0

    def run(self):
        for spec in SPECIALTY_PATHS:
            progress_key = f"arztauskunft_{spec['path']}"
            last_page, completed = self.get_progress(progress_key)
            if completed:
                self.logger.info(f"Skipping {spec['label']} (already completed)")
                continue

            try:
                count = self._scrape_specialty(spec, start_page=last_page or 1)
                self.save_progress(progress_key, count, completed=True)
            except Exception as e:
                self.logger.error(f"Failed {spec['label']}: {e}")

        self.finalize()

    def _scrape_specialty(self, spec: dict, start_page: int = 1) -> int:
        """Scrape all listing pages for a specialty."""
        path = spec["path"]
        label = spec["label"]
        count = 0
        page = start_page
        max_pages = 50  # Safety limit

        while page <= max_pages:
            url = f"{BASE_URL}/{path}/" if page == 1 else f"{BASE_URL}/{path}/{page}/"
            self.logger.info(f"  {label} page {page}: {url}")

            try:
                resp = self.session.get(url, timeout=30)
                if resp.status_code == 404:
                    self.logger.info(f"  {label} page {page}: 404, done")
                    break
                resp.raise_for_status()
            except Exception as e:
                self.logger.error(f"  {label} page {page} fetch failed: {e}")
                break

            # Force UTF-8 to avoid mojibake (KÃ¶ln → Köln)
            resp.encoding = "utf-8"
            soup = BeautifulSoup(resp.text, "lxml")

            # Find all profile links
            profile_links = soup.select('a[href*="/arzt/"]')
            if not profile_links:
                self.logger.info(f"  {label} page {page}: no profile links, done")
                break

            # Deduplicate links on the page
            seen_urls = set()
            unique_links = []
            for link in profile_links:
                href = link.get("href", "")
                if href and href not in seen_urls and "/arzt/" in href:
                    seen_urls.add(href)
                    unique_links.append(link)

            self.logger.info(f"  {label} page {page}: {len(unique_links)} doctors")

            for link in unique_links:
                try:
                    doctor = self._parse_listing_entry(link)
                    if doctor:
                        self.upsert_arzt(doctor)
                        count += 1
                except Exception as e:
                    self.logger.error(f"  Failed parsing entry: {e}")

            # Save progress after each page
            self.save_progress(f"arztauskunft_{spec['path']}", page)

            page += 1
            self.wait()

        self.logger.info(f"  {label}: {count} doctors total")
        return count

    def _parse_listing_entry(self, link_el) -> dict | None:
        """Parse a doctor from a listing page link and its surrounding context."""
        href = link_el.get("href", "")
        if not href or "/arzt/" not in href:
            return None

        # Make URL absolute
        if href.startswith("/"):
            href = BASE_URL + href

        # Extract info from URL slug:
        # /arzt/plastische-chirurgie/berlin/dr-firstname-lastname-1234567
        url_match = re.search(r"/arzt/[^/]+/([^/]+)/([^/]+?)(?:-(\d{5,}))?\s*$", href)
        if not url_match:
            return None

        city_slug = url_match.group(1)
        name_slug = url_match.group(2)

        # Get the display name from the link text or surrounding card
        display_name = link_el.get_text(strip=True)

        # Skip navigation/generic links
        skip_texts = {"mehr details", "details", "profil", "weiter", "mehr"}
        if display_name.lower().strip() in skip_texts:
            display_name = ""

        # Try to get more context from the parent card/container
        card = link_el.parent
        # Walk up to find a container with more content
        for _ in range(5):
            if card and card.parent and card.parent.name not in ("body", "html", "[document]"):
                text_len = len(card.get_text(strip=True))
                if text_len > len(display_name) + 20:
                    break
                card = card.parent
            else:
                break

        card_text = card.get_text("\n", strip=True) if card else ""

        # If display_name was generic, try to find the real name in card context
        if not display_name or len(display_name) < 4:
            # Look for a heading element in the card
            if card:
                for tag in ["h2", "h3", "h4", "strong", "b"]:
                    heading = card.find(tag)
                    if heading:
                        candidate = heading.get_text(strip=True)
                        if candidate.lower() not in skip_texts and len(candidate) >= 4:
                            display_name = candidate
                            break
            # Still nothing? Try first line of card text
            if not display_name or len(display_name) < 4:
                for line in card_text.split("\n"):
                    line = line.strip()
                    if line.lower() not in skip_texts and len(line) >= 4:
                        display_name = line
                        break

        # Parse the display name
        name_data = self._extract_name(display_name)
        if not name_data:
            # Fallback: parse from URL slug
            name_data = self._name_from_slug(name_slug)
        if not name_data:
            return None

        doctor = {
            **name_data,
            "facharzttitel": "Plastische und Ästhetische Chirurgie",
            "ist_facharzt": True,
            "verified": True,
            "source": "arztauskunft_de",
            "quelle_url": href,
            "land": "DE",
        }

        # Extract city from URL slug
        city = city_slug.replace("-", " ").title()
        # Fix common city name issues (e.g. Koeln → Köln, Muenchen → München)
        city = city.replace("Ue", "Ü").replace("ue", "ü")
        city = city.replace("Oe", "Ö").replace("oe", "ö")
        city = city.replace("Ae", "Ä").replace("ae", "ä")
        doctor["stadt"] = city

        # Try to extract address from card text
        self._extract_address(card_text, doctor)

        # Try to extract phone from card text
        phone_match = re.search(r"(?:Tel|Telefon)[.:\s]+([0-9\s/\-()]+)", card_text)
        if phone_match:
            doctor["telefon"] = phone_match.group(1).strip()

        return doctor

    # Keywords that indicate an institution, not a person
    INSTITUTION_KEYWORDS = {
        "klinik", "kliniken", "krankenhaus", "hospital", "clinic", "clinicum",
        "praxis", "zentrum", "center", "centrum", "institut", "universit",
        "berufsgen", "gemeinschaftspraxis", "mvz", "gmbh", "ggmbh", "gbr",
        "e.v.", "co. kg", "stiftung", "akademie", "ambulanz", "abteilung",
        "bergmannsheil", "charite", "charité", "asklepios", "helios",
        "vivantes", "agaplesion", "ameos", "atos", "sana ", "diakonie",
        "diakovere", "caritas", "evangelisch", "evang.", "ev. ", "kathol",
        "residenz", "campus", "gesundheit nord", "fachärzte", "bundeswehr",
        " se ", "ästhetik", "aesthetik", "aesthetic", "surgery", "chirurgia",
        "esthetica", "ethianum", "medcenter", "lubinus", "beauty",
        "lacomed", "lipoedem", "policum", "standort", "filiale", "tagesklinik",
        "fachklinik", "medizinisch", "operationszentrum", "op-zentrum",
        "hautarzt", "hautklinik", "med-plast", "chirurgen", "aasee",
        "vital", "park clinic", " rtz", "stift", "ev ",
    }

    def _is_institution(self, text: str) -> bool:
        """Check if text looks like an institution name rather than a person."""
        lower = text.lower()
        return any(kw in lower for kw in self.INSTITUTION_KEYWORDS)

    def _extract_name(self, text: str) -> dict | None:
        """Extract titel, vorname, nachname from display name."""
        text = text.strip()
        if not text or len(text) < 4:
            return None

        # Reject institution names
        if self._is_institution(text):
            return None

        # Remove salutation
        text = re.sub(r"^(Herr|Frau|Arzt|Ärztin)\s+", "", text, flags=re.IGNORECASE)

        # Split into title and name parts
        title_parts = []
        name_parts = []
        for word in text.split():
            word_lower = word.lower().rstrip(".,")
            if word_lower in {"prof", "prof.", "dr", "dr.", "med", "med.", "pd",
                             "priv.-doz", "priv.-doz.", "univ", "univ.",
                             "dent", "dent.", "habil", "habil.", "dipl", "dipl.",
                             "msc", "m.sc.", "ph.d", "ph.d."}:
                title_parts.append(word)
            else:
                name_parts.append(word)

        if len(name_parts) < 2:
            return None

        return {
            "titel": " ".join(title_parts) if title_parts else None,
            "vorname": name_parts[0],
            "nachname": " ".join(name_parts[1:]),
        }

    def _name_from_slug(self, slug: str) -> dict | None:
        """Fallback: extract name from URL slug like 'dr-med-firstname-lastname'."""
        # Reject institution slugs
        if any(kw in slug.lower() for kw in (
                "klinik", "kliniken", "krankenhaus", "hospital", "clinic", "clinicum",
                "praxis", "zentrum", "center", "centrum", "institut", "universit",
                "berufsgen", "gemeinschaftspraxis", "mvz", "gmbh", "ggmbh", "gbr",
                "stiftung", "akademie", "ambulanz", "abteilung", "bergmannsheil",
                "charite", "asklepios", "helios", "vivantes", "agaplesion",
                "ameos", "atos", "sana-", "diakonie", "diakovere", "caritas",
                "evangelisch", "kathol", "residenz", "campus", "bundeswehr",
                "facharzte", "aesthetik", "aesthetic", "surgery", "chirurgia",
                "esthetica", "ethianum", "medcenter", "lubinus",
                "beauty", "lacomed", "lipoedem", "policum", "standort",
                "filiale", "tagesklinik", "fachklinik", "medizinisch",
                "operationszentrum", "op-zentrum", "med-plast", "chirurgen",
                "aasee", "vital-residenz", "park-clinic", "stift", "ev-stift")):
            return None
        parts = slug.split("-")
        title_words = {"dr", "med", "prof", "priv", "doz", "dent", "habil", "dipl", "univ"}

        title_parts = []
        name_parts = []
        for p in parts:
            if p.lower() in title_words:
                title_parts.append(p.capitalize() + ".")
            else:
                name_parts.append(p.capitalize())

        if len(name_parts) < 2:
            return None

        return {
            "titel": " ".join(title_parts) if title_parts else None,
            "vorname": name_parts[0],
            "nachname": " ".join(name_parts[1:]),
        }

    def _extract_address(self, text: str, doctor: dict):
        """Try to extract PLZ and street from card text."""
        # Look for German PLZ pattern (5 digits + city)
        plz_match = re.search(r"(\d{5})\s+(\S.+?)(?:\n|$)", text)
        if plz_match:
            doctor["plz"] = plz_match.group(1)
            # City might already be set from URL, but text version is more accurate
            city_text = plz_match.group(2).strip()
            if city_text and len(city_text) > 2:
                doctor["stadt"] = city_text

        # Street: line before the PLZ
        street_match = re.search(r"(?:^|\n)(.+?)\n\s*\d{5}", text)
        if street_match:
            street = street_match.group(1).strip()
            # Sanity check: streets usually contain a number
            if re.search(r"\d", street) and len(street) < 80:
                doctor["strasse"] = street
