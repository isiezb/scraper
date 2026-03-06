"""Scraper for Arzt-Auskunft (Stiftung Gesundheit) — comprehensive DE doctor database.

Uses Playwright to render the Vue.js results page at arzt-auskunft.de.
This is the most comprehensive source: claims to have ALL ~400,000 German doctors.
Specialty codes: ots::246 (Plastische Chirurgie), ots::2330 (Plastische, Rekonstruktive und Ästhetische Chirurgie)
"""

import re
import time
import random
from base_scraper import BaseScraper


SPECIALTIES = [
    {"code": "ots::246", "label": "Plastische Chirurgie"},
    {"code": "ots::2330", "label": "Plastische, Rekonstruktive und Ästhetische Chirurgie"},
]

BASE_URL = "https://www.arzt-auskunft.de"


class ArztAuskunftScraper(BaseScraper):
    name = "arztauskunft_de"
    min_delay = 2.0
    max_delay = 4.0

    def __init__(self):
        super().__init__()
        self._browser = None
        self._page = None
        self._playwright = None

    def _init_browser(self):
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
            self.logger.info("Playwright browser initialized for Arzt-Auskunft")
            return True
        except ImportError:
            self.logger.warning("Playwright not available — cannot scrape arzt-auskunft.de")
            return False
        except Exception as e:
            self.logger.error(f"Failed to init Playwright: {e}")
            return False

    def close(self):
        if self._browser:
            self._browser.close()
            self._playwright.stop()
        super().close()

    def run(self):
        if not self._init_browser():
            return

        for spec in SPECIALTIES:
            progress_key = f"arztauskunft_{spec['code']}"
            _, completed = self.get_progress(progress_key)
            if completed:
                self.logger.info(f"Skipping {spec['label']} (already completed)")
                continue

            try:
                count = self._scrape_specialty(spec)
                self.save_progress(progress_key, count, completed=True)
            except Exception as e:
                self.logger.error(f"Failed {spec['label']}: {e}")

        self.finalize()

    def _scrape_specialty(self, spec: dict) -> int:
        """Scrape all doctors for a given specialty code."""
        code = spec["code"]
        label = spec["label"]
        url = f"{BASE_URL}/ergebnis?FRT={code}&form=fs1"

        self.logger.info(f"Loading {label}: {url}")
        self._page.goto(url, wait_until="networkidle", timeout=60000)

        # Wait for results or "no results" message
        try:
            self._page.wait_for_selector(
                ".result-card, .no-results, .ergebnis-card, .arzt-card, [class*='result']",
                timeout=30000,
            )
        except Exception:
            self.logger.warning(f"No result elements found for {label}, trying to wait longer...")
            time.sleep(5)

        count = 0
        page_num = 1
        max_pages = 100  # Safety limit

        while page_num <= max_pages:
            # Extract doctor cards from current page
            cards = self._extract_cards()
            if not cards:
                self.logger.info(f"  {label} page {page_num}: no more cards")
                break

            self.logger.info(f"  {label} page {page_num}: {len(cards)} doctors")

            for card in cards:
                try:
                    doctor = self._parse_card(card)
                    if doctor:
                        doctor["land"] = "DE"
                        self.upsert_doctor(doctor)
                        count += 1
                except Exception as e:
                    self.logger.error(f"  Failed parsing card: {e}")

            # Try to go to next page
            if not self._click_next_page():
                break

            page_num += 1
            self.wait()

        self.logger.info(f"  {label}: {count} doctors total")
        return count

    def _extract_cards(self) -> list[dict]:
        """Extract doctor data from the current page using JavaScript."""
        try:
            cards = self._page.evaluate("""() => {
                const results = [];
                // Try multiple selectors for result cards
                const selectors = [
                    '.result-card', '.ergebnis-card', '.arzt-card',
                    '[class*="result-item"]', '[class*="doctor-card"]',
                    '.card', '.list-group-item'
                ];

                let elements = [];
                for (const sel of selectors) {
                    elements = document.querySelectorAll(sel);
                    if (elements.length > 0) break;
                }

                // If no cards found via selectors, try to find any structured result elements
                if (elements.length === 0) {
                    // Look for links to /arzt/ profile pages
                    const profileLinks = document.querySelectorAll('a[href*="/arzt/"]');
                    for (const link of profileLinks) {
                        const card = link.closest('div') || link.parentElement;
                        if (card && !results.some(r => r.name === link.textContent.trim())) {
                            results.push({
                                name: link.textContent.trim(),
                                url: link.href,
                                html: card.innerHTML
                            });
                        }
                    }
                    return results;
                }

                for (const el of elements) {
                    const nameEl = el.querySelector('h2, h3, h4, .name, [class*="name"]');
                    const addressEl = el.querySelector('.address, [class*="address"], [class*="location"]');
                    const specEl = el.querySelector('.specialty, [class*="specialty"], [class*="fach"]');
                    const linkEl = el.querySelector('a[href*="/arzt/"]');

                    results.push({
                        name: nameEl ? nameEl.textContent.trim() : '',
                        address: addressEl ? addressEl.textContent.trim() : '',
                        specialty: specEl ? specEl.textContent.trim() : '',
                        url: linkEl ? linkEl.href : '',
                        html: el.innerHTML.substring(0, 2000)
                    });
                }
                return results;
            }""")
            return cards or []
        except Exception as e:
            self.logger.error(f"  Failed extracting cards: {e}")
            return []

    def _parse_card(self, card: dict) -> dict | None:
        """Parse a doctor card extracted from the page."""
        name = card.get("name", "").strip()
        if not name or len(name) < 4:
            return None

        # Extract name parts
        name_data = self._extract_name_from_text(name)
        if not name_data:
            return None

        doctor = {
            **name_data,
            "facharzttitel": "Plastische und Ästhetische Chirurgie",
            "ist_facharzt": True,
            "verified": True,
            "source": "arztauskunft_de",
        }

        # Parse address
        address = card.get("address", "")
        if address:
            plz_match = re.search(r"(\d{5})\s+(.+)", address)
            if plz_match:
                doctor["plz"] = plz_match.group(1)
                doctor["stadt"] = plz_match.group(2).strip()

            street_match = re.match(r"(.+?)\s+\d{5}", address)
            if street_match:
                doctor["strasse"] = street_match.group(1).strip()

        # Parse HTML for additional details
        html = card.get("html", "")
        if html:
            phone_match = re.search(r"(?:Tel|Telefon)[.:\s]+([0-9\s/\-()]+)", html)
            if phone_match:
                doctor["telefon"] = phone_match.group(1).strip()

            email_match = re.search(r"mailto:([^\"']+)", html)
            if email_match:
                doctor["email"] = email_match.group(1).strip()

        # Profile URL
        url = card.get("url", "")
        if url:
            doctor["quelle_url"] = url

        return doctor

    def _click_next_page(self) -> bool:
        """Try to click the next page button. Returns True if successful."""
        try:
            next_btn = self._page.query_selector(
                'a[rel="next"], .pagination .next a, [class*="next-page"], '
                'button:has-text("Weiter"), a:has-text("Weiter"), '
                'a:has-text("nächste"), .pagination li:last-child a'
            )
            if next_btn and next_btn.is_visible():
                next_btn.click()
                self._page.wait_for_load_state("networkidle", timeout=15000)
                time.sleep(1)
                return True
        except Exception:
            pass
        return False

    def _extract_name_from_text(self, text: str) -> dict | None:
        """Extract titel, vorname, nachname from a text string."""
        text = text.strip()
        if not text:
            return None

        # Remove common prefixes
        text = re.sub(r"^(Herr|Frau|Arzt|Ärztin)\s+", "", text, flags=re.IGNORECASE)

        # Extract title parts
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

        titel = " ".join(title_parts) if title_parts else None
        vorname = name_parts[0]
        nachname = " ".join(name_parts[1:])

        return {"titel": titel, "vorname": vorname, "nachname": nachname}
