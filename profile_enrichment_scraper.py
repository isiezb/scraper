"""Profile enrichment scraper — visits detail pages to fill in missing data.

Reads existing doctors from DB that have a quelle_url (ArztAuskunft profile link),
fetches each detail page, and extracts: phone, website, practice name,
Zusatzbezeichnungen, Abrechnungsart (GKV/Privat), full address.

Phase 2 enrichment — runs after all Phase 1 scrapers.
NEVER creates new records, only updates existing ones.
"""

import re
import time
from bs4 import BeautifulSoup
from base_scraper import BaseScraper


class ProfileEnrichmentScraper(BaseScraper):
    name = "profile_enrichment"
    min_delay = 2.0
    max_delay = 4.0

    def run(self):
        """Find doctors missing profile data and enrich them."""
        progress_key = "enrich_detail_pages"
        last_id, completed = self.get_progress(progress_key)
        if completed:
            self.logger.info("Profile enrichment already completed, skipping")
            return

        # Get doctors that have a quelle_url but are missing contact details
        cur = self.conn.cursor()
        cur.execute("""
            SELECT id, vorname, nachname, quelle_url
            FROM aerzte
            WHERE quelle_url IS NOT NULL
              AND quelle_url LIKE '%%arzt-auskunft.de%%'
              AND id > %s
              AND (telefon IS NULL OR website_url IS NULL)
            ORDER BY id ASC
        """, (last_id or 0,))
        doctors = cur.fetchall()
        cur.close()

        total = len(doctors)
        self.logger.info(f"Found {total} doctors to enrich")

        enriched = 0
        for i, (arzt_id, vorname, nachname, quelle_url) in enumerate(doctors):
            try:
                data = self._fetch_profile(quelle_url)
                if data:
                    updated = self._update_doctor(arzt_id, data)
                    if updated:
                        enriched += 1
                        self.logger.info(f"Enriched: {vorname} {nachname} (id={arzt_id})")
                    else:
                        self.logger.debug(f"No new data: {vorname} {nachname} (id={arzt_id})")
            except Exception as e:
                self.logger.error(f"Failed enriching {vorname} {nachname} (id={arzt_id}): {e}")

            # Save progress every 50 doctors
            if (i + 1) % 50 == 0:
                self.save_progress(progress_key, arzt_id)
                self.logger.info(f"Progress: {i + 1}/{total} processed, {enriched} enriched")

            self.wait()

        self.save_progress(progress_key, doctors[-1][0] if doctors else last_id or 0, completed=True)
        self.logger.info(f"Profile enrichment done: {enriched}/{total} enriched")

    def _fetch_profile(self, url: str) -> dict | None:
        """Fetch and parse an ArztAuskunft profile page."""
        try:
            resp = self.session.get(url, timeout=30)
            if resp.status_code == 404 or resp.status_code == 505:
                return None
            resp.raise_for_status()
        except Exception:
            return None

        resp.encoding = "utf-8"
        soup = BeautifulSoup(resp.text, "lxml")
        text = soup.get_text("\n", strip=True)

        data = {}

        # Phone: look for tel: links
        tel_links = soup.find_all("a", href=re.compile(r"^tel:"))
        if tel_links:
            # Take the first phone number, clean it up
            raw = tel_links[0].get_text(strip=True)
            phone = re.sub(r"\s+", " ", raw).strip()
            if phone and len(phone) >= 6:
                data["telefon"] = phone

        # Website: look for external http links that aren't arzt-auskunft.de
        for a in soup.find_all("a", href=re.compile(r"^https?://")):
            href = a.get("href", "")
            if "arzt-auskunft.de" in href:
                continue
            if any(skip in href for skip in [
                "facebook.com", "instagram.com", "twitter.com", "google.com",
                "youtube.com", "tiktok.com", "linkedin.com", "jameda.de",
                "doctolib.de", "sanego.de", "maps.google",
            ]):
                continue
            # Likely the doctor's own website
            data["website_url"] = href
            break

        # Abrechnungsart → GKV status
        abrechnungsart_match = re.search(
            r"Abrechnungsart[:\s]*(.+?)(?:\n|$)", text, re.IGNORECASE
        )
        if abrechnungsart_match:
            abr = abrechnungsart_match.group(1).lower()
            if "kasse" in abr or "gesetzlich" in abr:
                data["gkv_zugelassen"] = True
            elif "privat" in abr or "selbstzahler" in abr:
                data["gkv_zugelassen"] = False

        # Zusatzbezeichnungen / Schwerpunkte from h2
        h2 = soup.find("h2")
        if h2:
            h2_text = h2.get_text(strip=True)
            # Split multiple specialties
            specs = [s.strip() for s in re.split(r"[,;]", h2_text) if s.strip()]
            # Filter out the main "Plastische" one, keep Zusatzbezeichnungen
            zusatz = [s for s in specs if "plastisch" not in s.lower() and len(s) > 3]
            if zusatz:
                data["schwerpunkte"] = ", ".join(zusatz)

        # Practice name from <strong> near address
        # Look for strong tags that aren't just labels
        for strong in soup.find_all("strong"):
            txt = strong.get_text(strip=True)
            if len(txt) > 10 and not any(kw in txt.lower() for kw in [
                "abrechnungsart", "sprechzeiten", "leistung", "facharzt",
                "herr", "frau", "erreichbarkeit", "barrierefreiheit",
            ]):
                # Check if next sibling has address-like content
                sibling_text = ""
                for sib in strong.next_siblings:
                    sibling_text = str(sib).strip()
                    if sibling_text:
                        break
                if re.search(r"\d{5}", sibling_text):
                    data["klinik_praxis_name"] = txt
                    break

        # Full address: look for PLZ pattern in the text
        addr_match = re.search(
            r"(?:^|\n)(.{5,60}?),?\s*(\d{5})\s+([A-ZÄÖÜ][a-zäöüß]+(?:[\s-][A-Za-zäöüß]+)*)",
            text
        )
        if addr_match:
            strasse = addr_match.group(1).strip().rstrip(",")
            plz = addr_match.group(2)
            stadt = addr_match.group(3).strip()
            if re.search(r"\d", strasse) and len(strasse) < 80:
                data["strasse"] = strasse
            data["plz"] = plz
            data["stadt"] = stadt

        return data if data else None

    def _update_doctor(self, arzt_id: int, data: dict) -> bool:
        """Update doctor with enrichment data. Only fills in NULL fields."""
        cur = self.conn.cursor()

        # Only update fields that are currently NULL
        cur.execute("""
            SELECT telefon, website_url, gkv_zugelassen, schwerpunkte, strasse, plz
            FROM aerzte WHERE id = %s
        """, (arzt_id,))
        row = cur.fetchone()
        if not row:
            cur.close()
            return False

        existing_telefon, existing_website, existing_gkv, existing_schwer, existing_strasse, existing_plz = row

        updates = []
        values = []

        if not existing_telefon and data.get("telefon"):
            updates.append("telefon = %s")
            values.append(data["telefon"])
        if not existing_website and data.get("website_url"):
            updates.append("website_url = %s")
            values.append(data["website_url"])
        if existing_gkv is None and data.get("gkv_zugelassen") is not None:
            updates.append("gkv_zugelassen = %s")
            values.append(data["gkv_zugelassen"])
        if not existing_schwer and data.get("schwerpunkte"):
            updates.append("schwerpunkte = %s")
            values.append(data["schwerpunkte"])
        if not existing_strasse and data.get("strasse"):
            updates.append("strasse = %s")
            values.append(data["strasse"])
        if not existing_plz and data.get("plz"):
            updates.append("plz = %s")
            values.append(data["plz"])
        # Always update stadt if we have a better one (from detail page)
        if data.get("stadt"):
            updates.append("stadt = %s")
            values.append(data["stadt"])

        if not updates:
            cur.close()
            return False

        updates.append("letzte_aktualisierung = NOW()")
        values.append(arzt_id)
        cur.execute(
            f"UPDATE aerzte SET {', '.join(updates)} WHERE id = %s",
            values,
        )
        self.conn.commit()
        cur.close()
        return True


if __name__ == "__main__":
    scraper = ProfileEnrichmentScraper()
    try:
        scraper.run()
    finally:
        scraper.close()
