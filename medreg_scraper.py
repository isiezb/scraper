"""Scraper for Swiss MedReg (medregom.admin.ch).

Official register of all licensed medical professionals in Switzerland.
Provides GLN numbers as unique identifiers — the best merge key in DACH.

After MedReg scrape, cross-references each doctor with FMH Arztsuche
to enrich with fmh_mitglied, Schwerpunkte, Fähigkeitsausweise.
"""

import re
from datetime import datetime, timezone
from base_scraper import BaseScraper, generate_slug
from bs4 import BeautifulSoup

MEDREG_SEARCH_URL = "https://www.medregom.admin.ch/api/Practitioner"
FMH_SEARCH_URL = "https://www.doctorfmh.ch/search"

# Relevant specializations for aesthetic/plastic surgery
RELEVANT_SPECIALIZATIONS = [
    "Plastische, Rekonstruktive und Ästhetische Chirurgie",
    "Chirurgie",
    "Dermatologie und Venerologie",
    "Mund-, Kiefer- und Gesichtschirurgie",
    "Hals-Nasen-Ohren-Heilkunde",
    "Ophthalmologie",
]

CANTON_TO_BUNDESLAND = {
    "ZH": "Zürich", "BE": "Bern", "LU": "Luzern", "UR": "Uri",
    "SZ": "Schwyz", "OW": "Obwalden", "NW": "Nidwalden", "GL": "Glarus",
    "ZG": "Zug", "FR": "Freiburg", "SO": "Solothurn", "BS": "Basel-Stadt",
    "BL": "Basel-Landschaft", "SH": "Schaffhausen", "AR": "Appenzell Ausserrhoden",
    "AI": "Appenzell Innerrhoden", "SG": "St. Gallen", "GR": "Graubünden",
    "AG": "Aargau", "TG": "Thurgau", "TI": "Tessin", "VD": "Waadt",
    "VS": "Wallis", "NE": "Neuenburg", "GE": "Genf", "JU": "Jura",
}


class MedRegScraper(BaseScraper):
    name = "medreg"
    min_delay = 0.3
    max_delay = 0.8

    def __init__(self):
        super().__init__()
        self.inserted_doctors = []  # (arzt_id, gln, vorname, nachname) for FMH cross-ref

    def run(self):
        # Phase 1: Scrape MedReg
        for spec in RELEVANT_SPECIALIZATIONS:
            self.logger.info(f"Searching MedReg for: {spec}")
            try:
                self._search_specialization(spec)
            except Exception as e:
                self.logger.error(f"Failed searching {spec}: {e}")
            self.wait()

        # Phase 2: Cross-reference with FMH
        self.logger.info(f"Cross-referencing {len(self.inserted_doctors)} doctors with FMH...")
        for arzt_id, gln, vorname, nachname in self.inserted_doctors:
            try:
                self._check_fmh(arzt_id, gln, vorname, nachname)
            except Exception as e:
                self.logger.error(f"  FMH check failed for {vorname} {nachname}: {e}")
            self.wait()

        self.finalize()

    def _search_specialization(self, specialization: str):
        """Search MedReg API for doctors with a given specialization."""
        params = {
            "specialityName": specialization,
            "languageCode": "de",
            "pageSize": 200,
            "pageIndex": 0,
        }

        while True:
            resp = self.fetch(MEDREG_SEARCH_URL, params=params)
            if not resp:
                break

            try:
                data = resp.json()
            except Exception:
                self.logger.error("Failed to parse MedReg JSON response")
                break

            practitioners = data if isinstance(data, list) else data.get("data", data.get("practitioners", []))
            if not practitioners:
                break

            self.logger.info(f"  Page {params['pageIndex']}: {len(practitioners)} results")

            for practitioner in practitioners:
                try:
                    self._process_practitioner(practitioner, specialization)
                except Exception as e:
                    self.logger.error(f"  Failed processing practitioner: {e}")

            if len(practitioners) < params["pageSize"]:
                break
            params["pageIndex"] += 1
            self.wait()

    def _process_practitioner(self, data: dict, specialization: str):
        """Process a single practitioner from MedReg API response."""
        gln = str(data.get("glnNumber", data.get("gln", "")))
        if not gln or gln == "None":
            return

        vorname = data.get("firstName", data.get("vorname", ""))
        nachname = data.get("lastName", data.get("name", ""))
        if not vorname or not nachname:
            return

        titel = data.get("title", data.get("titel", ""))
        canton = data.get("canton", data.get("kanton", ""))
        stadt = data.get("city", data.get("ort", ""))
        plz = data.get("zipCode", data.get("plz", ""))
        bundesland = CANTON_TO_BUNDESLAND.get(canton, canton)
        zsr = data.get("zsrNumber", data.get("zsr", ""))
        status = data.get("status", data.get("bewilligungsstatus", ""))

        arzt_data = {
            "vorname": vorname,
            "nachname": nachname,
            "titel": titel or "",
            "geschlecht": self._map_gender(data.get("gender", data.get("geschlecht"))),
            "ist_facharzt": True,
            "facharzttitel": specialization,
            "selbstbezeichnung": specialization,
            "approbation_verifiziert": True,
            "land": "CH",
            "stadt": stadt or None,
            "bundesland": bundesland or None,
            "plz": str(plz) if plz else None,
            "datenquelle": "medreg",
            "quelle_url": MEDREG_SEARCH_URL,
            "gln_nummer": gln,
            "zsr_nummer": zsr or None,
            "verified": True,
            "source": "medreg",
            "source_type": "official",
            "last_verified_at": datetime.now(timezone.utc).isoformat(),
        }

        arzt_id = self.upsert_arzt(arzt_data)
        if not arzt_id:
            return

        self.inserted_doctors.append((arzt_id, gln, vorname, nachname))

        kategorie = self._map_kategorie(specialization)
        if kategorie:
            self.insert_spezialisierungen(arzt_id, [{
                "kategorie": kategorie,
                "eingriff": specialization,
                "erfahrungslevel": "spezialist",
            }])

    def _check_fmh(self, arzt_id: int, gln: str, vorname: str, nachname: str):
        """Cross-reference a doctor with FMH Arztsuche to check membership."""
        # Try FMH search by name
        params = {
            "name": f"{vorname} {nachname}",
            "language": "de",
        }
        resp = self.fetch(FMH_SEARCH_URL, params=params)
        if not resp:
            return

        # Try JSON response first
        try:
            data = resp.json()
            results = data if isinstance(data, list) else data.get("results", data.get("data", []))
            for result in results:
                result_gln = str(result.get("gln", result.get("glnNumber", "")))
                if result_gln == gln:
                    # Confirmed FMH member
                    self.enrich_arzt(arzt_id, {"fmh_mitglied": True})
                    self.logger.info(f"  FMH confirmed: {vorname} {nachname} (GLN={gln})")
                    return
        except Exception:
            pass

        # Fallback: parse HTML
        try:
            soup = BeautifulSoup(resp.text, "lxml")
            page_text = soup.get_text(" ", strip=True)
            if gln in page_text or (nachname.lower() in page_text.lower() and vorname.lower() in page_text.lower()):
                self.enrich_arzt(arzt_id, {"fmh_mitglied": True})
                self.logger.info(f"  FMH confirmed (HTML): {vorname} {nachname}")
                return
        except Exception:
            pass

        # Not found in FMH
        self.enrich_arzt(arzt_id, {"fmh_mitglied": False})

    def _map_gender(self, gender) -> str | None:
        if not gender:
            return None
        g = str(gender).lower()
        if g in ("m", "male", "männlich", "1"):
            return "m"
        if g in ("f", "w", "female", "weiblich", "2"):
            return "w"
        return "d"

    def _map_kategorie(self, specialization: str) -> str | None:
        s = specialization.lower()
        if "plastisch" in s or "ästhetisch" in s:
            return "koerper"
        if "dermatolog" in s:
            return "minimal_invasiv"
        if "mund" in s or "kiefer" in s or "gesicht" in s:
            return "gesicht"
        if "hals" in s or "nasen" in s or "ohren" in s:
            return "gesicht"
        if "ophthalmo" in s:
            return "gesicht"
        return None


if __name__ == "__main__":
    scraper = MedRegScraper()
    try:
        scraper.run()
    finally:
        scraper.close()
