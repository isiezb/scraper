"""Scraper for VDÄPC (Vereinigung der Deutschen Ästhetisch-Plastischen Chirurgen).

Fetches all members from the VDÄPC WordPress Store Locator API in a single request.
Unlike the DGPRÄC enrichment scraper, this one CREATES new records for members
not already in the database, in addition to enriching existing ones.
"""

import re
from base_scraper import BaseScraper

# Title keywords to extract from the "store" field
TITEL_KEYWORDS = {
    "prof", "prof.", "dr", "dr.", "med", "med.", "pd", "priv.-doz",
    "priv.-doz.", "univ", "univ.", "dent", "dent.", "dipl", "dipl.",
    "habil", "habil.", "msc", "m.sc.", "ph.d", "ph.d.", "mba",
}

# Map German country names to ISO codes
COUNTRY_MAP = {
    "deutschland": "DE",
    "germany": "DE",
    "de": "DE",
    "schweiz": "CH",
    "switzerland": "CH",
    "ch": "CH",
    "österreich": "AT",
    "austria": "AT",
    "at": "AT",
}

BUNDESLAND_MAP = {
    "berlin": "Berlin",
    "hamburg": "Hamburg",
    "bremen": "Bremen",
    "münchen": "Bayern", "nürnberg": "Bayern", "augsburg": "Bayern",
    "regensburg": "Bayern", "würzburg": "Bayern", "erlangen": "Bayern",
    "rosenheim": "Bayern", "ingolstadt": "Bayern",
    "köln": "Nordrhein-Westfalen", "düsseldorf": "Nordrhein-Westfalen",
    "bonn": "Nordrhein-Westfalen", "essen": "Nordrhein-Westfalen",
    "dortmund": "Nordrhein-Westfalen", "bochum": "Nordrhein-Westfalen",
    "münster": "Nordrhein-Westfalen", "aachen": "Nordrhein-Westfalen",
    "bielefeld": "Nordrhein-Westfalen", "wuppertal": "Nordrhein-Westfalen",
    "duisburg": "Nordrhein-Westfalen", "mönchengladbach": "Nordrhein-Westfalen",
    "frankfurt": "Hessen", "wiesbaden": "Hessen", "kassel": "Hessen",
    "darmstadt": "Hessen", "bad homburg": "Hessen",
    "stuttgart": "Baden-Württemberg", "karlsruhe": "Baden-Württemberg",
    "freiburg": "Baden-Württemberg", "heidelberg": "Baden-Württemberg",
    "mannheim": "Baden-Württemberg", "ulm": "Baden-Württemberg",
    "tübingen": "Baden-Württemberg", "reutlingen": "Baden-Württemberg",
    "konstanz": "Baden-Württemberg", "heilbronn": "Baden-Württemberg",
    "hannover": "Niedersachsen", "braunschweig": "Niedersachsen",
    "oldenburg": "Niedersachsen", "osnabrück": "Niedersachsen",
    "göttingen": "Niedersachsen", "wolfsburg": "Niedersachsen",
    "dresden": "Sachsen", "leipzig": "Sachsen", "chemnitz": "Sachsen",
    "kiel": "Schleswig-Holstein", "lübeck": "Schleswig-Holstein",
    "flensburg": "Schleswig-Holstein",
    "mainz": "Rheinland-Pfalz", "koblenz": "Rheinland-Pfalz",
    "trier": "Rheinland-Pfalz", "ludwigshafen": "Rheinland-Pfalz",
    "saarbrücken": "Saarland",
    "erfurt": "Thüringen", "jena": "Thüringen", "weimar": "Thüringen",
    "potsdam": "Brandenburg",
    "magdeburg": "Sachsen-Anhalt", "halle": "Sachsen-Anhalt",
    "schwerin": "Mecklenburg-Vorpommern", "rostock": "Mecklenburg-Vorpommern",
}


def _parse_name(store_field: str) -> dict:
    """Parse the 'store' field into titel, vorname, nachname.

    Examples:
        "Prof. Dr. med. Max Mustermann" -> titel="Prof. Dr. med.", vorname="Max", nachname="Mustermann"
        "Dr. Anna Maria von Berg" -> titel="Dr.", vorname="Anna Maria", nachname="von Berg"
    """
    words = store_field.strip().split()
    titel_parts = []
    name_parts = []
    in_name = False

    for word in words:
        clean = word.lower().rstrip(".,")
        if not in_name and clean in TITEL_KEYWORDS:
            titel_parts.append(word)
        else:
            in_name = True
            name_parts.append(word)

    if len(name_parts) < 2:
        return {"titel": " ".join(titel_parts), "vorname": "", "nachname": store_field.strip()}

    # Handle "von", "van", "de" etc. as part of nachname
    nobility = {"von", "van", "de", "zu", "vom", "zum", "zur", "ten", "ter"}
    # Find where nachname starts — last name is everything after first name(s)
    # If a nobility particle exists, nachname starts there
    nachname_start = len(name_parts) - 1
    for i, part in enumerate(name_parts):
        if i > 0 and part.lower() in nobility:
            nachname_start = i
            break

    # If no nobility particle, nachname is just the last word
    if nachname_start == len(name_parts) - 1:
        vorname = " ".join(name_parts[:-1])
        nachname = name_parts[-1]
    else:
        vorname = " ".join(name_parts[:nachname_start])
        nachname = " ".join(name_parts[nachname_start:])

    return {
        "titel": " ".join(titel_parts),
        "vorname": vorname,
        "nachname": nachname,
    }


def _guess_bundesland(city: str) -> str | None:
    if not city:
        return None
    return BUNDESLAND_MAP.get(city.lower().strip())


def _normalize_country(country_str: str) -> str:
    if not country_str:
        return "DE"
    return COUNTRY_MAP.get(country_str.lower().strip(), "DE")


def _clean_url(url: str) -> str | None:
    if not url or not url.strip():
        return None
    url = url.strip()
    if not url.startswith("http"):
        url = "https://" + url
    return url


def _clean_phone(phone: str) -> str | None:
    if not phone or not phone.strip():
        return None
    return phone.strip()


class VDAEPCScraper(BaseScraper):
    name = "vdaepc"
    min_delay = 0.5
    max_delay = 1.0

    API_URL = (
        "https://www.vdaepc.de/wp-admin/admin-ajax.php"
        "?action=store_search&lat=50.0&lng=10.0"
        "&max_results=1000&search_radius=9999&autoload=1"
    )

    def run(self):
        progress_key = "vdaepc_members"
        _, completed = self.get_progress(progress_key)
        if completed:
            self.logger.info("VDÄPC already completed, skipping")
            self.finalize()
            return

        self.logger.info("Fetching VDÄPC member directory...")
        resp = self.fetch(self.API_URL)
        if not resp:
            self.logger.error("Failed to fetch VDÄPC API")
            return

        try:
            members = resp.json()
        except Exception as e:
            self.logger.error(f"Failed to parse VDÄPC JSON: {e}")
            return

        if not isinstance(members, list):
            self.logger.error(f"Unexpected VDÄPC response type: {type(members)}")
            return

        self.logger.info(f"Found {len(members)} VDÄPC members")

        for member in members:
            try:
                self._process_member(member)
            except Exception as e:
                self.logger.error(f"Failed processing member: {e}")

        self.save_progress(progress_key, len(members), completed=True)
        self.finalize()

    def _process_member(self, member: dict):
        store = member.get("store", "").strip()
        if not store:
            return

        name = _parse_name(store)
        if not name["vorname"] or not name["nachname"]:
            self.logger.warning(f"Could not parse name: {store}")
            return

        city = member.get("city", "").strip()
        country = _normalize_country(member.get("country", ""))

        # Skip non-DE members (user only wants Germany for now)
        if country != "DE":
            self.logger.info(f"Skipping non-DE member: {store} ({city}, {country})")
            return

        # Validate specialty — VDÄPC is a plastic surgery society but some
        # members may list a different primary Facharzttitel (e.g. MKG)
        drspec = member.get("drspec", "").strip()
        if drspec and not any(kw in drspec.lower() for kw in ["plastisch", "ästhetisch", "aesthetisch", "plastic"]):
            # Has a specialty but it's not plastic surgery — still save since
            # VDÄPC membership implies plastic surgery involvement
            facharzttitel = drspec
        else:
            facharzttitel = drspec or "Facharzt für Plastische und Ästhetische Chirurgie"

        data = {
            "vorname": name["vorname"],
            "nachname": name["nachname"],
            "titel": name["titel"],
            "ist_facharzt": True,
            "facharzttitel": facharzttitel,
            "selbstbezeichnung": drspec or None,
            "plz": member.get("zip", "").strip() or None,
            "stadt": city or None,
            "bundesland": _guess_bundesland(city),
            "land": country,
            "strasse": member.get("address", "").strip() or None,
            "telefon": _clean_phone(member.get("phone", "")),
            "email": member.get("email", "").strip() or None,
            "fax": _clean_phone(member.get("fax", "")),
            "website_url": _clean_url(member.get("url", "")),
            "verified": True,
            "source": "vdaepc",
            "source_type": "professional_association",
            "quelle_url": "https://www.vdaepc.de/service-informationen/arztsuche/",
            "vdaepc_mitglied": True,
        }

        # Try lat/lng
        try:
            lat = float(member.get("lat", 0))
            lng = float(member.get("lng", 0))
            if lat and lng:
                data["latitude"] = lat
                data["longitude"] = lng
        except (ValueError, TypeError):
            pass

        arzt_id = self.upsert_arzt(data)

        # Also record in mitgliedschaften table
        if arzt_id:
            self.upsert_mitgliedschaft(
                arzt_id,
                "Vereinigung der Deutschen Ästhetisch-Plastischen Chirurgen",
                status="Mitglied",
                verifiziert=True,
                quelle_url="https://www.vdaepc.de/service-informationen/arztsuche/",
            )

        self.logger.info(
            f"{'Updated' if arzt_id else 'Skipped'}: {name['titel']} {name['vorname']} {name['nachname']} ({city})"
        )


if __name__ == "__main__":
    scraper = VDAEPCScraper()
    try:
        scraper.run()
    finally:
        scraper.close()
