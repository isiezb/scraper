"""Base scraper with shared logic: rate limiting, DB helpers, merge logic, logging."""

import hashlib
import time
import random
import re
import logging
from abc import ABC, abstractmethod

import requests
from db import get_conn

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)

# Title keywords to strip during normalization
TITEL_KEYWORDS = {
    "prof", "prof.", "dr", "dr.", "med", "med.", "pd", "priv.-doz",
    "priv.-doz.", "univ", "univ.", "dent", "dent.", "dipl", "dipl.",
    "habil", "habil.", "msc", "m.sc.", "ph.d", "ph.d.",
}

# Umlaut mapping for comparison (not for storage)
UMLAUT_MAP = {
    "ä": "ae", "ö": "oe", "ü": "ue", "ß": "ss",
    "Ä": "Ae", "Ö": "Oe", "Ü": "Ue",
    "é": "e", "è": "e", "ê": "e", "à": "a", "â": "a",
    "î": "i", "ô": "o", "û": "u", "ç": "c",
}


def normalize_name(vorname: str, nachname: str) -> str:
    """Normalize a name for comparison purposes.

    Strips titles, normalizes umlauts, lowercases, handles double names.
    Returns format: "nachname,vorname" (no spaces around comma).
    """
    def clean(name: str) -> str:
        # Remove title keywords
        words = name.split()
        cleaned = []
        for w in words:
            if w.lower().rstrip(".,") in TITEL_KEYWORDS:
                continue
            cleaned.append(w)
        name = " ".join(cleaned)
        # Normalize umlauts
        for orig, repl in UMLAUT_MAP.items():
            name = name.replace(orig, repl)
        # Lowercase, strip punctuation except hyphens
        name = name.lower().strip()
        name = re.sub(r"[^a-z0-9\s-]", "", name)
        name = re.sub(r"\s+", " ", name).strip()
        return name

    return f"{clean(nachname)},{clean(vorname)}"


def generate_slug(titel: str, vorname: str, nachname: str) -> str:
    """Generate SEO slug: dr-max-mustermann."""
    parts = []
    if titel:
        parts.append(titel.lower().replace(".", "").replace(" ", "-"))
    parts.append(vorname.lower())
    parts.append(nachname.lower())
    slug = "-".join(parts)
    for orig, repl in UMLAUT_MAP.items():
        slug = slug.replace(orig.lower(), repl.lower())
    slug = re.sub(r"[^a-z0-9-]", "", slug)
    slug = re.sub(r"-+", "-", slug).strip("-")
    return slug


def collision_group_hash(normalized_name: str, plz: str) -> str:
    """Generate a hash for collision grouping."""
    key = f"{normalized_name}|{plz or ''}"
    return hashlib.sha256(key.encode()).hexdigest()[:16]


# All columns that can be set on an aerzte record
ALL_AERZTE_COLUMNS = [
    "vorname", "nachname", "titel", "geschlecht",
    "ist_facharzt", "facharzttitel", "selbstbezeichnung",
    "approbation_verifiziert", "kammer_id", "approbation_jahr",
    "facharzt_seit_jahr", "land", "stadt", "bundesland", "plz",
    "latitude", "longitude",
    "seo_slug", "website_url", "datenquelle",
    "gln_nummer", "zsr_nummer", "kammer_mitgliedsnr", "arztsuche_id",
    "gkv_zugelassen", "kassenstatus_at", "kammer_region",
    "verified", "source", "source_type", "last_verified_at",
    "geburtsjahr", "telefon", "email", "fax", "strasse", "schwerpunkte",
    "fmh_mitglied", "dgpraec_mitglied", "dgaepc_mitglied",
    "vdaepc_mitglied", "isaps_mitglied",
    "name_collision", "collision_group", "collision_resolved",
    "quelle_url",
]

# Columns that can be updated on existing records
# NOTE: source and source_type are intentionally excluded — they should only
# be set on insert so the original source is preserved across scraper runs.
UPDATABLE_COLUMNS = [
    "ist_facharzt", "facharzttitel", "selbstbezeichnung",
    "approbation_verifiziert", "kammer_id", "land", "stadt",
    "bundesland", "plz", "latitude", "longitude",
    "website_url", "datenquelle",
    "gln_nummer", "zsr_nummer", "kammer_mitgliedsnr", "arztsuche_id",
    "gkv_zugelassen", "kassenstatus_at", "kammer_region",
    "verified", "last_verified_at",
    "geburtsjahr", "telefon", "email", "fax", "strasse", "schwerpunkte",
    "fmh_mitglied", "dgpraec_mitglied", "dgaepc_mitglied",
    "vdaepc_mitglied", "isaps_mitglied",
    "quelle_url",
]


class BaseScraper(ABC):
    name: str = "base"
    min_delay: float = 0.5
    max_delay: float = 1.5

    def __init__(self):
        self.logger = logging.getLogger(self.name)
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": random.choice(USER_AGENTS),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "de-DE,de;q=0.9,en;q=0.5",
        })
        self.conn = get_conn()
        self.stats = {"neu": 0, "aktualisiert": 0, "kollisionen": 0, "uebersprungen": 0}

    def close(self):
        self.conn.close()
        self.session.close()

    # ── Progress tracking (resume support) ────────────────────────────

    def get_progress(self, source_key: str) -> tuple[int, bool]:
        """Get last offset and completed status for a source key.
        Returns (last_offset, completed)."""
        cur = self.conn.cursor()
        cur.execute(
            "SELECT last_offset, completed FROM scraper_progress WHERE scraper = %s AND source_key = %s",
            (self.name, source_key),
        )
        row = cur.fetchone()
        cur.close()
        if row:
            return row[0], row[1] or False
        return 0, False

    def save_progress(self, source_key: str, offset: int, completed: bool = False):
        """Save scraping progress for resume."""
        cur = self.conn.cursor()
        cur.execute("""
            INSERT INTO scraper_progress (scraper, source_key, last_offset, completed, updated_at)
            VALUES (%s, %s, %s, %s, NOW())
            ON CONFLICT (scraper, source_key) DO UPDATE
            SET last_offset = EXCLUDED.last_offset, completed = EXCLUDED.completed, updated_at = NOW()
        """, (self.name, source_key, offset, completed))
        self.conn.commit()
        cur.close()

    def wait(self):
        delay = random.uniform(self.min_delay, self.max_delay)
        self.logger.debug(f"Waiting {delay:.1f}s...")
        time.sleep(delay)

    def fetch(self, url: str, **kwargs) -> requests.Response | None:
        start = time.time()
        try:
            self.session.headers["User-Agent"] = random.choice(USER_AGENTS)
            resp = self.session.get(url, timeout=30, **kwargs)
            resp.raise_for_status()
            self._log_request(url, "ok", int((time.time() - start) * 1000))
            return resp
        except requests.RequestException as e:
            status = "rate_limit" if hasattr(e, "response") and e.response and e.response.status_code == 429 else "fehler"
            self._log_request(url, status, int((time.time() - start) * 1000))
            self.logger.error(f"Request failed for {url}: {e}")
            return None

    def _log_request(self, url: str, status: str, laufzeit_ms: int):
        cur = self.conn.cursor()
        cur.execute(
            "INSERT INTO scraper_log (quelle, ziel_url, status, laufzeit_ms) VALUES (%s, %s, %s, %s)",
            (self.name, url, status, laufzeit_ms),
        )
        self.conn.commit()
        cur.close()

    # ── Merge logic ──────────────────────────────────────────────────

    def find_matching_doctor(self, data: dict) -> tuple[int | None, bool | None]:
        """Find an existing doctor record matching the candidate data.

        Returns (arzt_id, existing_verified) or (None, None) if no match.
        May flag a collision and return (None, None) if ambiguous.

        Merge hierarchy:
        1. gln_nummer — unique, state-issued (CH only)
        2. kammer_mitgliedsnr — if provided by chamber
        3. arztsuche_id — internal ID from source site
        4. Name + telefon — strong additional signal
        5. Name + PLZ + geburtsjahr/approbationsjahr — tiebreaker
        Name + PLZ alone → collision, no auto-merge.
        """
        cur = self.conn.cursor()

        # Stage 1: Unique ID lookups
        if data.get("gln_nummer"):
            cur.execute("SELECT id, verified FROM aerzte WHERE gln_nummer = %s", (data["gln_nummer"],))
            row = cur.fetchone()
            if row:
                cur.close()
                return row[0], row[1] or False

        if data.get("kammer_mitgliedsnr"):
            cur.execute("SELECT id, verified FROM aerzte WHERE kammer_mitgliedsnr = %s", (data["kammer_mitgliedsnr"],))
            row = cur.fetchone()
            if row:
                cur.close()
                return row[0], row[1] or False

        if data.get("arztsuche_id"):
            cur.execute("SELECT id, verified FROM aerzte WHERE arztsuche_id = %s", (data["arztsuche_id"],))
            row = cur.fetchone()
            if row:
                cur.close()
                return row[0], row[1] or False

        # Stage 2: Name-based matching with disambiguation
        norm_name = normalize_name(data["vorname"], data["nachname"])
        # Find all records with matching normalized name
        cur.execute(
            """SELECT id, verified, plz, telefon, geburtsjahr, approbation_jahr
               FROM aerzte
               WHERE LOWER(nachname) = LOWER(%s) AND LOWER(vorname) = LOWER(%s)""",
            (data["nachname"], data["vorname"]),
        )
        name_matches = cur.fetchall()

        if not name_matches:
            cur.close()
            return None, None

        # Name + telefon → secure match
        if data.get("telefon"):
            for m in name_matches:
                if m[3] and m[3] == data["telefon"]:
                    cur.close()
                    return m[0], m[1] or False

        # Name + PLZ + geburtsjahr → secure match
        candidate_plz = data.get("plz")
        if candidate_plz:
            plz_matches = [m for m in name_matches if m[2] == candidate_plz]

            if plz_matches:
                # Try geburtsjahr tiebreaker
                if data.get("geburtsjahr"):
                    for m in plz_matches:
                        if m[4] and m[4] == data["geburtsjahr"]:
                            cur.close()
                            return m[0], m[1] or False

                # Try approbationsjahr tiebreaker
                if data.get("approbation_jahr"):
                    for m in plz_matches:
                        if m[5] and m[5] == data["approbation_jahr"]:
                            cur.close()
                            return m[0], m[1] or False

                # Exactly 1 name+PLZ match → safe to update (same person)
                if len(plz_matches) == 1:
                    cur.close()
                    return plz_matches[0][0], plz_matches[0][1] or False

                # Multiple name+PLZ matches → true COLLISION
                cg = collision_group_hash(norm_name, candidate_plz)
                self._flag_collision(cur, plz_matches, cg)
                self.stats["kollisionen"] += 1
                self.logger.warning(
                    f"Collision: {data.get('titel', '')} {data['vorname']} {data['nachname']} PLZ={candidate_plz} "
                    f"({len(plz_matches)} existing match(es))"
                )
                data["name_collision"] = True
                data["collision_group"] = cg
                data["collision_resolved"] = False
                cur.close()
                return None, None

        # No PLZ on candidate — try city matching
        if data.get("stadt") and len(name_matches) > 1:
            cur2 = self.conn.cursor()
            cur2.execute(
                """SELECT id, verified FROM aerzte
                   WHERE LOWER(nachname) = LOWER(%s) AND LOWER(vorname) = LOWER(%s)
                   AND LOWER(stadt) = LOWER(%s)""",
                (data["nachname"], data["vorname"], data["stadt"]),
            )
            city_matches = cur2.fetchall()
            cur2.close()
            if len(city_matches) == 1:
                cur.close()
                return city_matches[0][0], city_matches[0][1] or False

        # If exactly 1 name match exists, safe to update
        if len(name_matches) == 1:
            cur.close()
            return name_matches[0][0], name_matches[0][1] or False

        cur.close()
        return None, None

    def _flag_collision(self, cur, existing_matches: list, collision_group: str):
        """Mark existing records as part of a collision group."""
        for m in existing_matches:
            arzt_id = m[0]
            cur.execute(
                "UPDATE aerzte SET name_collision = TRUE, collision_group = %s WHERE id = %s AND (collision_resolved IS NULL OR collision_resolved = FALSE)",
                (collision_group, arzt_id),
            )
        self.conn.commit()

    # ── Upsert ───────────────────────────────────────────────────────

    def upsert_arzt(self, data: dict) -> int | None:
        """Insert or update a doctor using the full merge logic.

        Returns arzt_id. Uses find_matching_doctor() for safe merging.
        Verified sources overwrite unverified. Unverified never overwrite verified.
        """
        slug = data.get("seo_slug") or generate_slug(
            data.get("titel", ""), data["vorname"], data["nachname"]
        )
        data["seo_slug"] = slug

        # Try to find a matching existing record
        arzt_id, existing_verified = self.find_matching_doctor(data)
        is_verified_source = data.get("verified", False)

        if arzt_id is not None:
            # Unverified source must not overwrite verified data
            if existing_verified and not is_verified_source:
                self.logger.info(f"Skipped (verified exists): {data.get('vorname')} {data['nachname']} (id={arzt_id})")
                self.stats["uebersprungen"] += 1
                return arzt_id

            # Update existing record
            cur = self.conn.cursor()
            updates = []
            values = []
            for key in UPDATABLE_COLUMNS:
                if key in data and data[key] is not None:
                    updates.append(f"{key} = %s")
                    values.append(data[key])
            if updates:
                updates.append("letzte_aktualisierung = NOW()")
                values.append(arzt_id)
                cur.execute(
                    f"UPDATE aerzte SET {', '.join(updates)} WHERE id = %s",
                    values,
                )
                self.conn.commit()
                self.stats["aktualisiert"] += 1
            cur.close()
            self.logger.info(f"Updated: {data.get('vorname')} {data['nachname']} (id={arzt_id})")
            return arzt_id
        else:
            # Insert new record
            cur = self.conn.cursor()

            # Handle slug collision — keep trying until unique
            cur.execute("SELECT id FROM aerzte WHERE seo_slug = %s", (slug,))
            if cur.fetchone():
                if data.get("plz"):
                    slug = f"{slug}-{data['plz']}"
                else:
                    slug = f"{slug}-{random.randint(100,999)}"
                # Check again after suffix
                cur.execute("SELECT id FROM aerzte WHERE seo_slug = %s", (slug,))
                if cur.fetchone():
                    slug = f"{slug}-{random.randint(1000,9999)}"
                data["seo_slug"] = slug

            try:
                columns = [c for c in ALL_AERZTE_COLUMNS if c in data]
                values = [data[c] for c in columns]
                placeholders = ", ".join(["%s"] * len(columns))
                col_str = ", ".join(columns)
                cur.execute(
                    f"INSERT INTO aerzte ({col_str}) VALUES ({placeholders}) RETURNING id",
                    values,
                )
                arzt_id = cur.fetchone()[0]
                self.conn.commit()
                self.stats["neu"] += 1
                cur.close()
                self.logger.info(f"Inserted: {data.get('vorname')} {data['nachname']} (id={arzt_id})")
                return arzt_id
            except Exception as e:
                self.conn.rollback()
                cur.close()
                self.logger.error(f"Insert failed for {data.get('vorname')} {data['nachname']}: {e}")
                return None

    def enrich_arzt(self, arzt_id: int, enrichment: dict):
        """Update specific fields on an existing doctor without full upsert.

        Used by enrichment-only scrapers (e.g. DGPRÄC) that should never
        create new records or overwrite core fields.
        """
        cur = self.conn.cursor()
        updates = []
        values = []
        # Only allow membership booleans and non-core enrichment fields
        enrichment_fields = [
            "fmh_mitglied", "dgpraec_mitglied", "dgaepc_mitglied",
            "vdaepc_mitglied", "isaps_mitglied",
        ]
        for key in enrichment_fields:
            if key in enrichment and enrichment[key] is not None:
                updates.append(f"{key} = %s")
                values.append(enrichment[key])
        if updates:
            values.append(arzt_id)
            cur.execute(
                f"UPDATE aerzte SET {', '.join(updates)} WHERE id = %s",
                values,
            )
            self.conn.commit()
        cur.close()

    # ── Helper methods ───────────────────────────────────────────────

    def upsert_mitgliedschaft(self, arzt_id: int, gesellschaft: str, **kwargs):
        cur = self.conn.cursor()
        cur.execute(
            "SELECT id FROM mitgliedschaften WHERE arzt_id = %s AND gesellschaft = %s",
            (arzt_id, gesellschaft),
        )
        if not cur.fetchone():
            cur.execute(
                "INSERT INTO mitgliedschaften (arzt_id, gesellschaft, mitglied_seit_jahr, mitgliedsstatus, verifiziert, quelle_url) VALUES (%s, %s, %s, %s, %s, %s)",
                (arzt_id, gesellschaft, kwargs.get("seit"), kwargs.get("status", "Mitglied"), kwargs.get("verifiziert", True), kwargs.get("quelle_url")),
            )
            self.conn.commit()
        cur.close()

    def insert_werdegang(self, arzt_id: int, entries: list[dict]):
        cur = self.conn.cursor()
        for entry in entries:
            cur.execute(
                """INSERT INTO werdegang (arzt_id, typ, institution, stadt, land, von_jahr, bis_jahr, beschreibung, verifiziert)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, FALSE)""",
                (arzt_id, entry.get("typ", "klinik"), entry.get("institution"),
                 entry.get("stadt"), entry.get("land", "DE"),
                 entry.get("von_jahr"), entry.get("bis_jahr"),
                 entry.get("beschreibung")),
            )
        self.conn.commit()
        cur.close()

    def insert_spezialisierungen(self, arzt_id: int, specs: list[dict]):
        cur = self.conn.cursor()
        for spec in specs:
            cur.execute(
                "SELECT id FROM spezialisierungen WHERE arzt_id = %s AND LOWER(eingriff) = LOWER(%s)",
                (arzt_id, spec["eingriff"]),
            )
            if not cur.fetchone():
                cur.execute(
                    "INSERT INTO spezialisierungen (arzt_id, kategorie, eingriff, erfahrungslevel) VALUES (%s, %s, %s, %s)",
                    (arzt_id, spec.get("kategorie", "koerper"), spec["eingriff"],
                     spec.get("erfahrungslevel", "basis")),
                )
        self.conn.commit()
        cur.close()

    def finalize(self):
        self.logger.info(
            f"Done: {self.stats['neu']} new, {self.stats['aktualisiert']} updated, "
            f"{self.stats['kollisionen']} collisions, {self.stats['uebersprungen']} skipped"
        )

    @abstractmethod
    def run(self):
        ...
