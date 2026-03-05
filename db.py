"""Database connection and schema setup for PostgreSQL (Supabase)."""

import os
import re
import psycopg2
from psycopg2.extras import RealDictCursor

DATABASE_URL = os.environ["DATABASE_URL"]


def get_conn():
    """Connect using explicit params to handle dots in pooler usernames and brackets in passwords."""
    if DATABASE_URL.startswith("postgresql://") or DATABASE_URL.startswith("postgres://"):
        # Use regex to parse — urlparse chokes on brackets in passwords (treats as IPv6)
        m = re.match(
            r"postgres(?:ql)?://([^:]+):(.+)@([^:/?]+):?(\d+)?/(.+)",
            DATABASE_URL,
        )
        if m:
            user, password, host, port, dbname = m.groups()
            # Strip brackets if present (Supabase uses [password] format)
            password = password.strip("[]")
            return psycopg2.connect(
                host=host,
                port=int(port) if port else 5432,
                user=user,
                password=password,
                dbname=dbname,
                sslmode="require",
            )
    # Keyword format: host=... port=... user=... password=... dbname=...
    return psycopg2.connect(DATABASE_URL)


def init_db():
    """Create all tables if they don't exist."""
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("""
        CREATE TABLE IF NOT EXISTS kliniken (
            id SERIAL PRIMARY KEY,
            name TEXT NOT NULL,
            typ TEXT CHECK(typ IN ('privatklinik_108','privatklinik_30','praxis','schoenheitskette')),
            website_url TEXT,
            land TEXT DEFAULT 'DE',
            stadt TEXT,
            bundesland TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            tuev_zertifiziert BOOLEAN DEFAULT FALSE,
            fallzahlen_plastik INTEGER,
            google_rating DOUBLE PRECISION,
            google_reviews_count INTEGER,
            impressum_gmbh BOOLEAN DEFAULT FALSE,
            handelsregister_nr TEXT
        );

        CREATE TABLE IF NOT EXISTS aerzte (
            id SERIAL PRIMARY KEY,
            vorname TEXT NOT NULL,
            nachname TEXT NOT NULL,
            titel TEXT,
            geschlecht TEXT CHECK(geschlecht IN ('m','w','d')),
            ist_facharzt BOOLEAN DEFAULT FALSE,
            facharzttitel TEXT,
            selbstbezeichnung TEXT,
            approbation_verifiziert BOOLEAN DEFAULT FALSE,
            kammer_id TEXT,
            approbation_jahr INTEGER,
            facharzt_seit_jahr INTEGER,
            klinik_id INTEGER REFERENCES kliniken(id),
            position TEXT,
            land TEXT DEFAULT 'DE',
            stadt TEXT,
            bundesland TEXT,
            plz TEXT,
            latitude DOUBLE PRECISION,
            longitude DOUBLE PRECISION,
            seo_slug TEXT UNIQUE NOT NULL,
            website_url TEXT,
            datenquelle TEXT,
            letzte_aktualisierung TIMESTAMP DEFAULT NOW(),
            -- Unique identifiers from official sources
            gln_nummer TEXT UNIQUE,
            zsr_nummer TEXT,
            kammer_mitgliedsnr TEXT,
            arztsuche_id TEXT,
            -- Country-specific status
            gkv_zugelassen BOOLEAN,
            kassenstatus_at TEXT,
            kammer_region TEXT,
            -- Verification metadata
            verified BOOLEAN DEFAULT FALSE,
            source TEXT,
            source_type TEXT CHECK(source_type IN ('official','professional_association')),
            last_verified_at TIMESTAMP,
            -- Disambiguation fields
            geburtsjahr INTEGER,
            telefon TEXT,
            -- Membership booleans (enrichment from societies)
            fmh_mitglied BOOLEAN,
            dgpraec_mitglied BOOLEAN,
            dgaepc_mitglied BOOLEAN,
            vdaepc_mitglied BOOLEAN,
            isaps_mitglied BOOLEAN,
            -- Collision tracking
            name_collision BOOLEAN DEFAULT FALSE,
            collision_group TEXT,
            collision_resolved BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS spezialisierungen (
            id SERIAL PRIMARY KEY,
            arzt_id INTEGER NOT NULL REFERENCES aerzte(id),
            kategorie TEXT CHECK(kategorie IN ('brust','gesicht','koerper','minimal_invasiv')),
            eingriff TEXT NOT NULL,
            erfahrungslevel TEXT CHECK(erfahrungslevel IN ('basis','fortgeschritten','spezialist'))
        );

        CREATE TABLE IF NOT EXISTS werdegang (
            id SERIAL PRIMARY KEY,
            arzt_id INTEGER NOT NULL REFERENCES aerzte(id),
            typ TEXT CHECK(typ IN ('studium','klinik','weiterbildung','promotion','zertifikat')),
            institution TEXT,
            stadt TEXT,
            land TEXT,
            von_jahr INTEGER,
            bis_jahr INTEGER,
            beschreibung TEXT,
            verifiziert BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS mitgliedschaften (
            id SERIAL PRIMARY KEY,
            arzt_id INTEGER NOT NULL REFERENCES aerzte(id),
            gesellschaft TEXT NOT NULL,
            mitglied_seit_jahr INTEGER,
            mitgliedsstatus TEXT,
            verifiziert BOOLEAN DEFAULT FALSE,
            quelle_url TEXT
        );

        CREATE TABLE IF NOT EXISTS promotionen (
            id SERIAL PRIMARY KEY,
            arzt_id INTEGER NOT NULL UNIQUE REFERENCES aerzte(id),
            titel TEXT,
            thema TEXT,
            universitaet TEXT,
            jahr INTEGER,
            repository_url TEXT,
            verifiziert BOOLEAN DEFAULT FALSE
        );

        CREATE TABLE IF NOT EXISTS preise (
            id SERIAL PRIMARY KEY,
            arzt_id INTEGER NOT NULL REFERENCES aerzte(id),
            eingriff TEXT NOT NULL,
            preis_von DOUBLE PRECISION,
            preis_bis DOUBLE PRECISION,
            waehrung TEXT DEFAULT 'EUR',
            quelle TEXT CHECK(quelle IN ('website','patient','selbstangabe'))
        );

        CREATE TABLE IF NOT EXISTS online_praesenz (
            id SERIAL PRIMARY KEY,
            arzt_id INTEGER NOT NULL REFERENCES aerzte(id),
            plattform TEXT CHECK(plattform IN ('instagram','youtube','tiktok','doctolib')),
            handle TEXT,
            url TEXT,
            follower INTEGER,
            letzte_aktivitaet TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS scraper_log (
            id SERIAL PRIMARY KEY,
            quelle TEXT NOT NULL,
            ziel_url TEXT,
            status TEXT CHECK(status IN ('ok','fehler','rate_limit','blocked')),
            eintraege_neu INTEGER DEFAULT 0,
            eintraege_aktualisiert INTEGER DEFAULT 0,
            laufzeit_ms INTEGER,
            zeitstempel TIMESTAMP DEFAULT NOW()
        );

        CREATE INDEX IF NOT EXISTS idx_aerzte_slug ON aerzte(seo_slug);
        CREATE INDEX IF NOT EXISTS idx_aerzte_stadt ON aerzte(stadt);
        CREATE INDEX IF NOT EXISTS idx_aerzte_bundesland ON aerzte(bundesland);
        CREATE INDEX IF NOT EXISTS idx_aerzte_facharzt ON aerzte(ist_facharzt);
        CREATE INDEX IF NOT EXISTS idx_aerzte_land ON aerzte(land);
        CREATE INDEX IF NOT EXISTS idx_aerzte_gln ON aerzte(gln_nummer);
        CREATE INDEX IF NOT EXISTS idx_aerzte_kammer_nr ON aerzte(kammer_mitgliedsnr);
        CREATE INDEX IF NOT EXISTS idx_aerzte_arztsuche_id ON aerzte(arztsuche_id);
        CREATE INDEX IF NOT EXISTS idx_aerzte_verified ON aerzte(verified);
        CREATE INDEX IF NOT EXISTS idx_aerzte_source ON aerzte(source);
        CREATE INDEX IF NOT EXISTS idx_aerzte_collision ON aerzte(name_collision) WHERE name_collision = TRUE;
        CREATE INDEX IF NOT EXISTS idx_aerzte_collision_group ON aerzte(collision_group);
        CREATE INDEX IF NOT EXISTS idx_spez_arzt ON spezialisierungen(arzt_id);
        CREATE INDEX IF NOT EXISTS idx_spez_eingriff ON spezialisierungen(eingriff);
    """)

    # Migration for existing databases: add new columns if missing
    migration_cols = [
        ("gln_nummer", "TEXT UNIQUE"),
        ("zsr_nummer", "TEXT"),
        ("kammer_mitgliedsnr", "TEXT"),
        ("arztsuche_id", "TEXT"),
        ("gkv_zugelassen", "BOOLEAN"),
        ("kassenstatus_at", "TEXT"),
        ("kammer_region", "TEXT"),
        ("verified", "BOOLEAN DEFAULT FALSE"),
        ("source", "TEXT"),
        ("source_type", "TEXT"),
        ("last_verified_at", "TIMESTAMP"),
        ("geburtsjahr", "INTEGER"),
        ("telefon", "TEXT"),
        ("fmh_mitglied", "BOOLEAN"),
        ("dgpraec_mitglied", "BOOLEAN"),
        ("dgaepc_mitglied", "BOOLEAN"),
        ("vdaepc_mitglied", "BOOLEAN"),
        ("isaps_mitglied", "BOOLEAN"),
        ("name_collision", "BOOLEAN DEFAULT FALSE"),
        ("collision_group", "TEXT"),
        ("collision_resolved", "BOOLEAN DEFAULT FALSE"),
    ]
    for col_name, col_type in migration_cols:
        try:
            cur.execute(f"ALTER TABLE aerzte ADD COLUMN {col_name} {col_type}")
            conn.commit()
        except Exception:
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()
