"""
db.py — Módulo central de base de datos SQLite.

Gestiona el schema, la conexión y las operaciones de escritura/lectura
sobre data/contratos.db.
"""

import sqlite3
import os
from datetime import datetime

DB_PATH = os.path.join("data", "contratos.db")


# ─── Conexión ─────────────────────────────────────────────────────────────────

def get_connection(db_path: str = DB_PATH) -> sqlite3.Connection:
    """Devuelve una conexión con foreign keys activadas y row_factory."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA journal_mode = WAL")  # Permite lecturas concurrentes
    return conn


# ─── Schema ───────────────────────────────────────────────────────────────────

SCHEMA = """
-- Seguimiento de los trimestres descargados y procesados
CREATE TABLE IF NOT EXISTS trimestres (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    year             INTEGER NOT NULL,
    quarter          INTEGER NOT NULL,
    filename         TEXT    NOT NULL,
    url              TEXT,
    downloaded_at    TEXT,
    processed_at     TEXT,
    num_contratos    INTEGER,
    UNIQUE(year, quarter)
);

-- Dimensión: áreas municipales
CREATE TABLE IF NOT EXISTS areas (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre  TEXT NOT NULL UNIQUE
);

-- Dimensión: tipos de contrato
CREATE TABLE IF NOT EXISTS tipos_contrato (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    nombre  TEXT NOT NULL UNIQUE
);

-- Dimensión: contratistas, unificados por CIF
CREATE TABLE IF NOT EXISTS contratistas (
    cif            TEXT PRIMARY KEY,
    nombre         TEXT,
    direccion      TEXT,
    tipo_entidad   TEXT,   -- SA, SL, Autónomo, Adm. Pública, etc.
    tipo_id        TEXT,   -- NIF, CIF (tipo de identificador fiscal)
    cif_valido     INTEGER DEFAULT 1,  -- 1=válido, 0=dudoso/inválido
    updated_at     TEXT    DEFAULT (datetime('now'))
);

-- Hecho: contratos menores
CREATE TABLE IF NOT EXISTS contratos (
    contrato_id         TEXT PRIMARY KEY,
    cif                 TEXT REFERENCES contratistas(cif),
    area_id             INTEGER REFERENCES areas(id),
    tipo_contrato_id    INTEGER REFERENCES tipos_contrato(id),
    trimestre_id        INTEGER REFERENCES trimestres(id),
    importe             REAL,
    fecha_adjudicacion  TEXT,   -- ISO date: YYYY-MM-DD o NULL
    objeto              TEXT,
    expediente          TEXT,
    year                INTEGER,
    quarter             INTEGER,
    source_file         TEXT,
    raw_row             TEXT,   -- fila original del Excel para auditoría
    created_at          TEXT    DEFAULT (datetime('now'))
);

-- Índices de uso frecuente
CREATE INDEX IF NOT EXISTS idx_contratos_cif        ON contratos(cif);
CREATE INDEX IF NOT EXISTS idx_contratos_area       ON contratos(area_id);
CREATE INDEX IF NOT EXISTS idx_contratos_fecha      ON contratos(fecha_adjudicacion);
CREATE INDEX IF NOT EXISTS idx_contratos_year       ON contratos(year, quarter);
CREATE INDEX IF NOT EXISTS idx_contratistas_nombre  ON contratistas(nombre);
"""


def init_schema(conn: sqlite3.Connection) -> None:
    """Crea las tablas e índices si no existen."""
    conn.executescript(SCHEMA)
    conn.commit()
    print("Schema inicializado correctamente.")


# ─── Helpers de dimensiones ───────────────────────────────────────────────────

def get_or_create_area(conn: sqlite3.Connection, nombre: str) -> int:
    """Devuelve el id del área, creándola si no existe."""
    cur = conn.execute("INSERT OR IGNORE INTO areas(nombre) VALUES (?)", (nombre,))
    conn.commit()
    row = conn.execute("SELECT id FROM areas WHERE nombre = ?", (nombre,)).fetchone()
    return row["id"]


def get_or_create_tipo(conn: sqlite3.Connection, nombre: str) -> int:
    """Devuelve el id del tipo de contrato, creándolo si no existe."""
    conn.execute("INSERT OR IGNORE INTO tipos_contrato(nombre) VALUES (?)", (nombre,))
    conn.commit()
    row = conn.execute("SELECT id FROM tipos_contrato WHERE nombre = ?", (nombre,)).fetchone()
    return row["id"]


def get_or_create_trimestre(
    conn: sqlite3.Connection,
    year: int,
    quarter: int,
    filename: str,
    url: str = None,
) -> int:
    """Devuelve el id del trimestre, creándolo si no existe."""
    conn.execute(
        """
        INSERT OR IGNORE INTO trimestres(year, quarter, filename, url, downloaded_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (year, quarter, filename, url, datetime.now().isoformat()),
    )
    conn.commit()
    row = conn.execute(
        "SELECT id FROM trimestres WHERE year = ? AND quarter = ?", (year, quarter)
    ).fetchone()
    return row["id"]


def upsert_contratista(
    conn: sqlite3.Connection,
    cif: str,
    nombre: str,
    direccion: str,
    tipo_entidad: str,
    tipo_id: str,
    cif_valido: bool = True,
) -> None:
    """Inserta o actualiza un contratista. No sobreescribe campos si ya existen con valor."""
    conn.execute(
        """
        INSERT INTO contratistas(cif, nombre, direccion, tipo_entidad, tipo_id, cif_valido, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(cif) DO UPDATE SET
            nombre       = COALESCE(NULLIF(excluded.nombre, ''), contratistas.nombre),
            direccion    = COALESCE(NULLIF(excluded.direccion, ''), contratistas.direccion),
            tipo_entidad = excluded.tipo_entidad,
            tipo_id      = excluded.tipo_id,
            cif_valido   = excluded.cif_valido,
            updated_at   = datetime('now')
        """,
        (cif, nombre or "", direccion or "", tipo_entidad or "", tipo_id or "", int(cif_valido)),
    )


def insert_contrato(
    conn: sqlite3.Connection,
    contrato_id: str,
    cif: str,
    area_id: int,
    tipo_contrato_id: int,
    trimestre_id: int,
    importe: float,
    fecha_adjudicacion,   # datetime, date, str o None
    objeto: str,
    expediente: str,
    year: int,
    quarter: int,
    source_file: str,
    raw_row: str = None,
) -> None:
    """Inserta un contrato. Si ya existe el contrato_id, lo ignora (idempotente)."""
    # Normalizar fecha a string ISO o None
    if fecha_adjudicacion is not None:
        try:
            fecha_str = str(fecha_adjudicacion)[:10]  # YYYY-MM-DD
            if fecha_str in ("NaT", "None", "nan", ""):
                fecha_str = None
        except Exception:
            fecha_str = None
    else:
        fecha_str = None

    conn.execute(
        """
        INSERT OR IGNORE INTO contratos(
            contrato_id, cif, area_id, tipo_contrato_id, trimestre_id,
            importe, fecha_adjudicacion, objeto, expediente,
            year, quarter, source_file, raw_row
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            contrato_id, cif, area_id, tipo_contrato_id, trimestre_id,
            importe, fecha_str, objeto or "", expediente or "",
            year, quarter, source_file, raw_row,
        ),
    )


def mark_trimestre_procesado(conn: sqlite3.Connection, trimestre_id: int, num_contratos: int) -> None:
    """Actualiza el trimestre con la fecha de procesado y el número de contratos."""
    conn.execute(
        "UPDATE trimestres SET processed_at = ?, num_contratos = ? WHERE id = ?",
        (datetime.now().isoformat(), num_contratos, trimestre_id),
    )
    conn.commit()


# ─── Consultas de uso común ───────────────────────────────────────────────────

def get_trimestres_procesados(conn: sqlite3.Connection) -> set:
    """Devuelve el conjunto de (year, quarter) ya procesados."""
    rows = conn.execute(
        "SELECT year, quarter FROM trimestres WHERE processed_at IS NOT NULL"
    ).fetchall()
    return {(r["year"], r["quarter"]) for r in rows}


def get_stats(conn: sqlite3.Connection) -> dict:
    """Resumen global de la base de datos."""
    row = conn.execute("""
        SELECT
            COUNT(*)           AS total_contratos,
            SUM(importe)       AS total_importe,
            AVG(importe)       AS media_importe,
            MIN(fecha_adjudicacion) AS fecha_min,
            MAX(fecha_adjudicacion) AS fecha_max
        FROM contratos
    """).fetchone()
    return dict(row)
