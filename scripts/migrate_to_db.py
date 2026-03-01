"""
migrate_to_db.py — Migración inicial de JSON procesados a SQLite.

Lee los ficheros JSON ya generados por process_data.py y los carga
en data/contratos.db. Operación idempotente: se puede ejecutar varias
veces sin duplicar datos (INSERT OR IGNORE en contratos, upsert en el resto).

Uso:
    python scripts/migrate_to_db.py
"""

import sys
import os
import json
import re

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import (
    get_connection,
    init_schema,
    get_or_create_area,
    get_or_create_tipo,
    get_or_create_trimestre,
    upsert_contratista,
    insert_contrato,
    mark_trimestre_procesado,
    DB_PATH,
)

PROCESSED_DIR = "data/processed"


def load_json(filename: str):
    path = os.path.join(PROCESSED_DIR, filename)
    if not os.path.exists(path):
        raise FileNotFoundError(f"No se encuentra {path}. Ejecuta process_data.py primero.")
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def migrate():
    print(f"Destino: {DB_PATH}")
    os.makedirs("data", exist_ok=True)

    conn = get_connection()
    init_schema(conn)

    # ── 1. Contratistas ──────────────────────────────────────────────────────
    print("Migrando contratistas...", end=" ", flush=True)
    contratistas = load_json("dim_contractors.json")
    for c in contratistas:
        upsert_contratista(
            conn,
            cif=c.get("cif", ""),
            nombre=c.get("nombre", ""),
            direccion=c.get("direccion", ""),
            tipo_entidad=c.get("tipo_entidad", ""),
            tipo_id=c.get("tipo_id", ""),
            cif_valido=True,
        )
    conn.commit()
    print(f"{len(contratistas)} registros.")

    # ── 2. Áreas ─────────────────────────────────────────────────────────────
    print("Migrando áreas...", end=" ", flush=True)
    areas_data = load_json("dim_areas.json")
    area_id_map: dict[int, int] = {}  # id_original → id_sqlite
    for a in areas_data:
        new_id = get_or_create_area(conn, a["nombre"])
        area_id_map[a["id"]] = new_id
    print(f"{len(areas_data)} registros.")

    # ── 3. Tipos de contrato ──────────────────────────────────────────────────
    print("Migrando tipos de contrato...", end=" ", flush=True)
    tipos_data = load_json("dim_types.json")
    tipo_id_map: dict[int, int] = {}  # id_original → id_sqlite
    for t in tipos_data:
        new_id = get_or_create_tipo(conn, t["nombre"])
        tipo_id_map[t["id"]] = new_id
    print(f"{len(tipos_data)} registros.")

    # ── 4. Trimestres (inferidos del source_file) ─────────────────────────────
    print("Cargando contratos...", end=" ", flush=True)
    fact_contracts = load_json("fact_contracts.json")

    # Construir mapa source_file → trimestre_id
    trimestre_id_map: dict[str, int] = {}
    for c in fact_contracts:
        sf = c.get("source_file", "")
        if sf and sf not in trimestre_id_map:
            match = re.search(r"(\d{4})_Q(\d)", sf)
            if match:
                year, quarter = int(match.group(1)), int(match.group(2))
                tid = get_or_create_trimestre(conn, year, quarter, sf)
                trimestre_id_map[sf] = tid

    # ── 5. Contratos ──────────────────────────────────────────────────────────
    inserted = 0
    skipped = 0
    for c in fact_contracts:
        sf = c.get("source_file", "")
        area_id_orig = c.get("area_id")
        tipo_id_orig = c.get("tipo_id")

        area_id = area_id_map.get(area_id_orig) if area_id_orig else None
        tipo_contrato_id = tipo_id_map.get(tipo_id_orig) if tipo_id_orig else None
        trimestre_id = trimestre_id_map.get(sf)

        cif = c.get("cif") or None

        # Si el CIF no existe en contratistas, lo insertamos como desconocido
        # para no violar la FK
        if cif:
            check = conn.execute(
                "SELECT 1 FROM contratistas WHERE cif = ?", (cif,)
            ).fetchone()
            if not check:
                upsert_contratista(conn, cif, "", "", "Desconocido", "", cif_valido=False)

        insert_contrato(
            conn,
            contrato_id=c["contrato_id"],
            cif=cif,
            area_id=area_id,
            tipo_contrato_id=tipo_contrato_id,
            trimestre_id=trimestre_id,
            importe=c.get("importe", 0.0),
            fecha_adjudicacion=c.get("fecha_adjudicacion"),
            objeto=c.get("objeto", ""),
            expediente=c.get("expediente", ""),
            year=c.get("year"),
            quarter=c.get("quarter"),
            source_file=sf,
        )
        inserted += 1

    conn.commit()
    print(f"{inserted} contratos insertados.")

    # ── 6. Marcar trimestres como procesados ──────────────────────────────────
    for sf, tid in trimestre_id_map.items():
        count = conn.execute(
            "SELECT COUNT(*) FROM contratos WHERE source_file = ?", (sf,)
        ).fetchone()[0]
        mark_trimestre_procesado(conn, tid, count)

    conn.close()

    # ── Resumen final ─────────────────────────────────────────────────────────
    conn2 = get_connection()
    stats = conn2.execute("""
        SELECT
            (SELECT COUNT(*) FROM contratos)    AS contratos,
            (SELECT COUNT(*) FROM contratistas) AS contratistas,
            (SELECT COUNT(*) FROM areas)        AS areas,
            (SELECT COUNT(*) FROM trimestres)   AS trimestres,
            (SELECT SUM(importe) FROM contratos) AS total_importe
    """).fetchone()
    conn2.close()

    print("\n── Migración completada ──────────────────────────────")
    print(f"  Contratos:    {stats['contratos']:,}")
    print(f"  Contratistas: {stats['contratistas']:,}")
    print(f"  Áreas:        {stats['areas']}")
    print(f"  Trimestres:   {stats['trimestres']}")
    print(f"  Inversión:    €{stats['total_importe']:,.2f}")
    print(f"  DB:           {DB_PATH}")


if __name__ == "__main__":
    migrate()
