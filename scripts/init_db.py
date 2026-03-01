"""
init_db.py — Inicializa la base de datos SQLite.

Crea data/contratos.db con el schema completo si no existe.
Seguro de ejecutar varias veces (CREATE TABLE IF NOT EXISTS).

Uso:
    python scripts/init_db.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from db import get_connection, init_schema, DB_PATH

if __name__ == "__main__":
    os.makedirs("data", exist_ok=True)
    print(f"Inicializando base de datos en: {DB_PATH}")
    conn = get_connection()
    init_schema(conn)
    conn.close()
    print("Listo.")
