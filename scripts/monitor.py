"""
monitor.py — Detecta, descarga y procesa nuevos trimestres del portal municipal.

Flujo:
  1. Consulta la tabla `trimestres` para saber qué hay ya procesado.
  2. Raspa la página de transparencia del ayuntamiento buscando ficheros XLS/XLSX.
  3. Compara con lo que ya está en DB para identificar trimestres nuevos.
  4. Descarga los ficheros nuevos a data/raw/.
  5. Re-lanza el ETL completo (process_data.py) para integrarlos.

Uso:
    python scripts/monitor.py               # Modo normal
    python scripts/monitor.py --dry-run     # Solo muestra qué descargaría, sin actuar
    python scripts/monitor.py --force       # Re-descarga aunque ya esté en DB
"""

import argparse
import logging
import os
import re
import subprocess
import sys
from datetime import date, timedelta

import requests
from bs4 import BeautifulSoup

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from db import get_connection, get_trimestres_procesados

# ── Configuración ─────────────────────────────────────────────────────────────

PORTAL_URL = "https://www.rincondelavictoria.es/areas/contratacion/relaciones-de-contratos-menores"
BASE_URL   = "https://www.rincondelavictoria.es"
RAW_DIR    = "data/raw"

# Días de margen tras el fin de un trimestre para considerarlo publicable
# (el ayuntamiento suele tardar entre 1 y 2 meses en publicarlo)
PUBLICATION_DELAY_DAYS = 45

# ── Logging ───────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger(__name__)


# ── Lógica de trimestres esperados ────────────────────────────────────────────

def _quarter_end(year: int, quarter: int) -> date:
    """Fecha en que finaliza un trimestre."""
    ends = {1: date(year, 3, 31), 2: date(year, 6, 30),
            3: date(year, 9, 30), 4: date(year, 12, 31)}
    return ends[quarter]


def expected_quarters(today: date = None, since_year: int = 2024) -> list:
    """
    Devuelve la lista de (year, quarter) que deberían estar publicados a fecha de hoy.
    Se considera publicable un trimestre cuando han pasado PUBLICATION_DELAY_DAYS
    desde su fin.
    """
    today = today or date.today()
    result = []
    for year in range(since_year, today.year + 1):
        for quarter in range(1, 5):
            end = _quarter_end(year, quarter)
            if end + timedelta(days=PUBLICATION_DELAY_DAYS) <= today:
                result.append((year, quarter))
    return result


# ── Scraping del portal ───────────────────────────────────────────────────────

def _parse_quarter_from_href(href: str):
    """
    Extrae (year, quarter) del nombre del fichero en la URL.
    Ejemplo: '/sites/default/files/2026-01/CONTRATOS MENORES 4 TRIMESTRE 2025 - copia.xls'
             → (2025, 4)
    """
    # Decodificar %20 y similares para simplificar el regex
    name = requests.utils.unquote(href).upper()
    m = re.search(r"(\d)\s+TRIMESTRE\s+(\d{4})", name)
    if m:
        return int(m.group(2)), int(m.group(1))  # (year, quarter)
    return None


def scrape_portal() -> list[dict]:
    """
    Raspa la página de transparencia y devuelve una lista de dicts:
        [{"url": "https://...", "year": 2025, "quarter": 4, "filename": "2025_Q4.xls"}, ...]
    """
    log.info(f"Accediendo al portal: {PORTAL_URL}")
    try:
        resp = requests.get(PORTAL_URL, timeout=20, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
    except requests.RequestException as e:
        log.error(f"No se pudo acceder al portal: {e}")
        return []

    soup = BeautifulSoup(resp.text, "html.parser")

    found = []
    # Buscar todos los <a> cuyo href apunte a un XLS/XLSX dentro de sites/default/files
    for tag in soup.find_all("a", href=re.compile(r"/sites/default/files/.*\.(xls|xlsx)$", re.I)):
        href = tag["href"]
        full_url = BASE_URL + href if href.startswith("/") else href

        parsed = _parse_quarter_from_href(href)
        if parsed is None:
            log.debug(f"No se pudo parsear trimestre de: {href}")
            continue

        year, quarter = parsed
        filename = f"{year}_Q{quarter}.xls"
        found.append({"url": full_url, "year": year, "quarter": quarter, "filename": filename})
        log.debug(f"  Encontrado: {year} Q{quarter} — {full_url}")

    # Deduplicar (puede haber varios links al mismo trimestre)
    seen = set()
    unique = []
    for item in found:
        key = (item["year"], item["quarter"])
        if key not in seen:
            seen.add(key)
            unique.append(item)

    log.info(f"Ficheros encontrados en el portal: {len(unique)}")
    return sorted(unique, key=lambda x: (x["year"], x["quarter"]))


# ── Descarga ──────────────────────────────────────────────────────────────────

def download_file(url: str, dest_path: str) -> bool:
    """Descarga un fichero XLS a dest_path. Devuelve True si tiene éxito."""
    log.info(f"Descargando: {url}")
    try:
        resp = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"})
        resp.raise_for_status()
        with open(dest_path, "wb") as f:
            f.write(resp.content)
        size_kb = len(resp.content) / 1024
        log.info(f"  → {dest_path}  ({size_kb:.0f} KB)")
        return True
    except requests.RequestException as e:
        log.error(f"  Error al descargar {url}: {e}")
        return False


# ── ETL ───────────────────────────────────────────────────────────────────────

def run_etl() -> bool:
    """Ejecuta el ETL completo (process_data.py)."""
    log.info("Lanzando ETL (process_data.py)...")
    result = subprocess.run(
        [sys.executable, "scripts/process_data.py"],
        capture_output=False,
    )
    if result.returncode == 0:
        log.info("ETL completado correctamente.")
        return True
    else:
        log.error(f"ETL falló con código {result.returncode}.")
        return False


# ── Monitor principal ─────────────────────────────────────────────────────────

def monitor(dry_run: bool = False, force: bool = False) -> None:
    sep = "─" * 55

    print(sep)
    print(f"  Monitor de contratos menores — {date.today()}")
    print(sep)

    # 1. Trimestres ya procesados
    conn = get_connection()
    ya_procesados = get_trimestres_procesados(conn)
    conn.close()
    log.info(f"Trimestres en DB: {sorted(ya_procesados)}")

    # 2. Trimestres que deberían existir
    esperados = expected_quarters()
    log.info(f"Trimestres esperados: {sorted(esperados)}")

    # 3. Pendientes (esperados pero no en DB)
    pendientes_db = sorted(set(esperados) - ya_procesados)

    # 4. Scraping del portal
    portal_items = scrape_portal()
    portal_map = {(item["year"], item["quarter"]): item for item in portal_items}

    # 5. Cruzar: pendientes en DB que además están en el portal
    para_descargar = []
    for yq in (pendientes_db if not force else esperados):
        if yq in portal_map:
            para_descargar.append(portal_map[yq])
        else:
            log.warning(f"  {yq[0]} Q{yq[1]} — pendiente en DB pero NO encontrado en el portal")

    print()
    if not para_descargar:
        print("✅  Todo está actualizado. No hay trimestres nuevos.")
    else:
        print(f"🆕  Trimestres nuevos detectados: {len(para_descargar)}")
        for item in para_descargar:
            print(f"    · {item['year']} Q{item['quarter']}  →  {item['url']}")

    if dry_run:
        print()
        print("ℹ️  Modo --dry-run: no se descarga nada.")
        return

    if not para_descargar:
        return

    # 6. Descarga
    print()
    os.makedirs(RAW_DIR, exist_ok=True)
    descargados = []
    for item in para_descargar:
        dest = os.path.join(RAW_DIR, item["filename"])
        if download_file(item["url"], dest):
            descargados.append(item)

    if not descargados:
        log.error("No se pudo descargar ningún fichero.")
        return

    # 7. ETL
    print()
    ok = run_etl()
    print()
    print(sep)
    if ok:
        print(f"✅  Proceso completado. {len(descargados)} trimestre(s) integrado(s).")
    else:
        print("❌  El ETL falló. Revisa los logs.")
    print(sep)


# ── Entry point ───────────────────────────────────────────────────────────────

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Monitor de contratos menores")
    parser.add_argument("--dry-run", action="store_true",
                        help="Solo muestra qué descargaría, sin actuar")
    parser.add_argument("--force",   action="store_true",
                        help="Re-descarga todos los trimestres aunque ya estén en DB")
    args = parser.parse_args()

    monitor(dry_run=args.dry_run, force=args.force)
