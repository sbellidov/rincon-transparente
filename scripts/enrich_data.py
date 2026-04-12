#!/usr/bin/env python3
"""
enrich_data.py — Enriquece contratistas con datos registrales de apiempresas.es.

Por cada ejecución consulta hasta MAX_PER_RUN CIFs de sociedades (no autónomos)
priorizando las de mayor volumen de contratación. Los resultados se almacenan
en data/enriched/companies_cache.json (commiteado al repo) para persistir entre
ejecuciones del pipeline.

Requisito: variable de entorno APIEMPRESAS_KEY con la API key de apiempresas.es.
Si no está definida el script termina sin error (degradación elegante).
"""
import json
import os
import time
from datetime import date
from pathlib import Path

import requests
from typing import Optional

CACHE_PATH     = Path('data/enriched/companies_cache.json')
SUMMARY_PATH   = Path('data/processed/contractors_summary.json')
API_URL        = 'https://apiempresas.es/api/v1/companies'
MAX_PER_RUN    = 20
SLEEP_BETWEEN  = 1.0  # segundos entre llamadas

# Tipos de entidad que corresponden a personas físicas → sin datos registrales
AUTONOMO_TYPES = {'Autónomo'}


def load_json(path: Path):
    with open(path, encoding='utf-8') as f:
        return json.load(f)


def save_json(path: Path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


HEADERS = {
    'X-API-KEY':      '',   # se sobreescribe en la llamada
    'User-Agent':     'rincon-transparente-etl/1.0 (https://rincontransparente.com)',
    'Accept':         'application/json',
    'Accept-Language':'es-ES,es;q=0.9',
    'Connection':     'keep-alive',
}

MAX_RETRIES   = 3
RETRY_BACKOFF = 2.0  # segundos base para backoff exponencial


def fetch_company(cif: str, api_key: str) -> Optional[dict]:
    """Consulta la API con reintentos y devuelve los campos útiles, o None si no hay datos."""
    headers = {**HEADERS, 'X-API-KEY': api_key}
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(
                API_URL,
                params={'cif': cif},
                headers=headers,
                timeout=20,
            )
            if resp.status_code == 404:
                return None
            if resp.status_code == 401:
                print(f'  ⚠  API key inválida (401) — verifica APIEMPRESAS_KEY')
                return None
            resp.raise_for_status()
            data = resp.json().get('data') or {}
            if not data:
                return None
            return {
                'fetched_at':      str(date.today()),
                'status':          data.get('status'),
                'founded':         data.get('founded'),
                'cnae':            data.get('cnae'),
                'cnae_label':      data.get('cnae_label'),
                'cnae_2025_label': data.get('cnae_2025_label'),
                'province':        data.get('province'),
            }
        except requests.RequestException as exc:
            if attempt < MAX_RETRIES:
                wait = RETRY_BACKOFF * attempt
                print(f'  ⚠  Intento {attempt}/{MAX_RETRIES} fallido para {cif}: {exc} — reintentando en {wait}s')
                time.sleep(wait)
            else:
                print(f'  ⚠  Error definitivo consultando {cif}: {exc}')
    return None


def enrich():
    api_key = os.environ.get('APIEMPRESAS_KEY', '').strip()
    if not api_key:
        print('APIEMPRESAS_KEY no configurada — saltando enriquecimiento.')
        return

    if not SUMMARY_PATH.exists():
        print(f'No se encontró {SUMMARY_PATH} — ejecuta process_data.py primero.')
        return

    # Carga caché existente
    cache: dict = load_json(CACHE_PATH) if CACHE_PATH.exists() else {}

    # Candidatos: sociedades no en caché, ordenadas por volumen de contratación desc
    summary = load_json(SUMMARY_PATH)
    candidates = [
        c for c in summary
        if c.get('tipo_entidad') not in AUTONOMO_TYPES
        and c.get('cif')
        and c['cif'] not in cache
    ]
    candidates.sort(key=lambda c: c.get('total_importe', 0), reverse=True)

    to_query = candidates[:MAX_PER_RUN]
    total_cached   = sum(1 for v in cache.values() if v is not None)
    total_pending  = len(candidates) - len(to_query)

    print(f'Caché: {total_cached} empresas · Pendientes tras esta ejecución: {total_pending}')
    print(f'Consultando {len(to_query)} CIFs (top volumen no enriquecidos)...')

    nuevos = 0
    for contractor in to_query:
        cif    = contractor['cif']
        nombre = contractor.get('nombre', cif)
        result = fetch_company(cif, api_key)
        cache[cif] = result  # None = no encontrado, también se cachea
        estado = result.get('status', '—') if result else 'no encontrado'
        print(f'  {cif}  {nombre[:45]:<45}  {estado}')
        nuevos += 1
        if nuevos < len(to_query):
            time.sleep(SLEEP_BETWEEN)

    save_json(CACHE_PATH, cache)
    total_cached_now = sum(1 for v in cache.values() if v is not None)
    print(f'\nCaché actualizada: {total_cached_now} empresas con datos · {len(cache)} CIFs totales')


if __name__ == '__main__':
    enrich()
