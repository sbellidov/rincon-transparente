# Contratos Mayores — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Incorporar contratos mayores adjudicados (PLACE, desde 2022) al portal rincontransparente.com con ETL nuevo, análisis unificado y frontend actualizado.

**Architecture:** Dos scripts ETL nuevos (download_mayores.py, process_mayores.py) se añaden al pipeline semanal. El frontend recibe un nuevo JSON contracts_mayores.json y actualiza las KPI cards del dashboard, añade una sección dedicada y enriquece la vista de contratistas con badges. Los contratos menores existentes no se modifican.

**Tech Stack:** Python 3.12, requests, xml.etree.ElementTree; vanilla JS, Chart.js 4, Lucide; GitHub Actions

**Spec:** docs/superpowers/specs/2026-04-18-contratos-mayores-design.md

**Nota de seguridad frontend:** Todo el contenido externo insertado en el DOM usa la funcion auxiliar esc() (escape de &, <, >) definida inline. El patron sigue el existente en app.js. Los datos vienen de JSONs propios generados por el ETL.

---

## Estructura de ficheros

| Fichero | Accion | Responsabilidad |
|---|---|---|
| data/place_profiles.json | Crear (manual, Task 1) | IDs de perfiles PLACE de cada entidad contratante |
| scripts/discover_place_profiles.py | Crear (puntual) | Verifica feeds Atom de PLACE |
| scripts/download_mayores.py | Crear | Descarga feeds Atom paginados de PLACE |
| scripts/process_mayores.py | Crear | Normaliza Atom XML al schema de contratos mayores |
| tests/test_process_mayores.py | Crear | Tests unitarios parse_entry + analyze |
| scripts/analyze_data.py | Modificar | Aniade clave mayores en analysis.json; parametrizar processed_dir |
| scripts/publish_data.py | Modificar | Aniadir contracts_mayores.json a COPY_AS_IS |
| .github/workflows/update.yml | Modificar | Aniade los dos nuevos scripts al pipeline semanal |
| docs/index.html | Modificar | Nueva pestania C. Mayores + contenedor mayoresView |
| docs/style.css | Modificar | Badge .badge-mayor + .kpi-breakdown |
| docs/app.js | Modificar | Estado global, carga JSON, KPIs dashboard, seccion mayores, badge contratistas, grafico apilado |

---

## Task 1: Descubrir y documentar perfiles PLACE

**Files:**
- Create: data/place_profiles.json
- Create: scripts/discover_place_profiles.py

- [ ] **Step 1: Buscar perfiles en el directorio PLACE**

Ir a https://contrataciondelestado.es/wps/portal/plataforma/perfilContratante/directorio
Buscar "Rincon de la Victoria". Para cada entidad encontrada abrir su perfil
y copiar el idPerfil de la URL:
  https://contrataciondelestado.es/.../IC_SelfPerfilContratante?idPerfil=XXXXX

Entidades esperadas: Alcaldia, Pleno, APAL Deportes.

- [ ] **Step 2: Crear y ejecutar scripts/discover_place_profiles.py**

```python
# scripts/discover_place_profiles.py
"""Script puntual para verificar los feeds Atom de PLACE. No entra en el pipeline."""
import requests
import xml.etree.ElementTree as ET

# Rellenar con los IDs encontrados en Step 1
PROFILE_IDS = {
    "alcaldia_pleno": "XXXXX",
    "apal_deportes":  "YYYYY",
}

NS = {
    'atom': 'http://www.w3.org/2005/Atom',
    'cac':  'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
    'cbc':  'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
}

def t(entry, path):
    el = entry.find(path, NS)
    return el.text if el is not None else "NOT FOUND"

for name, pid in PROFILE_IDS.items():
    url = f"https://contrataciondelestado.es/wps/poc?uri=deeplink:perfilContratante_feeds&idPerfil={pid}"
    r = requests.get(url, timeout=30)
    print(f"\n=== {name} (HTTP {r.status_code}) ===")
    if r.status_code != 200 or '<entry' not in r.text:
        print("  ERROR: sin entries"); continue
    root = ET.fromstring(r.content)
    entries = root.findall('atom:entry', NS)
    print(f"  Entries en pagina 1: {len(entries)}")
    if entries:
        e = entries[0]
        base = 'cac:ContractFolderStatus/'
        for campo, path in [
            ('expediente',    base+'cbc:ContractFolderID'),
            ('objeto',        base+'cac:ProcurementProject/cbc:Name'),
            ('tipo code',     base+'cac:ProcurementProject/cbc:TypeCode'),
            ('proc code',     base+'cac:TenderingProcess/cbc:ProcedureCode'),
            ('presupuesto',   base+'cac:TenderingTerms/cac:BudgetAmount/cbc:TaxExclusiveAmount'),
            ('valor_estim',   base+'cac:TenderingTerms/cac:BudgetAmount/cbc:TotalAmount'),
            ('fecha_adj',     base+'cac:TenderResult/cbc:AwardDate'),
            ('num_lic',       base+'cac:TenderResult/cbc:ReceivedTenderQuantity'),
            ('importe_adj',   base+'cac:TenderResult/cac:AwardedTenderedProject/cbc:TaxExclusiveAmount'),
            ('adjudicatario', base+'cac:TenderResult/cac:WinningParty/cac:PartyName/cbc:Name'),
            ('cif',           base+'cac:TenderResult/cac:WinningParty/cac:PartyIdentification/cbc:ID'),
            ('entidad',       base+'cac:LocatedContractingParty/cac:Party/cac:PartyName/cbc:Name'),
            ('cpv',           base+'cac:ProcurementProject/cac:MainCommodityClassification/cbc:ItemClassificationCode'),
        ]:
            print(f"  {campo:15s}: {t(e, path)}")

if __name__ == '__main__':
    pass
```

```bash
source .venv/bin/activate
python scripts/discover_place_profiles.py
```

Si algun campo devuelve NOT FOUND, ajustar las rutas en parse_entry() (Task 3).

- [ ] **Step 3: Crear data/place_profiles.json con los IDs reales**

```json
{
  "profiles": [
    {
      "id": "XXXXX",
      "nombre": "Alcaldia del Ayuntamiento de Rincon de la Victoria",
      "nif": "P2908200E",
      "dir3": "L01290825"
    },
    {
      "id": "YYYYY",
      "nombre": "Ayuntamiento de Rincon de la Victoria - Apal Deportes",
      "nif": "P7908201B",
      "dir3": "LA0001744"
    }
  ]
}
```

- [ ] **Step 4: Commit**

```bash
git add data/place_profiles.json scripts/discover_place_profiles.py
git commit -m "chore(place): documentar perfiles PLACE y script de descubrimiento"
```

---

## Task 2: Tests para process_mayores.py

**Files:**
- Create: tests/test_process_mayores.py

- [ ] **Step 1: Crear fichero con fixture XML y tests**

Usar la entrada XML real del Step 2 de Task 1 si esta disponible.
Si no, usar este XML de referencia (ajustar rutas si Task 1 mostro paths distintos):

```python
# tests/test_process_mayores.py
"""Tests unitarios de process_mayores.py."""
import pytest
import json
import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

SAMPLE_ATOM_ENTRY = """\
<?xml version="1.0" encoding="UTF-8"?>
<entry xmlns="http://www.w3.org/2005/Atom"
       xmlns:cac="urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2"
       xmlns:cbc="urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2">
  <id>https://contrataciondelestado.es/wps/poc?uri=deeplink:licitacion&amp;idEvl=TEST001</id>
  <title type="text">Gestion campamentos de verano municipales 2024</title>
  <updated>2024-04-22T00:00:00Z</updated>
  <cac:ContractFolderStatus>
    <cbc:ContractFolderID>Contratacion 19/24</cbc:ContractFolderID>
    <cbc:ContractFolderStatusCode>ADJ</cbc:ContractFolderStatusCode>
    <cac:ProcurementProject>
      <cbc:Name>Gestion campamentos de verano municipales 2024</cbc:Name>
      <cbc:TypeCode>2</cbc:TypeCode>
      <cac:MainCommodityClassification>
        <cbc:ItemClassificationCode>92330000</cbc:ItemClassificationCode>
      </cac:MainCommodityClassification>
    </cac:ProcurementProject>
    <cac:TenderingProcess>
      <cbc:ProcedureCode>2</cbc:ProcedureCode>
    </cac:TenderingProcess>
    <cac:TenderingTerms>
      <cac:BudgetAmount>
        <cbc:TaxExclusiveAmount currencyID="EUR">71000.00</cbc:TaxExclusiveAmount>
        <cbc:TotalAmount currencyID="EUR">85910.00</cbc:TotalAmount>
      </cac:BudgetAmount>
    </cac:TenderingTerms>
    <cac:TenderResult>
      <cbc:AwardDate>2024-05-28</cbc:AwardDate>
      <cbc:ReceivedTenderQuantity>2</cbc:ReceivedTenderQuantity>
      <cac:AwardedTenderedProject>
        <cbc:TaxExclusiveAmount currencyID="EUR">63000.00</cbc:TaxExclusiveAmount>
      </cac:AwardedTenderedProject>
      <cac:WinningParty>
        <cac:PartyName>
          <cbc:Name>GRUPO AML &amp; APIMMA - EVENTS</cbc:Name>
        </cac:PartyName>
        <cac:PartyIdentification>
          <cbc:ID schemeName="NIF">B29123456</cbc:ID>
        </cac:PartyIdentification>
      </cac:WinningParty>
    </cac:TenderResult>
    <cac:LocatedContractingParty>
      <cac:Party>
        <cac:PartyName>
          <cbc:Name>Alcaldia del Ayuntamiento de Rincon de la Victoria</cbc:Name>
        </cac:PartyName>
      </cac:Party>
    </cac:LocatedContractingParty>
  </cac:ContractFolderStatus>
</entry>"""


class TestParseEntry:

    def test_campos_basicos(self):
        import xml.etree.ElementTree as ET
        from process_mayores import parse_entry
        entry = ET.fromstring(SAMPLE_ATOM_ENTRY)
        r = parse_entry(entry)
        assert r is not None
        assert r['objeto'] == 'Gestion campamentos de verano municipales 2024'
        assert r['expediente'] == 'Contratacion 19/24'
        assert r['tipo_contrato'] == 'Servicios'
        assert r['procedimiento'] == 'Restringido'
        assert r['entidad_contratante'] == 'Alcaldia del Ayuntamiento de Rincon de la Victoria'

    def test_importes(self):
        import xml.etree.ElementTree as ET
        from process_mayores import parse_entry
        entry = ET.fromstring(SAMPLE_ATOM_ENTRY)
        r = parse_entry(entry)
        assert r['presupuesto_base'] == 71000.0
        assert r['valor_estimado'] == 85910.0
        assert r['importe_adjudicacion'] == 63000.0
        assert r['ahorro_euros'] == pytest.approx(8000.0)
        assert r['pct_reduccion'] == pytest.approx(11.27, abs=0.1)

    def test_adjudicatario(self):
        import xml.etree.ElementTree as ET
        from process_mayores import parse_entry
        entry = ET.fromstring(SAMPLE_ATOM_ENTRY)
        r = parse_entry(entry)
        assert r['adjudicatario'] == 'GRUPO AML & APIMMA - EVENTS'
        assert r['cif'] == 'B29123456'
        assert r['tipo_entidad'] == 'SL'
        assert r['num_licitadores'] == 2

    def test_fechas(self):
        import xml.etree.ElementTree as ET
        from process_mayores import parse_entry
        entry = ET.fromstring(SAMPLE_ATOM_ENTRY)
        r = parse_entry(entry)
        assert r['fecha_formalizacion'] == '2024-05-28'
        assert r['year'] == 2024

    def test_cpv(self):
        import xml.etree.ElementTree as ET
        from process_mayores import parse_entry
        entry = ET.fromstring(SAMPLE_ATOM_ENTRY)
        r = parse_entry(entry)
        assert r['cpv_codigo'] == '92330000'

    def test_descarta_sin_adjudicatario(self):
        import xml.etree.ElementTree as ET
        from process_mayores import parse_entry
        xml_sin = SAMPLE_ATOM_ENTRY.replace('<cac:WinningParty>', '<cac:WinningParty_X>')
        xml_sin = xml_sin.replace('</cac:WinningParty>', '</cac:WinningParty_X>')
        entry = ET.fromstring(xml_sin)
        assert parse_entry(entry) is None

    def test_descarta_antes_de_2022(self):
        import xml.etree.ElementTree as ET
        from process_mayores import parse_entry
        xml_antiguo = SAMPLE_ATOM_ENTRY.replace('2024-05-28', '2021-12-31')
        entry = ET.fromstring(xml_antiguo)
        assert parse_entry(entry) is None


class TestInferTipoEntidad:

    def test_b_es_sl(self):
        from process_mayores import infer_tipo_entidad
        assert infer_tipo_entidad('B29123456') == 'SL'

    def test_a_es_sa(self):
        from process_mayores import infer_tipo_entidad
        assert infer_tipo_entidad('A29123456') == 'SA'

    def test_digito_es_autonomo(self):
        from process_mayores import infer_tipo_entidad
        assert infer_tipo_entidad('12345678Z') == 'Autonomo'

    def test_x_es_autonomo(self):
        from process_mayores import infer_tipo_entidad
        assert infer_tipo_entidad('X1234567Z') == 'Autonomo'

    def test_none_devuelve_none(self):
        from process_mayores import infer_tipo_entidad
        assert infer_tipo_entidad(None) is None

    def test_vacio_devuelve_none(self):
        from process_mayores import infer_tipo_entidad
        assert infer_tipo_entidad('') is None
```

NOTA: el fixture usa texto sin tildes para evitar problemas de encoding en el XML.
Actualizar el fixture con texto real de PLACE tras ejecutar Task 1.

- [ ] **Step 2: Ejecutar y verificar que fallan**

```bash
pytest tests/test_process_mayores.py -v 2>&1 | head -20
```

Esperado: ModuleNotFoundError: No module named 'process_mayores'

- [ ] **Step 3: Commit**

```bash
git add tests/test_process_mayores.py
git commit -m "test(mayores): tests unitarios parse_entry e infer_tipo_entidad"
```

---

## Task 3: Implementar process_mayores.py

**Files:**
- Create: scripts/process_mayores.py

- [ ] **Step 1: Crear el script**

```python
#!/usr/bin/env python3
"""
process_mayores.py -- Normaliza Atom XML de PLACE a contracts_mayores.json/csv.

Orden de ejecucion: despues de download_mayores.py
"""
import json
import csv
import xml.etree.ElementTree as ET
from pathlib import Path

RAW_DIR       = Path('data/raw/mayores')
PROCESSED_DIR = Path('data/processed')

NS = {
    'atom': 'http://www.w3.org/2005/Atom',
    'cac':  'urn:oasis:names:specification:ubl:schema:xsd:CommonAggregateComponents-2',
    'cbc':  'urn:oasis:names:specification:ubl:schema:xsd:CommonBasicComponents-2',
}

TIPO_CONTRATO = {
    '1': 'Obras', '2': 'Servicios', '3': 'Suministros',
    '4': 'Concesion de servicios', '5': 'Concesion de obras',
    '21': 'Mixto', '99': 'Otros',
}
PROCEDIMIENTO = {
    '1': 'Abierto', '2': 'Restringido',
    '3': 'Negociado con publicidad', '4': 'Negociado sin publicidad',
    '5': 'Dialogo competitivo', '6': 'Concurso de proyectos',
    '7': 'Asociacion para la innovacion', '8': 'Licitacion con negociacion',
}
MIN_YEAR = 2022


def infer_tipo_entidad(cif):
    if not cif:
        return None
    first = cif[0].upper()
    if first == 'A':
        return 'SA'
    if first == 'B':
        return 'SL'
    if first in set('0123456789XYZ'):
        return 'Autonomo'
    return None


def _text(el, path):
    found = el.find(path, NS)
    return found.text.strip() if found is not None and found.text else None


def _float(el, path):
    val = _text(el, path)
    try:
        return float(val) if val is not None else None
    except ValueError:
        return None


def parse_entry(entry):
    """Parsea un <entry> Atom de PLACE. Devuelve dict normalizado o None si no aplica."""
    b = 'cac:ContractFolderStatus/'

    fecha = _text(entry, b + 'cac:TenderResult/cbc:AwardDate')
    if not fecha:
        return None
    try:
        year = int(fecha[:4])
    except (ValueError, TypeError):
        return None
    if year < MIN_YEAR:
        return None

    adjudicatario = _text(entry, b + 'cac:TenderResult/cac:WinningParty/cac:PartyName/cbc:Name')
    if not adjudicatario:
        return None

    presupuesto = _float(entry, b + 'cac:TenderingTerms/cac:BudgetAmount/cbc:TaxExclusiveAmount')
    importe_adj = _float(entry, b + 'cac:TenderResult/cac:AwardedTenderedProject/cbc:TaxExclusiveAmount')

    ahorro = round(presupuesto - importe_adj, 2) if presupuesto and importe_adj else None
    pct    = round((ahorro / presupuesto) * 100, 2) if ahorro and presupuesto else None

    cif = _text(entry, b + 'cac:TenderResult/cac:WinningParty/cac:PartyIdentification/cbc:ID')

    num_lic_str = _text(entry, b + 'cac:TenderResult/cbc:ReceivedTenderQuantity')
    num_licitadores = int(num_lic_str) if num_lic_str and num_lic_str.isdigit() else None

    tipo_code = _text(entry, b + 'cac:ProcurementProject/cbc:TypeCode')
    proc_code = _text(entry, b + 'cac:TenderingProcess/cbc:ProcedureCode')

    return {
        'year':                year,
        'expediente':          _text(entry, b + 'cbc:ContractFolderID'),
        'objeto':              _text(entry, b + 'cac:ProcurementProject/cbc:Name'),
        'tipo_contrato':       TIPO_CONTRATO.get(tipo_code, tipo_code or 'Otros'),
        'procedimiento':       PROCEDIMIENTO.get(proc_code, proc_code or ''),
        'entidad_contratante': _text(entry, b + 'cac:LocatedContractingParty/cac:Party/cac:PartyName/cbc:Name'),
        'presupuesto_base':    presupuesto,
        'valor_estimado':      _float(entry, b + 'cac:TenderingTerms/cac:BudgetAmount/cbc:TotalAmount'),
        'importe_adjudicacion': importe_adj,
        'pct_reduccion':       pct,
        'ahorro_euros':        ahorro,
        'fecha_publicacion':   _text(entry, 'atom:updated'),
        'fecha_formalizacion': fecha,
        'num_licitadores':     num_licitadores,
        'adjudicatario':       adjudicatario,
        'cif':                 cif,
        'tipo_entidad':        infer_tipo_entidad(cif),
        'cpv_codigo':          _text(entry, b + 'cac:ProcurementProject/cac:MainCommodityClassification/cbc:ItemClassificationCode'),
        'cpv_descripcion':     None,
    }


def parse_feed(xml_content):
    root = ET.fromstring(xml_content)
    return [c for c in (parse_entry(e) for e in root.findall('atom:entry', NS)) if c]


def process_mayores(raw_dir=RAW_DIR, processed_dir=PROCESSED_DIR):
    raw_dir       = Path(raw_dir)
    processed_dir = Path(processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)
    all_contracts = []

    if not raw_dir.exists():
        print(f'  raw_dir no existe: {raw_dir}. Ejecutar download_mayores.py primero.')
        return

    for json_file in sorted(raw_dir.glob('*.json')):
        with open(json_file, encoding='utf-8') as f:
            pages = json.load(f)
        for xml_content in pages:
            batch = parse_feed(xml_content)
            all_contracts.extend(batch)
            print(f'  {json_file.name}: +{len(batch)} contratos')

    seen, unique = set(), []
    for c in all_contracts:
        key = (c.get('expediente') or '', c.get('entidad_contratante') or '')
        if key not in seen:
            seen.add(key)
            unique.append(c)

    unique.sort(key=lambda x: (x['year'], x.get('fecha_formalizacion') or ''))

    json_path = processed_dir / 'contracts_mayores.json'
    with open(json_path, 'w', encoding='utf-8') as f:
        json.dump(unique, f, indent=2, ensure_ascii=False)

    csv_path = processed_dir / 'contracts_mayores.csv'
    if unique:
        with open(csv_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=list(unique[0].keys()))
            writer.writeheader()
            writer.writerows(unique)

    print(f'\n  Total contratos mayores unicos: {len(unique)}')


if __name__ == '__main__':
    process_mayores()
```

- [ ] **Step 2: Ejecutar los tests**

```bash
pytest tests/test_process_mayores.py -v
```

Esperado: todos en verde. Si alguno falla por ruta XML, ajustar el path en parse_entry()
usando lo verificado en Task 1 Step 2.

- [ ] **Step 3: Commit**

```bash
git add scripts/process_mayores.py
git commit -m "feat(etl): process_mayores.py -- parsea Atom PLACE a contracts_mayores.json"
```

---

## Task 4: Implementar download_mayores.py

**Files:**
- Create: scripts/download_mayores.py

- [ ] **Step 1: Crear el script**

```python
#!/usr/bin/env python3
"""
download_mayores.py -- Descarga feeds Atom paginados de PLACE para los perfiles del ayuntamiento.

Lee data/place_profiles.json. Guarda las paginas XML como lista JSON en data/raw/mayores/.
Usa fichero .tmp para no sobreescribir datos validos si una descarga falla.
Orden de ejecucion: antes de process_mayores.py
"""
import json
import time
import requests
from pathlib import Path

PROFILES_PATH = Path('data/place_profiles.json')
RAW_DIR       = Path('data/raw/mayores')
FEED_TPL      = ('https://contrataciondelestado.es/wps/poc'
                 '?uri=deeplink:perfilContratante_feeds&idPerfil={id}&page={page}')
TIMEOUT = 30
DELAY   = 1.0


def load_profiles():
    with open(PROFILES_PATH, encoding='utf-8') as f:
        return json.load(f)['profiles']


def fetch_page(profile_id, page):
    url = FEED_TPL.format(id=profile_id, page=page)
    try:
        r = requests.get(url, timeout=TIMEOUT, headers={'Accept': 'application/atom+xml'})
        r.raise_for_status()
        return r.text if '<entry' in r.text else None
    except requests.RequestException as e:
        print(f'  Error pagina {page} de {profile_id}: {e}')
        return None


def download_profile(profile):
    pid, nombre = profile['id'], profile['nombre']
    pages, page = [], 1
    print(f'\n  Perfil: {nombre}')
    while True:
        xml = fetch_page(pid, page)
        if xml is None:
            break
        pages.append(xml)
        print(f'    Pagina {page}: {len(xml):,} bytes')
        page += 1
        time.sleep(DELAY)
    print(f'    Total paginas: {len(pages)}')
    return pages


def download_mayores():
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    for profile in load_profiles():
        pages = download_profile(profile)
        if not pages:
            print(f'  Sin datos para {profile["id"]}')
            continue
        safe = profile['nombre'].lower().replace(' ', '_')[:40]
        out  = RAW_DIR / f'{safe}.json'
        tmp  = out.with_suffix('.tmp')
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(pages, f, ensure_ascii=False)
        tmp.replace(out)
        print(f'  Guardado: {out}')


if __name__ == '__main__':
    download_mayores()
```

- [ ] **Step 2: Ejecutar manualmente**

```bash
python scripts/download_mayores.py
ls data/raw/mayores/
```

Esperado: al menos un .json en data/raw/mayores/.
Si llegan 0 paginas, revisar los IDs en data/place_profiles.json.

- [ ] **Step 3: Ejecutar process_mayores.py sobre datos reales**

```bash
python scripts/process_mayores.py
```

Verificar que data/processed/contracts_mayores.json contiene registros con campos del schema.

- [ ] **Step 4: Ejecutar suite completa**

```bash
pytest tests/ -v --tb=short
```

Esperado: todos en verde.

- [ ] **Step 5: Commit**

```bash
git add scripts/download_mayores.py
git commit -m "feat(etl): download_mayores.py -- descarga feeds Atom paginados de PLACE"
```

---

## Task 5: Actualizar analyze_data.py

**Files:**
- Modify: scripts/analyze_data.py
- Modify: tests/test_process_mayores.py

- [ ] **Step 1: Anadir test que falla al final de tests/test_process_mayores.py**

```python
class TestAnalyzeMayores:

    def test_analyze_incluye_clave_mayores(self, tmp_path):
        import json
        processed = tmp_path / 'data' / 'processed'
        processed.mkdir(parents=True)

        (processed / 'contracts.csv').write_text(
            'year,quarter,importe,area,tipo_contrato_limpio,tipo_entidad,'
            'fecha_adjudicacion,adjudicatario\n'
            '2024,1,5000,Alcaldia,Servicios,SL,2024-01-15,EMPRESA SL\n',
            encoding='utf-8'
        )
        (processed / 'contracts_mayores.json').write_text(json.dumps([
            {'year': 2024, 'importe_adjudicacion': 100000, 'tipo_contrato': 'Obras',
             'entidad_contratante': 'Alcaldia', 'adjudicatario': 'CONSTRUCTORA SA',
             'ahorro_euros': 10000, 'num_licitadores': 3}
        ]), encoding='utf-8')

        from analyze_data import analyze
        analyze(processed_dir=str(processed))

        result = json.loads((processed / 'analysis.json').read_text('utf-8'))
        assert 'mayores' in result
        assert result['mayores']['summary']['total_contracts'] == 1
        assert result['mayores']['summary']['total_amount'] == 100000
        assert '2024' in result['mayores']['by_year']
        assert 'Obras' in result['mayores']['by_tipo']
```

```bash
pytest tests/test_process_mayores.py::TestAnalyzeMayores -v
```

Esperado: FAIL (analyze() no acepta processed_dir ni tiene clave mayores)

- [ ] **Step 2: Modificar scripts/analyze_data.py**

Cambiar la firma de analyze():

```python
def analyze(processed_dir: str = 'data/processed'):
    df = pd.read_csv(os.path.join(processed_dir, 'contracts.csv'))
```

Cambiar la linea de output al final de analyze() (antes del print):

```python
    output_path = os.path.join(processed_dir, 'analysis.json')
```

Anadir el bloque de mayores justo antes de `analysis = clean_nan(analysis)`:

```python
    # -- Contratos Mayores ------------------------------------------------
    mayores_path = os.path.join(processed_dir, 'contracts_mayores.json')
    if os.path.exists(mayores_path):
        with open(mayores_path, encoding='utf-8') as _f:
            mayores = json.load(_f)
        if mayores:
            total_amount_m = sum(c.get('importe_adjudicacion') or 0 for c in mayores)
            total_ahorro   = sum(c.get('ahorro_euros') or 0 for c in mayores)
            lic_vals       = [c['num_licitadores'] for c in mayores if c.get('num_licitadores')]
            avg_lic        = round(sum(lic_vals) / len(lic_vals), 1) if lic_vals else 0

            by_year_m, by_tipo_m = {}, {}
            for c in mayores:
                y = str(c.get('year', ''))
                if y:
                    by_year_m.setdefault(y, {'sum': 0, 'count': 0})
                    by_year_m[y]['sum']   += c.get('importe_adjudicacion') or 0
                    by_year_m[y]['count'] += 1
                t = c.get('tipo_contrato', 'Otros')
                by_tipo_m.setdefault(t, {'sum': 0, 'count': 0})
                by_tipo_m[t]['sum']   += c.get('importe_adjudicacion') or 0
                by_tipo_m[t]['count'] += 1

            analysis['mayores'] = {
                'summary': {
                    'total_contracts': len(mayores),
                    'total_amount':    total_amount_m,
                    'total_ahorro':    total_ahorro,
                    'avg_licitadores': avg_lic,
                },
                'by_year': by_year_m,
                'by_tipo': by_tipo_m,
            }
```

- [ ] **Step 3: Ejecutar todos los tests**

```bash
pytest tests/ -v --tb=short
```

Esperado: todos en verde.

- [ ] **Step 4: Commit**

```bash
git add scripts/analyze_data.py tests/test_process_mayores.py
git commit -m "feat(etl): analyze_data incluye agregados de contratos mayores"
```

---

## Task 6: Actualizar publish_data.py y GitHub Actions

**Files:**
- Modify: scripts/publish_data.py
- Modify: .github/workflows/update.yml

- [ ] **Step 1: Anadir contracts_mayores.json a COPY_AS_IS en publish_data.py**

Localizar la lista COPY_AS_IS (linea ~24) y anadir la nueva entrada:

```python
COPY_AS_IS = [
    'analysis.json',
    'audit_summary.json',
    'dim_areas.json',
    'dim_types.json',
    'contracts_mayores.json',
]
```

- [ ] **Step 2: Verificar publicacion**

```bash
python scripts/publish_data.py
ls -la docs/data/contracts_mayores.json
```

Esperado: fichero creado en docs/data/.

- [ ] **Step 3: Actualizar .github/workflows/update.yml**

En el step "Ejecutar pipeline ETL", anadir los dos nuevos scripts:

```yaml
        run: |
          python scripts/discover_data.py
          python scripts/download_data.py
          python scripts/download_mayores.py
          python scripts/process_data.py
          python scripts/process_mayores.py
          python scripts/enrich_data.py
          python scripts/analyze_data.py
          python scripts/audit_data.py
```

En el step "Commit y push", anadir data/raw/mayores/ al git add:

```yaml
          git add docs/data/ data/catalog.json data/enriched/companies_cache.json data/raw/mayores/
```

- [ ] **Step 4: Commit**

```bash
git add scripts/publish_data.py .github/workflows/update.yml
git commit -m "feat(ci): publicar contracts_mayores y anadir al pipeline semanal"
```

---

## Task 7: Frontend -- HTML y CSS

**Files:**
- Modify: docs/index.html
- Modify: docs/style.css

- [ ] **Step 1: Anadir pestania en .tabs-bar (docs/index.html linea ~111)**

Insertar entre el boton "Contratos" y el boton "Contratistas":

```html
            <button class="tab-btn" data-view="mayores">
                <i data-lucide="file-check"></i><span class="tab-label">C. Mayores</span>
            </button>
```

- [ ] **Step 2: Anadir contenedor mayoresView tras el bloque tableView**

Buscar el cierre del div de la seccion tableView e insertar a continuacion:

```html
        <!-- TAB: CONTRATOS MAYORES -->
        <div id="mayoresView" class="view-content">
            <div class="kpi-row" id="mayores-kpi-row"></div>
            <div class="filters-bar">
                <div class="filters-row" id="mayores-filters-row"></div>
                <div class="search-row">
                    <input type="search" id="mayoresSearch"
                           placeholder="Buscar por objeto o adjudicatario..."
                           class="search-input">
                </div>
            </div>
            <div class="results-meta" id="mayores-results-meta"></div>
            <div class="table-wrapper">
                <table class="contracts-table">
                    <thead>
                        <tr>
                            <th>Anno</th><th>Objeto</th><th>Tipo</th>
                            <th>Procedimiento</th><th>Entidad</th>
                            <th class="text-right">Presupuesto</th>
                            <th class="text-right">Adjudicado</th>
                            <th class="text-right">% Reduc.</th>
                            <th>Adjudicatario</th><th>Fecha</th>
                        </tr>
                    </thead>
                    <tbody id="mayoresTableBody"></tbody>
                </table>
            </div>
            <div class="pagination" id="mayoresPagination"></div>
            <div style="margin-top:1rem">
                <button id="exportMayoresBtn" class="btn-export">
                    <i data-lucide="download"></i> Descargar CSV
                </button>
            </div>
        </div>
```

- [ ] **Step 3: Anadir estilos al final de docs/style.css**

```css
/* Badge Contrato Mayor */
.badge-mayor {
    display: inline-block;
    font-size: 0.62rem;
    font-weight: 600;
    letter-spacing: 0.03em;
    padding: 2px 6px;
    border-radius: 4px;
    background: #1d4ed8;
    color: #fff;
    vertical-align: middle;
    margin-left: 6px;
    white-space: nowrap;
}

/* KPI breakdown Mayores / Menores */
.kpi-breakdown {
    font-size: 0.70rem;
    color: var(--text-muted, #94a3b8);
    margin-top: 3px;
}
```

- [ ] **Step 4: Commit**

```bash
git add docs/index.html docs/style.css
git commit -m "feat(frontend): html pestania C. Mayores, contenedor y estilos badge/breakdown"
```

---

## Task 8: Frontend -- Estado global, carga JSON y KPIs dashboard

**Files:**
- Modify: docs/app.js

- [ ] **Step 1: Anadir estado global en seccion "1. Estado global"**

Tras `let contractorsSummary = [];` anadir:

```javascript
let allMayores = [];
let filteredMayores = [];
let mayoresPage = 1;
let mayoresSearch = '';
let mayoresFilters = { anio: '', tipo: '', entidad: '', procedimiento: '' };
```

- [ ] **Step 2: Ampliar Promise.all en init() para incluir contracts_mayores.json**

Sustituir el bloque Promise.all + asignaciones:

```javascript
        const [contractsRes, analysisRes, summaryRes, auditRes, mayoresRes] = await Promise.all([
            fetch('data/contracts.json?v=' + Date.now()),
            fetch('data/analysis.json?v=' + Date.now()),
            fetch('data/contractors_summary.json?v=' + Date.now()),
            fetch('data/audit_summary.json?v=' + Date.now()),
            fetch('data/contracts_mayores.json?v=' + Date.now()),
        ]);

        allContracts       = await contractsRes.json();
        analysisData       = await analysisRes.json();
        contractorsSummary = await summaryRes.json();
        auditData          = await auditRes.json();
        allMayores         = await mayoresRes.json();
        filteredMayores    = [...allMayores];
```

Anadir tras renderQuality():

```javascript
        renderMayoresKpis();
        renderMayoresTable();
        populateMayoresFilters();
        setupMayoresSearch();
```

Anadir al final de init() (antes del catch), el indice CIF -> mayores:

```javascript
        window._mayoresByCif = {};
        for (const c of allMayores) {
            if (!c.cif) continue;
            (window._mayoresByCif[c.cif] = window._mayoresByCif[c.cif] || []).push(c);
        }
```

- [ ] **Step 3: Reemplazar renderDashboard() completa**

```javascript
function renderDashboard() {
    const summary = analysisData.summary || {};
    const maySum  = analysisData.mayores?.summary || {};

    const totalMenores = summary.total_contracts || 0;
    const totalMayores = maySum.total_contracts || 0;
    const invMenores   = summary.total_amount || 0;
    const invMayores   = maySum.total_amount || 0;

    const cards = [
        {
            label: 'Total Contratos',
            value: (totalMenores + totalMayores).toLocaleString('es-ES'),
            icon: 'file-text',
            breakdown: `Mayores: ${totalMayores.toLocaleString('es-ES')} | Menores: ${totalMenores.toLocaleString('es-ES')}`,
        },
        {
            label: 'Inversion Total',
            value: formatCurrency(invMenores + invMayores),
            icon: 'euro',
            breakdown: `Mayores: ${formatCurrency(invMayores)} | Menores: ${formatCurrency(invMenores)}`,
        },
        {
            label: 'Media por Contrato',
            value: formatCurrency(summary.avg_amount || 0),
            icon: 'trending-up',
        },
        {
            label: 'Contratistas unicos',
            value: contractorsSummary.length.toLocaleString('es-ES'),
            icon: 'building-2',
        },
    ];

    const container = document.getElementById('summary-cards');
    container.innerHTML = cards.map(card => `
        <div class="card stat-card">
            <div style="display: flex; justify-content: space-between; align-items: start">
                <h4>${card.label}</h4>
                <div class="icon-circle">
                    <i data-lucide="${card.icon}" style="width: 16px; height: 16px"></i>
                </div>
            </div>
            <div class="value">${card.value}</div>
            ${card.breakdown ? `<div class="kpi-breakdown">${card.breakdown}</div>` : ''}
        </div>
    `).join('');
    lucide.createIcons();
}
```

- [ ] **Step 4: Verificar en servidor local**

```bash
python scripts/serve_web.py
```

Abrir http://localhost:8000. Las dos primeras KPI cards muestran breakdown Mayores/Menores.

- [ ] **Step 5: Commit**

```bash
git add docs/app.js
git commit -m "feat(frontend): cargar contracts_mayores y breakdown en KPI cards dashboard"
```

---

## Task 9: Frontend -- Seccion Contratos Mayores

**Files:**
- Modify: docs/app.js

Anadir las siguientes funciones en la seccion de renderizado (tras renderDashboard):

- [ ] **Step 1: Anadir renderMayoresKpis()**

```javascript
function renderMayoresKpis() {
    const container = document.getElementById('mayores-kpi-row');
    if (!container) return;
    const d = analysisData.mayores?.summary || {};
    const cards = [
        { label: 'Contratos mayores',  value: (d.total_contracts || 0).toLocaleString('es-ES'), icon: 'file-check' },
        { label: 'Importe adjudicado', value: formatCurrency(d.total_amount || 0),              icon: 'euro' },
        { label: 'Ahorro total',       value: formatCurrency(d.total_ahorro || 0),              icon: 'trending-down' },
        { label: 'Media licitadores',  value: (d.avg_licitadores || 0).toFixed(1),              icon: 'users' },
    ];
    container.innerHTML = cards.map(c => `
        <div class="card stat-card">
            <div style="display:flex;justify-content:space-between;align-items:start">
                <h4>${c.label}</h4>
                <div class="icon-circle"><i data-lucide="${c.icon}" style="width:16px;height:16px"></i></div>
            </div>
            <div class="value">${c.value}</div>
        </div>`).join('');
    lucide.createIcons();
}
```

- [ ] **Step 2: Anadir populateMayoresFilters() y applyMayoresFilters()**

```javascript
function populateMayoresFilters() {
    const container = document.getElementById('mayores-filters-row');
    if (!container) return;
    const years     = [...new Set(allMayores.map(c => c.year))].sort();
    const tipos     = [...new Set(allMayores.map(c => c.tipo_contrato).filter(Boolean))].sort();
    const entidades = [...new Set(allMayores.map(c => c.entidad_contratante).filter(Boolean))].sort();
    const procs     = [...new Set(allMayores.map(c => c.procedimiento).filter(Boolean))].sort();

    const sel = (id, label, opts) =>
        '<select id="' + id + '" class="filter-select">' +
        '<option value="">' + label + '</option>' +
        opts.map(o => '<option value="' + o + '">' + o + '</option>').join('') +
        '</select>';

    container.innerHTML =
        sel('mfAnio',    'Anno',         years) +
        sel('mfTipo',    'Tipo',         tipos) +
        sel('mfEntidad', 'Entidad',      entidades) +
        sel('mfProc',    'Procedimiento',procs);

    const map = { mfAnio: 'anio', mfTipo: 'tipo', mfEntidad: 'entidad', mfProc: 'procedimiento' };
    Object.keys(map).forEach(id => {
        document.getElementById(id)?.addEventListener('change', e => {
            mayoresFilters[map[id]] = e.target.value;
            mayoresPage = 1;
            applyMayoresFilters();
        });
    });
}

function applyMayoresFilters() {
    const q = mayoresSearch.toLowerCase();
    filteredMayores = allMayores.filter(c => {
        if (mayoresFilters.anio          && String(c.year) !== mayoresFilters.anio)           return false;
        if (mayoresFilters.tipo          && c.tipo_contrato !== mayoresFilters.tipo)          return false;
        if (mayoresFilters.entidad       && c.entidad_contratante !== mayoresFilters.entidad) return false;
        if (mayoresFilters.procedimiento && c.procedimiento !== mayoresFilters.procedimiento) return false;
        if (q && !(c.objeto || '').toLowerCase().includes(q) &&
                 !(c.adjudicatario || '').toLowerCase().includes(q)) return false;
        return true;
    });
    renderMayoresTable();
}

function setupMayoresSearch() {
    document.getElementById('mayoresSearch')?.addEventListener('input', e => {
        mayoresSearch = e.target.value.trim();
        mayoresPage = 1;
        applyMayoresFilters();
    });
}
```

- [ ] **Step 3: Anadir renderMayoresTable() y exportMayoresCsv()**

```javascript
function renderMayoresTable() {
    const tbody = document.getElementById('mayoresTableBody');
    const meta  = document.getElementById('mayores-results-meta');
    if (!tbody) return;

    const total = filteredMayores.length;
    const start = (mayoresPage - 1) * ITEMS_PER_PAGE;
    const page  = filteredMayores.slice(start, start + ITEMS_PER_PAGE);

    if (meta) meta.textContent = total.toLocaleString('es-ES') + ' contratos';

    // esc(): escapa caracteres HTML para insercion segura
    const esc = s => (s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

    tbody.innerHTML = page.map(c => {
        const pct = c.pct_reduccion != null ? c.pct_reduccion.toFixed(1) + '%' : '-';
        return '<tr>' +
            '<td>' + c.year + '</td>' +
            '<td title="' + esc(c.objeto) + '">' + esc(c.objeto) + '</td>' +
            '<td>' + esc(c.tipo_contrato) + '</td>' +
            '<td>' + esc(c.procedimiento) + '</td>' +
            '<td>' + esc(c.entidad_contratante) + '</td>' +
            '<td class="text-right">' + (c.presupuesto_base != null ? formatCurrency(c.presupuesto_base) : '-') + '</td>' +
            '<td class="text-right">' + (c.importe_adjudicacion != null ? formatCurrency(c.importe_adjudicacion) : '-') + '</td>' +
            '<td class="text-right">' + pct + '</td>' +
            '<td>' + esc(c.adjudicatario) + '</td>' +
            '<td style="white-space:nowrap">' + (c.fecha_formalizacion || '-') + '</td>' +
            '</tr>';
    }).join('');

    const pgContainer = document.getElementById('mayoresPagination');
    if (pgContainer) {
        const pages = Math.ceil(total / ITEMS_PER_PAGE);
        pgContainer.innerHTML = pages <= 1 ? '' : Array.from({ length: pages }, (_, i) =>
            '<button class="page-btn' + (i + 1 === mayoresPage ? ' active' : '') +
            '" data-page="' + (i + 1) + '">' + (i + 1) + '</button>'
        ).join('');
        pgContainer.querySelectorAll('.page-btn').forEach(btn =>
            btn.addEventListener('click', e => {
                mayoresPage = parseInt(e.target.dataset.page);
                renderMayoresTable();
            })
        );
    }

    const exportBtn = document.getElementById('exportMayoresBtn');
    if (exportBtn) {
        const clone = exportBtn.cloneNode(true);
        exportBtn.replaceWith(clone);
        clone.addEventListener('click', exportMayoresCsv);
    }
    lucide.createIcons();
}

function exportMayoresCsv() {
    const cols = ['year','objeto','tipo_contrato','procedimiento','entidad_contratante',
                  'presupuesto_base','importe_adjudicacion','pct_reduccion','ahorro_euros',
                  'adjudicatario','cif','fecha_formalizacion','num_licitadores','cpv_codigo'];
    const escCsv = v => {
        const s = v != null ? String(v) : '';
        return s.includes(',') || s.includes('"') || s.includes('\n') ? '"' + s.replace(/"/g,'""') + '"' : s;
    };
    const rows = filteredMayores.map(c => cols.map(k => escCsv(c[k])).join(','));
    const blob = new Blob(['\uFEFF' + cols.join(',') + '\n' + rows.join('\n')],
                          { type: 'text/csv;charset=utf-8;' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = 'contratos-mayores-rincon.csv';
    a.click();
    URL.revokeObjectURL(a.href);
}
```

- [ ] **Step 4: Probar en servidor local**

```bash
python scripts/serve_web.py
```

Verificar: pestania C. Mayores muestra KPIs, tabla con datos, filtros y busqueda funcionan, CSV se descarga correctamente.

- [ ] **Step 5: Commit**

```bash
git add docs/app.js
git commit -m "feat(frontend): seccion Contratos Mayores -- KPIs, tabla, filtros, paginacion, CSV"
```

---

## Task 10: Frontend -- Badge en Contratistas y grafico apilado

**Files:**
- Modify: docs/app.js

- [ ] **Step 1: Anadir funcion miniTableRowsMayores() junto a miniTableRows()**

```javascript
function miniTableRowsMayores(cif) {
    const mayores = (window._mayoresByCif || {})[cif] || [];
    if (!mayores.length) return '';
    const esc = s => (s || '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
    return mayores.map(m =>
        '<tr>' +
        '<td class="text-muted" style="white-space:nowrap">' + (m.fecha_formalizacion || '-') + '</td>' +
        '<td>' + esc(m.objeto) + ' <span class="badge-mayor">Contrato Mayor</span></td>' +
        '<td class="text-muted">-</td>' +
        '<td>' + (m.tipo_contrato ? '<small class="tipo-tag">' + esc(m.tipo_contrato) + '</small>' : '-') + '</td>' +
        '<td class="text-muted">' + (m.expediente || '-') + '</td>' +
        '<td class="text-right"><strong>' + formatCurrency(m.importe_adjudicacion || 0) + '</strong></td>' +
        '</tr>'
    ).join('');
}
```

- [ ] **Step 2: Modificar renderContractors() para incluir filas de mayores**

Localizar en renderContractors() el bloque tbody (linea ~267):

```javascript
                        <tbody class="mini-contracts-tbody">${miniTableRows(c.contratos, c.cif)}</tbody>
```

Reemplazar con:

```javascript
                        <tbody class="mini-contracts-tbody">${miniTableRows(c.contratos, c.cif)}${miniTableRowsMayores(c.cif)}</tbody>
```

- [ ] **Step 3: Actualizar el grafico anual en renderCharts() -- barras apiladas**

Localizar el bloque que crea yearChart (linea ~363). Reemplazar las variables de datos
y la configuracion del Chart:

```javascript
    const yearLabels = Object.keys(analysisData.by_year || {}).sort();
    const valMenores = yearLabels.map(y => (analysisData.by_year[y] || {}).sum || 0);
    const valMayores = yearLabels.map(y => (analysisData.mayores?.by_year?.[y] || {}).sum || 0);

    new Chart(yearCtx, {
        type: 'bar',
        plugins: [ChartDataLabels],
        data: {
            labels: yearLabels,
            datasets: [
                {
                    label: 'Contratos Menores',
                    data: valMenores,
                    backgroundColor: 'rgba(37, 99, 235, 0.7)',
                    borderColor: '#2563eb',
                    borderWidth: 1,
                    borderRadius: 2,
                },
                {
                    label: 'Contratos Mayores',
                    data: valMayores,
                    backgroundColor: 'rgba(16, 185, 129, 0.7)',
                    borderColor: '#10b981',
                    borderWidth: 1,
                    borderRadius: 2,
                },
            ],
        },
        options: {
            responsive: true,
            maintainAspectRatio: false,
            layout: { padding: { top: 24 } },
            plugins: {
                legend: {
                    display: true,
                    labels: { color: legendColor(), font: chartDefaults.font },
                },
                tooltip: { callbacks: { label: ctx => ' ' + ctx.dataset.label + ': ' + fmtM(ctx.parsed.y) } },
                datalabels: { display: false },
            },
            scales: {
                x: {
                    stacked: true,
                    grid: { display: false },
                    ticks: { color: chartDefaults.ticks, font: chartDefaults.font },
                },
                y: {
                    stacked: true,
                    grid: { color: chartDefaults.grid },
                    ticks: { color: chartDefaults.ticks, font: chartDefaults.font, callback: v => fmtM(v) },
                },
            },
        },
    });
```

- [ ] **Step 4: Probar en servidor local**

```bash
python scripts/serve_web.py
```

Verificar:
- Contratistas con contratos mayores muestran filas con badge azul "Contrato Mayor"
- Vista Analisis -> grafico anual muestra barras apiladas en dos colores

- [ ] **Step 5: Ejecutar suite completa de tests**

```bash
pytest tests/ -v --tb=short
```

Esperado: todos los tests en verde.

- [ ] **Step 6: Commit y PR**

```bash
git add docs/app.js
git commit -m "feat(frontend): badge Contrato Mayor en contratistas y grafico anual apilado"
```

```bash
gh pr create --title "feat: contratos mayores PLACE -- ETL + frontend" --body "$(cat <<'PREOF'
## Resumen

- Nuevo ETL (download_mayores.py, process_mayores.py) que descarga contratos mayores adjudicados desde PLACE via Atom feeds (desde 2022)
- 3 entidades contratantes: Alcaldia, Pleno, APAL Deportes
- Schema de 18 campos (directos de PLACE + inferidos/calculados)
- Dashboard: KPI cards con breakdown Mayores | Menores en Total Contratos e Inversion
- Nueva seccion C. Mayores: tabla paginada con filtros, busqueda libre y descarga CSV
- Vista Contratistas: badge Contrato Mayor en contratos mayores
- Grafico anual: barras apiladas Menores vs Mayores

## Plan de test

- [ ] pytest tests/ -v -- todos en verde
- [ ] python scripts/download_mayores.py && python scripts/process_mayores.py -- genera datos reales
- [ ] Servidor local: seccion Mayores funcional, badge contratistas, grafico apilado, sin regresiones en menores

:robot: Generated with Claude Code
PREOF
)"
```
