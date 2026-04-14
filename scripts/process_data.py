# ============================================================
# process_data.py — Pipeline principal del ETL
#
# Lee los Excel trimestrales de data/raw/ y genera:
#   - contracts.csv / contracts.json  → un registro por contrato
#   - contractors_summary.json        → contratistas con contratos anidados
#   - dim_contractors/areas/types.json → dimensiones del star schema
#   - fact_contracts.json             → tabla de hechos
#
# Orden de ejecución: después de download_data.py
# ============================================================

import pandas as pd
import os
import re
import json
import hashlib
from datetime import datetime


# ── Limpieza de importes ─────────────────────────────────────

def clean_amount(val):
    """Convierte un importe en texto a float, tolerante a errores de input humano.

    Casos cubiertos:
      - Formato europeo estándar: '1.234,56 €' → 1234.56
      - Símbolo € pegado: '€1234', '1234€'
      - D1  Coma como separador de miles: '6,705' → 6705 ; '1,234,567' → 1234567
            (solo cuando TODOS los grupos tras las comas tienen exactamente 3 dígitos)
      - D3  Notación científica en string: '1.5E+4' → 15000.0
      - Multi-punto (fix previo):
            · Último grupo 1-2 dígitos → decimal mal escrito: '6.705.88' → 6705.88
            · Último grupo 3 dígitos   → separadores de miles: '1.234.567' → 1234567
            · Último grupo ≥ 4 dígitos → ambiguo → None (con aviso)
    """
    if pd.isna(val): return 0.0
    if isinstance(val, (int, float)): return float(val)
    raw = str(val).replace('€', '').strip()
    if not raw: return 0.0

    # D3: notación científica en string (antes de cualquier replace de puntos)
    if re.match(r'^[\d.,]+[eE][+-]?\d+$', raw):
        try:
            return float(raw.replace(',', '.'))
        except:
            return 0.0

    # D1: coma como separador de miles sin punto decimal
    # Si hay comas y no hay punto, y TODOS los grupos tras cada coma tienen 3 dígitos
    if ',' in raw and '.' not in raw:
        parts = raw.split(',')
        if len(parts) >= 2 and all(re.match(r'^\d{3}$', p.strip()) for p in parts[1:]):
            raw = raw.replace(',', '')  # eliminar separadores de miles → entero

    # Multi-punto sin coma: posible decimal mal escrito
    if raw.count('.') >= 2 and ',' not in raw:
        last_segment = raw.rsplit('.', 1)[1]
        m = re.match(r'^\d+', last_segment)
        if m:
            n = len(m.group())
            if n <= 2:
                integer_str = raw.rsplit('.', 1)[0].replace('.', '')
                try:
                    return float(f"{integer_str}.{last_segment}")
                except:
                    return 0.0
            elif n >= 4:
                print(f"  ⚠ Importe ambiguo (múltiples puntos, {n} dígitos finales): "
                      f"{repr(val)} → nullificado")
                return None

    # Formato europeo: punto como separador de miles, coma como decimal
    cleaned = raw.replace('.', '').replace(',', '.')
    try:
        return float(cleaned)
    except:
        return 0.0


# ── Validación de identificadores fiscales españoles ────────────

def validate_spanish_id(code):
    """Valida un NIF (personas físicas / NIE extranjeros) o CIF (entidades).

    NIF: 8 dígitos + letra de control calculada con módulo 23.
    NIE: X/Y/Z + 7 dígitos + letra; se sustituye X→0, Y→1, Z→2 antes del cálculo.
    CIF: letra inicial + 7 dígitos + dígito/letra de control según la letra.

    Devuelve (es_válido, código_normalizado, tipo).
    """
    if not code or not isinstance(code, str):
        return False, None, "Invalid"
    raw = code.upper().strip()
    clean = re.sub(r'[^A-Z0-9]', '', raw)
    if len(clean) != 9:
        return False, clean, "Unknown"
    first = clean[0]
    middle = clean[1:8]
    last = clean[8]
    # NIF / NIE
    if first.isdigit() or first in 'XYZ':
        mapping = "TRWAGMYFPDXBNJZSQVHLCKE"
        try:
            if first.isdigit():
                num_str = clean[:8]
            else:
                prefix = {'X': '0', 'Y': '1', 'Z': '2'}[first]
                num_str = prefix + middle
            idx = int(num_str) % 23
            return last == mapping[idx], clean, "NIF"
        except:
            return False, clean, "NIF (Error)"
    # CIF
    if first in 'ABCDEFGHJNPQRSUVW':
        try:
            if not middle.isdigit(): return False, clean, "CIF"
            # Suma de posiciones pares (1-based)
            a = sum(int(middle[i]) for i in [1, 3, 5])
            # Suma de posiciones impares: cada dígito × 2, sumando sus cifras
            b = 0
            for i in [0, 2, 4, 6]:
                prod = int(middle[i]) * 2
                b += (prod // 10) + (prod % 10)
            c = a + b
            e = (10 - (c % 10)) % 10
            letters_map = 'JABCDEFGHI'
            if first in 'PQRSW': return last == letters_map[e], clean, "CIF"
            elif first in 'ABEH': return last == str(e), clean, "CIF"
            else: return (last == str(e) or last == letters_map[e]), clean, "CIF"
        except: return False, clean, "CIF (Error)"
    return False, clean, "Unknown"


def extract_cif_address(combined):
    """Extrae el CIF y la dirección de una celda que los combina (formato 2023+).

    Intenta primero un regex de 9 caracteres alfanuméricos; si falla,
    parte por el primer separador guion/espacio.
    Devuelve (cif_normalizado, dirección, es_válido, tipo_id).
    """
    if pd.isna(combined): return None, None, False, None
    # D6: normalizar espacios múltiples antes de extraer CIF y dirección
    combined = re.sub(r'\s+', ' ', str(combined).strip())
    potential_match = re.search(r'([A-Z0-9][\s-]*){9}', combined.upper())
    cif_raw = None
    address = combined
    if potential_match:
        cif_raw = potential_match.group(0)
        clean_cif = re.sub(r'[^A-Z0-9]', '', cif_raw)
        if len(clean_cif) == 9:
            # Más de 3 letras → no puede ser un NIF/CIF válido (texto extraído por error)
            if sum(1 for c in clean_cif if c.isalpha()) > 3:
                cif_raw = None
            else:
                address = combined.replace(cif_raw, "").strip("- ").strip()
        else: cif_raw = None
    if not cif_raw:
        parts = re.split(r'[- ]', combined, 1)
        cif_raw = parts[0].strip().upper()
        clean_candidate = re.sub(r'[^A-Z0-9]', '', cif_raw)
        if len(clean_candidate) < 8:
            cif_raw = None
            address = combined
        elif sum(1 for c in clean_candidate if c.isalpha()) > 3:
            # También descartar candidatos con demasiadas letras en el fallback
            cif_raw = None
            address = combined
        else:
            address = parts[1].strip() if len(parts) > 1 else ""
    if cif_raw:
        is_valid, normalized, id_type = validate_spanish_id(cif_raw)
        return normalized, address, is_valid, id_type
    return None, combined, False, "None"


def get_entity_type(cif):
    """Infiere el tipo de entidad a partir del primer carácter del NIF.

    Mapa completo AEAT (personas jurídicas):
      A=SA, B=SL, C/D/H/V=Otras sociedades, E=Com.Bienes,
      F=Cooperativa, G=Asociación, J=Soc.Civil,
      P/Q/S=Adm.Pública, R=Entidad religiosa, U=UTE,
      N/W=Entidad extranjera.
    Personas físicas: dígito=DNI, X/Y/Z=NIE, K/L/M=otros especiales.
    """
    if not cif: return "Desconocido"
    cif = str(cif).upper()
    first_char = cif[0]
    if first_char.isdigit() or first_char in ['X', 'Y', 'Z', 'K', 'L', 'M']:
        return "Autónomo"
    mapping = {
        'A': 'SA',
        'B': 'SL',
        'C': 'Otras sociedades',
        'D': 'Otras sociedades',
        'E': 'Comunidad de Bienes',
        'F': 'Cooperativa',
        'G': 'Asociación',
        'H': 'Otras sociedades',
        'J': 'Sociedad Civil',
        'N': 'Entidad extranjera',
        'P': 'Adm. Pública',
        'Q': 'Adm. Pública',
        'R': 'Entidad religiosa',
        'S': 'Adm. Pública',
        'U': 'UTE',
        'V': 'Otras sociedades',
        'W': 'Entidad extranjera',
    }
    return mapping.get(first_char, "Otras sociedades")


def clean_tipo(tipo):
    """Normaliza el tipo de contrato desde texto libre a una de las
    categorías canónicas: Servicio / Suministro / Obras / Otros.
    """
    if pd.isna(tipo): return "Otros"
    tipo = str(tipo).upper()
    if "SERVICIO" in tipo: return "Servicio"
    if "SUMINISTRO" in tipo: return "Suministro"
    if "OBRA" in tipo: return "Obras"
    return "Otros"


def find_header_row(df_raw):
    """Localiza la fila de cabecera en un Excel crudo escaneando las 10 primeras filas.

    Los Excel municipales incluyen títulos y logos antes de la tabla real.
    Se considera cabecera la primera fila que contenga 'OBJETO' y
    ('ADJUDICATARIO' o 'ADJUDICACIÓN').
    """
    for i in range(min(10, len(df_raw))):
        row_values = [str(x).upper() for x in df_raw.iloc[i].values]
        row_str = " ".join(row_values)
        if 'OBJETO' in row_str and ('ADJUDICATARIO' in row_str or 'ADJUDICACIÓN' in row_str):
            return i
    return None


def parse_adj_dom_cif(combined):
    """Parsea el formato 2022: 'NOMBRE. NIF/CIF CÓDIGO. DIRECCIÓN'.

    En 2022 el ayuntamiento publicó una sola columna con nombre, CIF y
    dirección concatenados. A partir de 2023 pasaron a columnas separadas.

    Maneja erratas frecuentes: NNIF, NIE, CIFF, 'CIF:', 'CIF-' y separadores
    con coma o espacio antes de la etiqueta.
    Devuelve (nombre, cif_normalizado, dirección).
    """
    if pd.isna(combined):
        return "", "", ""
    s = str(combined).strip()
    # Intento principal: etiqueta NIF/NIE/CIF seguida de separador y código
    m = re.search(
        r'[.,\s]\s*(?:N{1,2}IF|NIE|CIF{1,2})[:\s\.\-]+([\w.\-]{8,12})[\.,]?\s*(.*)',
        s, re.IGNORECASE
    )
    if m:
        name = s[:m.start()].strip().rstrip('.,')
        cif_raw = re.sub(r'[^A-Z0-9]', '', m.group(1).upper())
        if len(cif_raw) == 9:
            address = m.group(2).strip().rstrip('.')
            return name, cif_raw, address
    # Fallback: patrón CIF sin etiqueta tras separador (ej. 'NOMBRE. Q-2866001-G DIRECCIÓN')
    m2 = re.search(r'[.,]\s+([A-Z][0-9]{7}[A-Z0-9]|[A-Z]-[0-9]{7}-[A-Z])\s+(.*)', s, re.IGNORECASE)
    if m2:
        name = s[:m2.start()].strip().rstrip('.,')
        cif_raw = re.sub(r'[^A-Z0-9]', '', m2.group(1).upper())
        if len(cif_raw) == 9:
            address = m2.group(2).strip().rstrip('.')
            return name, cif_raw, address
    return s, "", ""


# ── Normalización de áreas ─────────────────────────────────────────────────
# Estructura oficial del Ayuntamiento de Rincón de la Victoria (12 áreas).
# Las ~119 variantes del Excel se mapean a ~30 nombres canónicos.
AREA_MAP = {
    # ── Presidencia ──────────────────────────────────────────────────────────
    "Presidencia":                                        "Presidencia",
    "Alcaldía":                                           "Presidencia",
    "Contratación":                                       "Presidencia",
    "Planes Estratégicos":                                "Presidencia",
    "Planes estratégicos":                                "Presidencia",
    "Relación con los Vecinos del Núcleo de Benagalbón":  "Presidencia",
    "Delegación de Relaciones con los Vecinos de Benagalbón": "Presidencia",

    # ── Comunicación ─────────────────────────────────────────────────────────
    "Comunicación":    "Comunicación",
    "Comunicacion":    "Comunicación",
    "Radio Municipal": "Comunicación",

    # ── Sostenibilidad Medioambiental ─────────────────────────────────────────
    "Sostenibilidad Medioambiental": "Sostenibilidad Medioambiental",
    "Sosteniblidad Medioambiental":  "Sostenibilidad Medioambiental",
    "Medio Ambiente":                "Sostenibilidad Medioambiental",

    # ── Régimen Interior ──────────────────────────────────────────────────────
    "Régimen Interior":   "Régimen Interior",
    "Secretaría":         "Régimen Interior",
    "Intervención":       "Régimen Interior",
    "Estadística":        "Régimen Interior",
    "Padrón":             "Régimen Interior",
    "Régimen Interior, Recursos Humanos, Protocolo, Protección Civil, Relaciones Consorcio Bomberos": "Régimen Interior",

    # ── Recursos Humanos ──────────────────────────────────────────────────────
    "Recursos Humanos": "Recursos Humanos",

    # ── Protocolo ────────────────────────────────────────────────────────────
    "Protocolo": "Protocolo",

    # ── Protección Civil ──────────────────────────────────────────────────────
    "Protección Civil":  "Protección Civil",
    "Protección civil":  "Protección Civil",

    # ── Economía y Hacienda ───────────────────────────────────────────────────
    "Economía y Hacienda": "Economía y Hacienda",
    "Economia y Hacienda": "Economía y Hacienda",
    "Economía":            "Economía y Hacienda",

    # ── Cultura ───────────────────────────────────────────────────────────────
    "Cultura":               "Cultura",
    "cultura":               "Cultura",
    "Cultrura":              "Cultura",
    "Cultura, Feria y Fiestas": "Cultura",

    # ── Ferias y Fiestas ──────────────────────────────────────────────────────
    "Ferias y Fiestas": "Ferias y Fiestas",
    "Feria y Fiestas":  "Ferias y Fiestas",

    # ── Turismo ───────────────────────────────────────────────────────────────
    "Turismo":              "Turismo",
    "turismo":              "Turismo",
    "Cueva del Tesoro-Turismo": "Turismo",

    # ── Cueva del Tesoro ──────────────────────────────────────────────────────
    "Cueva del Tesoro":   "Cueva del Tesoro",
    "Cueval del Tesosro": "Cueva del Tesoro",

    # ── Villa Romana Antíopa ──────────────────────────────────────────────────
    "Villa Antiopa":                    "Villa Romana Antíopa",
    "Villa Antíopa":                    "Villa Romana Antíopa",
    "Villa Antiiopa":                   "Villa Romana Antíopa",
    "Villa Antiíopa":                   "Villa Romana Antíopa",
    "Villa Antíopa y Cueva del Tesoro": "Villa Romana Antíopa",
    "Villa Romana":                     "Villa Romana Antíopa",

    # ── Urbanismo ─────────────────────────────────────────────────────────────
    "Urbanismo":               "Urbanismo",
    "Urbanismo - APAL Deportes": "Urbanismo",
    "Patrimonio":              "Urbanismo",

    # ── Infraestructuras ──────────────────────────────────────────────────────
    "Infraestructuras":  "Infraestructuras",
    "Infraestructura":   "Infraestructuras",
    "Ingfraestructuras": "Infraestructuras",

    # ── Obras y Servicios Generales ───────────────────────────────────────────
    "Obras y Servicios Generales":           "Obras y Servicios Generales",
    "Obars y Servicios Generales":           "Obras y Servicios Generales",
    "Servicios Generales":                   "Obras y Servicios Generales",
    "Servicios Operativos":                  "Obras y Servicios Generales",
    "Servicos Operativos":                   "Obras y Servicios Generales",
    "SSOO con destino Educación":            "Obras y Servicios Generales",
    "Servicios Operativos destino Apal de Deportes": "Obras y Servicios Generales",
    "Servicios Operativos destino Juventud": "Obras y Servicios Generales",
    "Servicios Operativos/Ferias y Fiestas": "Obras y Servicios Generales",
    "Limpieza Edificios municipales":        "Obras y Servicios Generales",
    "Limpieza de edificios":                 "Obras y Servicios Generales",

    # ── Playas ────────────────────────────────────────────────────────────────
    "Playas": "Playas",
    "playas": "Playas",

    # ── Parques y Jardines ────────────────────────────────────────────────────
    "Parques y Jardines": "Parques y Jardines",

    # ── Cementerios y Servicios Fúnebres ─────────────────────────────────────
    "Cementerios y Servicios Fúnebres":   "Cementerios y Servicios Fúnebres",
    "Cementerios y Servicios Funerarios": "Cementerios y Servicios Fúnebres",
    "Cementterios y Servicios Fúnebres":  "Cementerios y Servicios Fúnebres",
    "Cementerios":                         "Cementerios y Servicios Fúnebres",
    "Cementerio":                          "Cementerios y Servicios Fúnebres",

    # ── Seguridad Ciudadana ───────────────────────────────────────────────────
    "Seguridad Ciudadana": "Seguridad Ciudadana",
    "Policía Local":       "Seguridad Ciudadana",

    # ── Bienestar Social ──────────────────────────────────────────────────────
    "Bienestar Social":              "Bienestar Social",
    "Bienestar Social (CMIM)":       "Bienestar Social",
    "Bienestar Social0":             "Bienestar Social",
    "Políticas Sociales":            "Bienestar Social",
    "Politicas Sociales":            "Bienestar Social",
    "Servicios Sociales Comunitarios": "Bienestar Social",

    # ── Educación ─────────────────────────────────────────────────────────────
    "Educación":  "Educación",
    "Educacion":  "Educación",

    # ── Formación y Empleo ────────────────────────────────────────────────────
    "Formación y Empleo":      "Formación y Empleo",
    "Formación y empleo":      "Formación y Empleo",
    "Empleo":                  "Formación y Empleo",
    "Agricultura, Ganadería y Pesca": "Formación y Empleo",

    # ── Juventud ──────────────────────────────────────────────────────────────
    "Juventud": "Juventud",

    # ── Promoción de la Tercera Edad ──────────────────────────────────────────
    "Promoción de la Tercera Edad": "Promoción de la Tercera Edad",
    "Promoción Tercera Edad":       "Promoción de la Tercera Edad",

    # ── Comercio ──────────────────────────────────────────────────────────────
    "Comercio":  "Comercio",
    "comercio":  "Comercio",

    # ── Vía Pública ───────────────────────────────────────────────────────────
    "Vía Pública": "Vía Pública",

    # ── Sanidad y Consumo ─────────────────────────────────────────────────────
    "Sanidad y Consumo": "Sanidad y Consumo",
    "Sanidad Y consumo": "Sanidad y Consumo",
    "Sanidad":           "Sanidad y Consumo",

    # ── Nuevas Tecnologías y Transparencia ────────────────────────────────────
    "Nuevas Tecnologías":                    "Nuevas Tecnologías y Transparencia",
    "Nuevas tecnologías":                    "Nuevas Tecnologías y Transparencia",
    "Transparencia y Participación Ciudadana": "Nuevas Tecnologías y Transparencia",
    "Transparencia y Participación":         "Nuevas Tecnologías y Transparencia",
    "Transparencia":                         "Nuevas Tecnologías y Transparencia",

    # ── Movilidad y Transportes ───────────────────────────────────────────────
    "Transportes":            "Movilidad y Transportes",
    "Transporte":             "Movilidad y Transportes",
    "Transporte Público Urbano": "Movilidad y Transportes",
    "Movilidad":              "Movilidad y Transportes",
    "Movilidad y Transporte": "Movilidad y Transportes",

    # ── Deportes (APAL) ───────────────────────────────────────────────────────
    "Deportes (APAL)": "Deportes (APAL)",
    "Deportes":        "Deportes (APAL)",

    # ── Errores / sin área legible ────────────────────────────────────────────
    "De":        None,   # valor truncado, sin área
    "Suministro": None,  # tipo de contrato, no área
}


def normalize_expediente(exp):
    """Normaliza el expediente a formato NÚMERO/AA (año de 2 cifras, sin espacios).
    Ejemplo: '123/2024' → '123/24'.
    """
    if not exp or pd.isna(exp):
        return exp
    exp = str(exp).strip()
    m = re.match(r'^(\d+)/(20(\d{2}))$', exp)
    if m:
        return f"{m.group(1)}/{m.group(3)}"
    return exp


def normalize_area(area):
    """Normaliza el nombre de un área al canónico definido en AREA_MAP."""
    if pd.isna(area) or area is None:
        return None
    stripped = str(area).strip()
    if stripped in AREA_MAP:
        return AREA_MAP[stripped]
    # Área genuinamente nueva no contemplada: mantenerla y avisarla
    import warnings
    warnings.warn(f"Área no mapeada: {repr(stripped)}", stacklevel=2)
    return stripped


def sanitize_text(text):
    """Limpia un campo de texto: elimina saltos de línea, tabulaciones
    y espacios múltiples, preservando tildes y caracteres españoles.
    """
    if not text or pd.isna(text): return ""
    text = str(text).strip()
    text = re.sub(r'[\r\n\t]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def preprocess_date(val):
    """Normaliza una fecha antes de pasarla a pd.to_datetime.

    Convierte años de 2 dígitos en 4 dígitos asumiendo el rango 20xx.
    Soporta formatos comunes: DD/MM/YY, DD-MM-YY, MM/DD/YY.
    """
    if pd.isna(val): return val
    s = str(val).strip()
    # Formato DD/MM/YY o DD-MM-YY con año de 2 dígitos
    m = re.match(r'^(\d{1,2})[/\-](\d{1,2})[/\-](\d{2})$', s)
    if m:
        d, mo, y = m.group(1), m.group(2), m.group(3)
        return f"{d}/{mo}/20{y}"
    return val


# ── Pipeline principal ───────────────────────────────────────

def process_files(raw_dir='data/raw', processed_dir='data/processed'):
    """Lee todos los Excel de raw_dir, normaliza y valida los datos,
    y genera el star schema completo en processed_dir.

    Pasos internos:
      1. Detectar cabecera y leer cada hoja relevante del Excel
      2. Detectar formato 2022 (columna combinada) y dividirla
      3. Extraer y validar CIF, normalizar áreas, tipos y fechas
      4. Propagar CIFs: si el mismo nombre aparece sin CIF en otro registro,
         se completa con el CIF conocido (por moda del nombre)
      5. Construir dimensiones (contratistas, áreas, tipos) y tabla de hechos
      6. Generar contracts.json reducido solo con los campos del frontend
    """
    all_data = []

    # Mapeo de nombres de columna del Excel → nombres internos
    # Necesario porque el ayuntamiento usa variantes y abreviaciones distintas por año
    column_mapping = {
        'CIF/DOMICILIO': 'cif_domicilio', 'ADJUDICATARIO': 'adjudicatario',
        'EXPTE.': 'expediente', 'Nº DE ORDEN': 'n_orden', 'Nº DE ÓRDEN': 'n_orden',
        'ÓRDEN DE RESOLUCIÓN': 'n_orden', 'OBJETO': 'objeto',
        'DURACIÓN/ PLAZO ENTREGA': 'duracion_plazo', 'DURA CIÓN/ PLAZO ENTREG': 'duracion_plazo',
        'CONTRATO': 'tipo_contrato', 'IMPORTE DEL CONTRATO (IVA incluido)': 'importe',
        'IMPORTE DEL CONTRATO (IVA inclu': 'importe', 'IMPORTE DEL CONTRATO': 'importe',
        'ÁREA': 'area', 'FECHA DE ADJUDICACIÓN': 'fecha_adjudicacion'
    }

    raw_files = sorted([f for f in os.listdir(raw_dir) if f.endswith('.xls') or f.endswith('.xlsx')])

    for filename in raw_files:
        file_path = os.path.join(raw_dir, filename)
        print(f"Processing {filename}...")
        try:
            engine = 'xlrd' if file_path.endswith('.xls') else 'openpyxl'
            if engine == 'xlrd':
                import xlrd as _xlrd
                sheet_names = _xlrd.open_workbook(file_path, on_demand=True).sheet_names()
            else:
                import openpyxl as _openpyxl
                sheet_names = _openpyxl.load_workbook(file_path, read_only=True).sheetnames
            for sheet_name in sheet_names:
                uname = sheet_name.upper()
                # Solo procesar hojas del Ayuntamiento o de Deportes (APAL); ignorar índices y portadas
                if not ('AYTO' in uname or 'DEP' in uname or 'CONTRATO' in uname): continue
                print(f"  Reading sheet: {sheet_name}")
                df_raw = pd.read_excel(file_path, engine=engine, sheet_name=sheet_name, header=None)
                header_row = find_header_row(df_raw)
                if header_row is None: continue
                df = pd.read_excel(file_path, engine=engine, sheet_name=sheet_name, header=header_row)
                df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
                # Guardar la fila raw completa para trazabilidad en el informe de auditoría
                df['raw_row'] = df.apply(lambda row: " || ".join([str(val).strip() for val in row.values]), axis=1)
                df['source_file'] = filename
                df.columns = [str(c).replace('\n', ' ').strip() for c in df.columns]
                # Formato 2022: columna única 'ADJUDICATARIO/DOMICILIO/CIF' → separar en columnas estándar
                if 'ADJUDICATARIO/DOMICILIO/CIF' in df.columns:
                    parsed = df['ADJUDICATARIO/DOMICILIO/CIF'].apply(parse_adj_dom_cif)
                    df['ADJUDICATARIO'] = parsed.apply(lambda x: x[0])
                    df['CIF/DOMICILIO'] = parsed.apply(lambda x: x[1])
                    df.drop(columns=['ADJUDICATARIO/DOMICILIO/CIF'], inplace=True)
                rename_dict = {}
                for col in df.columns:
                    for possible, internal in column_mapping.items():
                        if possible == col or col.startswith(possible[:20]):
                            rename_dict[col] = internal
                            break
                df = df.rename(columns=rename_dict)
                valid_cols = [c for c in df.columns if c in list(column_mapping.values()) + ['raw_row', 'source_file']]
                df = df[valid_cols]
                if df.empty: continue
                # Asegurar columnas obligatorias que pueden faltar en ficheros más antiguos
                if 'fecha_adjudicacion' not in df.columns:
                    df['fecha_adjudicacion'] = pd.NaT
                if 'area' not in df.columns:
                    df['area'] = None
                df['importe'] = df['importe'].apply(clean_amount)
                # D4: normalizar años de 2 dígitos antes del parseo
                if 'fecha_adjudicacion' in df.columns:
                    df['fecha_adjudicacion'] = df['fecha_adjudicacion'].apply(preprocess_date)
                df['fecha_adjudicacion'] = pd.to_datetime(df['fecha_adjudicacion'], errors='coerce')
                if not df.empty:
                    _max_year = datetime.now().year + 1
                    # Nulificar fechas fuera del rango válido (artefactos de parseo del Excel)
                    df.loc[(df['fecha_adjudicacion'].dt.year < 2020) | (df['fecha_adjudicacion'].dt.year > _max_year), 'fecha_adjudicacion'] = pd.NaT
                df['adjudicatario'] = df['adjudicatario'].apply(sanitize_text)
                # D5: sanear también el campo objeto
                if 'objeto' in df.columns:
                    df['objeto'] = df['objeto'].apply(sanitize_text)
                if 'expediente' in df.columns:
                    df['expediente'] = df['expediente'].apply(normalize_expediente)
                extracted = df['cif_domicilio'].apply(extract_cif_address)
                df['cif'] = extracted.apply(lambda x: x[0])
                df['domicilio_limpio'] = extracted.apply(lambda x: sanitize_text(x[1]))
                df['check_cif'] = extracted.apply(lambda x: x[2])
                df['tipo_id'] = extracted.apply(lambda x: x[3])
                df['tipo_entidad'] = df['cif'].apply(get_entity_type)
                # Las hojas DEP pertenecen siempre al área Deportes (APAL)
                if 'DEP' in uname: df['area'] = 'Deportes (APAL)'
                match = re.search(r'(\d{4})_Q(\d)', filename)
                if match:
                    df['year'] = int(match.group(1))
                    df['quarter'] = int(match.group(2))
                all_data.append(df)
        except Exception as e: print(f"  Error processing {filename}: {e}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        # Eliminar filas ruido: adjudicatario Y objeto en blanco → no es un contrato
        _blank = lambda s: s.isna() | (s.astype(str).str.strip() == '')
        noise = _blank(final_df['adjudicatario']) & _blank(final_df['objeto'])
        dropped = noise.sum()
        if dropped:
            print(f"  ⚠ Eliminadas {dropped} filas sin adjudicatario ni objeto (ruido)")
        final_df = final_df[~noise]
        final_df['area'] = final_df['area'].apply(normalize_area)

        # Propagación de CIFs: si el mismo nombre aparece sin CIF en algún registro,
        # se completa con el CIF más frecuente conocido para ese nombre
        print("Unifying and Propagating...")
        name_to_cif = {}
        valid_cifs = final_df[final_df['check_cif']].dropna(subset=['cif', 'adjudicatario'])
        for name, group in valid_cifs.groupby('adjudicatario'):
            if name:
                # D8: usar el CIF del fichero más reciente (ej. 2024_Q4 > 2023_Q1)
                name_to_cif[name.upper().strip()] = group.sort_values('source_file').iloc[-1]['cif']

        def fill_cif(row):
            if not row['check_cif'] or pd.isna(row['cif']):
                return name_to_cif.get(str(row['adjudicatario']).upper().strip(), row['cif'])
            return row['cif']
        final_df['cif'] = final_df.apply(fill_cif, axis=1)
        final_df['check_cif'] = final_df['cif'].apply(lambda x: validate_spanish_id(x)[0])
        final_df['tipo_entidad'] = final_df['cif'].apply(get_entity_type)
        final_df['tipo_contrato_limpio'] = final_df['tipo_contrato'].apply(clean_tipo)

        # Dimensión contratistas: nombre y dirección canónicos = moda por CIF
        print("Generating Dimensions...")
        cif_canonical = {}
        cif_address = {}
        entity_df = final_df.dropna(subset=['cif'])
        for cif, group in entity_df.groupby('cif'):
            # D8: nombre y dirección canónicos desde el fichero más reciente
            sorted_group = group.sort_values('source_file')
            valid_names = sorted_group.dropna(subset=['adjudicatario'])
            if not valid_names.empty:
                cif_canonical[cif] = valid_names.iloc[-1]['adjudicatario']
            valid_addr = sorted_group.dropna(subset=['domicilio_limpio'])
            if not valid_addr.empty:
                cif_address[cif] = valid_addr.iloc[-1]['domicilio_limpio']

        dim_contractors = final_df.dropna(subset=['cif']).drop_duplicates('cif').copy()
        dim_contractors['nombre'] = dim_contractors['cif'].apply(lambda x: cif_canonical.get(x, ""))
        dim_contractors['direccion'] = dim_contractors['cif'].apply(lambda x: cif_address.get(x, ""))
        dim_contractors = dim_contractors[['cif', 'nombre', 'direccion', 'tipo_entidad', 'tipo_id']]

        # Dimensión áreas
        areas = sorted(final_df['area'].dropna().unique())
        dim_areas = pd.DataFrame({'id': range(1, len(areas)+1), 'nombre': areas})
        area_to_id = dict(zip(dim_areas['nombre'], dim_areas['id']))

        # Dimensión tipos de contrato
        types = sorted(final_df['tipo_contrato_limpio'].dropna().unique())
        dim_types = pd.DataFrame({'id': range(1, len(types)+1), 'nombre': types})
        type_to_id = dict(zip(dim_types['nombre'], dim_types['id']))

        # Tabla de hechos: ID sintético por MD5 de los campos clave del contrato
        print("Generating Fact Table...")
        final_df['area_id'] = final_df['area'].map(area_to_id)
        final_df['tipo_id'] = final_df['tipo_contrato_limpio'].map(type_to_id)
        final_df['contrato_id'] = final_df.apply(
            lambda r: hashlib.md5(f"{r['expediente']}{r['objeto']}{r['importe']}{r['fecha_adjudicacion']}".encode()).hexdigest(),
            axis=1
        )
        fact_contracts = final_df[['contrato_id', 'cif', 'area_id', 'tipo_id', 'importe', 'fecha_adjudicacion', 'objeto', 'expediente', 'quarter', 'year', 'source_file']].copy()

        # Resumen agregado por contratista para la pestaña Contratistas del frontend
        print("Generating Contractors Summary...")
        summary = []
        for cif, group in final_df.groupby('cif'):
            contractor_row = dim_contractors[dim_contractors['cif'] == cif]
            if contractor_row.empty: continue
            contractor_info = contractor_row.iloc[0].to_dict()
            contract_cols = ['fecha_adjudicacion', 'objeto', 'expediente', 'importe', 'area', 'tipo_contrato_limpio']
            contracts_list = (
                group[contract_cols]
                .sort_values('fecha_adjudicacion', ascending=False)
                .replace({pd.NA: None, pd.NaT: None})
                .to_dict('records')
            )
            summary.append({
                **contractor_info,
                'total_importe': float(group['importe'].sum()),
                'num_contratos': int(len(group)),
                'contratos': contracts_list
            })
        summary = sorted(summary, key=lambda x: x['total_importe'], reverse=True)

        # Serialización a JSON: pd.Timestamp no es serializable por defecto
        def default_ser(obj):
            if isinstance(obj, pd.Timestamp): return obj.isoformat()
            return str(obj)

        os.makedirs(processed_dir, exist_ok=True)
        final_df.to_csv(os.path.join(processed_dir, 'contracts.csv'), index=False)

        with open(os.path.join(processed_dir, 'dim_contractors.json'), 'w', encoding='utf-8') as f:
            json.dump(dim_contractors.to_dict('records'), f, indent=2, ensure_ascii=False)
        with open(os.path.join(processed_dir, 'dim_areas.json'), 'w', encoding='utf-8') as f:
            json.dump(dim_areas.to_dict('records'), f, indent=2, ensure_ascii=False)
        with open(os.path.join(processed_dir, 'dim_types.json'), 'w', encoding='utf-8') as f:
            json.dump(dim_types.to_dict('records'), f, indent=2, ensure_ascii=False)
        with open(os.path.join(processed_dir, 'fact_contracts.json'), 'w', encoding='utf-8') as f:
            json.dump(fact_contracts.replace({pd.NA: None, pd.NaT: None}).to_dict('records'), f, indent=2, ensure_ascii=False, default=default_ser)
        with open(os.path.join(processed_dir, 'contractors_summary.json'), 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False, default=default_ser)

        # contracts.json: versión reducida con solo los campos que usa el frontend
        frontend_cols = [
            'fecha_adjudicacion', 'adjudicatario', 'cif', 'tipo_entidad',
            'objeto', 'area', 'importe', 'year', 'quarter',
            'tipo_contrato_limpio', 'expediente', 'contrato_id',
        ]
        existing_cols = [c for c in frontend_cols if c in final_df.columns]
        contracts_frontend = final_df[existing_cols].replace({pd.NA: None, pd.NaT: None})
        with open(os.path.join(processed_dir, 'contracts.json'), 'w', encoding='utf-8') as f:
            json.dump(contracts_frontend.to_dict('records'), f, indent=2, ensure_ascii=False, default=default_ser)

        print(f"Extraction complete. {len(final_df)} records processed.")

if __name__ == "__main__":
    process_files()
