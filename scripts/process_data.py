import pandas as pd
import os
import re
import xlrd
import json
import hashlib

def clean_amount(val):
    if pd.isna(val): return 0.0
    if isinstance(val, (int, float)): return float(val)
    val = str(val).replace('€', '').strip()
    if not val: return 0.0
    val = val.replace('.', '').replace(',', '.')
    try:
        return float(val)
    except:
        return 0.0

def validate_spanish_id(code):
    if not code or not isinstance(code, str):
        return False, None, "Invalid"
    raw = code.upper().strip()
    clean = re.sub(r'[^A-Z0-9]', '', raw)
    if len(clean) != 9:
        return False, clean, "Unknown"
    first = clean[0]
    middle = clean[1:8]
    last = clean[8]
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
    if first in 'ABCDEFGHJNPQRSUVW':
        try:
            if not middle.isdigit(): return False, clean, "CIF"
            a = sum(int(middle[i]) for i in [1, 3, 5])
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
    if pd.isna(combined): return None, None, False, None
    combined = str(combined).strip()
    potential_match = re.search(r'([A-Z0-9][\s-]*){9}', combined.upper())
    cif_raw = None
    address = combined
    if potential_match:
        cif_raw = potential_match.group(0)
        clean_cif = re.sub(r'[^A-Z0-9]', '', cif_raw)
        if len(clean_cif) == 9:
            address = combined.replace(cif_raw, "").strip("- ").strip()
        else: cif_raw = None
    if not cif_raw:
        parts = re.split(r'[- ]', combined, 1)
        cif_raw = parts[0].strip().upper()
        if len(re.sub(r'[^A-Z0-9]', '', cif_raw)) < 8:
            cif_raw = None
            address = combined
        else: address = parts[1].strip() if len(parts) > 1 else ""
    if cif_raw:
        is_valid, normalized, id_type = validate_spanish_id(cif_raw)
        return normalized, address, is_valid, id_type
    return None, combined, False, "None"

def get_entity_type(cif):
    if not cif: return "Desconocido"
    cif = str(cif).upper()
    first_char = cif[0]
    if first_char.isdigit() or first_char in ['X', 'Y', 'Z']: return "Autónomo"
    mapping = {
        'A': 'SA', 'B': 'SL', 'N': 'SL', 'G': 'Asociación',
        'P': 'Adm. Pública', 'Q': 'Adm. Pública', 'S': 'Adm. Pública',
        'U': 'UTE', 'E': 'Comunidad de Bienes', 'F': 'Cooperativa',
        'J': 'Sociedad Civil', 'V': 'Sociedad Agraria de Transformación'
    }
    return mapping.get(first_char, "Empresa/Otros")

def clean_tipo(tipo):
    if pd.isna(tipo): return "Otros"
    tipo = str(tipo).upper()
    if "SERVICIO" in tipo: return "Servicio"
    if "SUMINISTRO" in tipo: return "Suministro"
    if "OBRA" in tipo: return "Obras"
    return "Otros"

def find_header_row(df_raw):
    for i in range(min(10, len(df_raw))):
        row_values = [str(x).upper() for x in df_raw.iloc[i].values]
        row_str = " ".join(row_values)
        if 'OBJETO' in row_str and ('ADJUDICATARIO' in row_str or 'ADJUDICACIÓN' in row_str):
            return i
    return None

def parse_adj_dom_cif(combined):
    """
    Parses the 2022 format: 'NOMBRE. NIF/CIF CÓDIGO. DIRECCIÓN'
    Handles typos and variants: NNIF, NIE, CIFF, CIF: CIF. CIF- and comma/space before label.
    Returns (name, cif_code, address).
    """
    if pd.isna(combined):
        return "", "", ""
    s = str(combined).strip()
    # Primary: labeled NIF/NIE/CIF with various separators before and after
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
    # Fallback: unlabeled CIF pattern after separator (e.g. 'NOMBRE. Q-2866001-G DIRECCIÓN')
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
    "Villa Antiiopa":                   "Villa Romana Antíopa",
    "Villa Antíopa y Cueva del Tesoro": "Villa Romana Antíopa",
    "Villa Romana":                     "Villa Romana Antíopa",
    "Villa Antiiopa":                   "Villa Romana Antíopa",

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
    """Normaliza el expediente a formato NÚMERO/AA (año de 2 cifras, sin espacios)."""
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
    if not text or pd.isna(text): return ""
    # Remove anomalous characters, extra dots, commas and normalize spaces
    text = str(text).strip()
    # Replace common issues (like the ones the user might be seeing)
    text = re.sub(r'[\r\n\t]', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    # Basic cleaning but preserving accents and Spanish chars
    return text.strip()


def process_files():
    raw_dir = 'data/raw'
    processed_dir = 'data/processed'
    all_data = []

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
            book = xlrd.open_workbook(file_path, on_demand=True)
            for sheet_name in book.sheet_names():
                uname = sheet_name.upper()
                if not ('AYTO' in uname or 'DEP' in uname or 'CONTRATO' in uname): continue
                print(f"  Reading sheet: {sheet_name}")
                df_raw = pd.read_excel(file_path, engine='xlrd', sheet_name=sheet_name, header=None)
                header_row = find_header_row(df_raw)
                if header_row is None: continue
                df = pd.read_excel(file_path, engine='xlrd', sheet_name=sheet_name, header=header_row)
                df = df.dropna(how='all', axis=0).dropna(how='all', axis=1)
                df['raw_row'] = df.apply(lambda row: " || ".join([str(val).strip() for val in row.values]), axis=1)
                df['source_file'] = filename
                df.columns = [str(c).replace('\n', ' ').strip() for c in df.columns]
                # 2022 format: combined 'ADJUDICATARIO/DOMICILIO/CIF' → split into standard columns
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
                # Ensure required columns exist (older files may lack them)
                if 'fecha_adjudicacion' not in df.columns:
                    df['fecha_adjudicacion'] = pd.NaT
                if 'area' not in df.columns:
                    df['area'] = None
                df['importe'] = df['importe'].apply(clean_amount)
                df['fecha_adjudicacion'] = pd.to_datetime(df['fecha_adjudicacion'], errors='coerce')
                if not df.empty:
                    df.loc[(df['fecha_adjudicacion'].dt.year < 2020) | (df['fecha_adjudicacion'].dt.year > 2026), 'fecha_adjudicacion'] = pd.NaT
                
                # Pre-clean names in the raw dataframe
                df['adjudicatario'] = df['adjudicatario'].apply(sanitize_text)
                if 'expediente' in df.columns:
                    df['expediente'] = df['expediente'].apply(normalize_expediente)
                
                extracted = df['cif_domicilio'].apply(extract_cif_address)
                df['cif'] = extracted.apply(lambda x: x[0])
                df['domicilio_limpio'] = extracted.apply(lambda x: sanitize_text(x[1]))
                df['check_cif'] = extracted.apply(lambda x: x[2])
                df['tipo_id'] = extracted.apply(lambda x: x[3])
                df['tipo_entidad'] = df['cif'].apply(get_entity_type)
                if 'DEP' in uname: df['area'] = 'Deportes (APAL)'
                match = re.search(r'(\d{4})_Q(\d)', filename)
                if match:
                    df['year'] = int(match.group(1))
                    df['quarter'] = int(match.group(2))
                all_data.append(df)
        except Exception as e: print(f"  Error processing {filename}: {e}")

    if all_data:
        final_df = pd.concat(all_data, ignore_index=True)
        final_df = final_df.dropna(subset=['adjudicatario', 'objeto'], how='all')
        
        # Normalizar nombres de área
        final_df['area'] = final_df['area'].apply(normalize_area)

        # Propagating CIFs and Unifying Names
        print("Unifying and Propagating...")
        name_to_cif = {}
        # Use sanitized names for propagation logic
        valid_cifs = final_df[final_df['check_cif']].dropna(subset=['cif', 'adjudicatario'])
        for name, group in valid_cifs.groupby('adjudicatario'):
            if name:
                name_to_cif[name.upper().strip()] = group['cif'].mode()[0]
        
        def fill_cif(row):
            if not row['check_cif'] or pd.isna(row['cif']):
                return name_to_cif.get(str(row['adjudicatario']).upper().strip(), row['cif'])
            return row['cif']
        final_df['cif'] = final_df.apply(fill_cif, axis=1)
        final_df['check_cif'] = final_df['cif'].apply(lambda x: validate_spanish_id(x)[0])
        final_df['tipo_entidad'] = final_df['cif'].apply(get_entity_type)
        final_df['tipo_contrato_limpio'] = final_df['tipo_contrato'].apply(clean_tipo)

        # Dimension: Contractors - Build canonical name and address using MODE per CIF
        print("Generating Dimensions...")
        cif_canonical = {}
        cif_address = {}
        # Group by CIF to find the most used name AND address
        entity_df = final_df.dropna(subset=['cif'])
        for cif, group in entity_df.groupby('cif'):
            if not group['adjudicatario'].dropna().empty:
                cif_canonical[cif] = group['adjudicatario'].mode()[0]
            if not group['domicilio_limpio'].dropna().empty:
                cif_address[cif] = group['domicilio_limpio'].mode()[0]
        
        dim_contractors = final_df.dropna(subset=['cif']).drop_duplicates('cif').copy()
        dim_contractors['nombre'] = dim_contractors['cif'].apply(lambda x: cif_canonical.get(x, ""))
        dim_contractors['direccion'] = dim_contractors['cif'].apply(lambda x: cif_address.get(x, ""))
        dim_contractors = dim_contractors[['cif', 'nombre', 'direccion', 'tipo_entidad', 'tipo_id']]
        
        # Dimension: Areas
        areas = sorted(final_df['area'].dropna().unique())
        dim_areas = pd.DataFrame({'id': range(1, len(areas)+1), 'nombre': areas})
        area_to_id = dict(zip(dim_areas['nombre'], dim_areas['id']))
        
        # Dimension: Types
        types = sorted(final_df['tipo_contrato_limpio'].dropna().unique())
        dim_types = pd.DataFrame({'id': range(1, len(types)+1), 'nombre': types})
        type_to_id = dict(zip(dim_types['nombre'], dim_types['id']))
        
        # Data Modeling: Fact Table
        print("Generating Fact Table...")
        final_df['area_id'] = final_df['area'].map(area_to_id)
        final_df['tipo_id'] = final_df['tipo_contrato_limpio'].map(type_to_id)
        final_df['contrato_id'] = final_df.apply(lambda r: hashlib.md5(f"{r['expediente']}{r['objeto']}{r['importe']}{r['fecha_adjudicacion']}".encode()).hexdigest(), axis=1)
        
        fact_contracts = final_df[['contrato_id', 'cif', 'area_id', 'tipo_id', 'importe', 'fecha_adjudicacion', 'objeto', 'expediente', 'quarter', 'year', 'source_file']].copy()
        
        # Aggregated Summary for Web
        print("Generating Contractors Summary...")
        summary = []
        for cif, group in final_df.groupby('cif'):
            # Fetch from dimension to ensure canonical name/address
            contractor_row = dim_contractors[dim_contractors['cif'] == cif]
            if contractor_row.empty: continue
            contractor_info = contractor_row.iloc[0].to_dict()
            
            contracts_list = group.sort_values('fecha_adjudicacion', ascending=False).replace({pd.NA: None, pd.NaT: None}).where(pd.notnull(group), None).to_dict('records')
            summary.append({
                **contractor_info,
                'total_importe': float(group['importe'].sum()),
                'num_contratos': int(len(group)),
                'contratos': contracts_list
            })
        summary = sorted(summary, key=lambda x: x['total_importe'], reverse=True)

        # Save all
        os.makedirs(processed_dir, exist_ok=True)
        final_df.to_csv(os.path.join(processed_dir, 'contracts.csv'), index=False)
        
        # Helper to handle dates in JSON
        def default_ser(obj):
            if isinstance(obj, pd.Timestamp): return obj.isoformat()
            return str(obj)

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
        
        # Keep original contracts.json for backward compatibility or direct use
        with open(os.path.join(processed_dir, 'contracts.json'), 'w', encoding='utf-8') as f:
            json.dump(final_df.replace({pd.NA: None, pd.NaT: None}).to_dict('records'), f, indent=2, ensure_ascii=False, default=default_ser)
            
        print(f"Extraction complete. {len(final_df)} records processed.")

if __name__ == "__main__":
    process_files()

