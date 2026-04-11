"""
Tests unitarios del ETL — scripts/process_data.py

Cubre las funciones puras del pipeline: validación de NIFs,
clasificación de entidades, normalización de tipos e importes,
parseo del formato 2022 y detección de cabecera en Excel.
"""

import pytest
import pandas as pd
from process_data import (
    validate_spanish_id,
    get_entity_type,
    clean_tipo,
    clean_amount,
    parse_adj_dom_cif,
    find_header_row,
    preprocess_date,
)


# ── validate_spanish_id ──────────────────────────────────────────────────────

class TestValidateSpanishId:

    def test_nif_valido(self):
        # 12345678Z es un NIF real válido (12345678 % 23 = 25 → 'Z')
        valid, normalized, tipo = validate_spanish_id('12345678Z')
        assert valid is True
        assert normalized == '12345678Z'
        assert tipo == 'NIF'

    def test_nif_invalido_letra_incorrecta(self):
        valid, _, tipo = validate_spanish_id('12345678A')
        assert valid is False
        assert tipo == 'NIF'

    def test_nie_valido(self):
        # X1234567L: X→0, 01234567 % 23 = 11 → 'L'
        valid, normalized, tipo = validate_spanish_id('X1234567L')
        assert valid is True
        assert normalized == 'X1234567L'
        assert tipo == 'NIF'

    def test_nie_y_valido(self):
        # Y1234567X: Y→1, 11234567 % 23 = 10 → 'X'
        valid, normalized, tipo = validate_spanish_id('Y1234567X')
        assert valid is True

    def test_cif_sa_valido(self):
        # A08015497 — CIF de Banco Santander (ejemplo de dominio público)
        valid, normalized, tipo = validate_spanish_id('A08015497')
        assert valid is True
        assert tipo == 'CIF'

    def test_cif_sl_valido(self):
        # B12345674 — CIF con control numérico calculado: e=4, B∈ABEH → str(e)
        valid, normalized, tipo = validate_spanish_id('B12345674')
        assert valid is True
        assert tipo == 'CIF'

    def test_cif_organismo_publico(self):
        # Q-letras de control: letra. Q2866001G (Univ. Málaga — dominio público)
        valid, normalized, tipo = validate_spanish_id('Q2866001G')
        assert valid is True
        assert tipo == 'CIF'

    def test_longitud_incorrecta(self):
        valid, _, tipo = validate_spanish_id('1234567')
        assert valid is False
        assert tipo == 'Unknown'

    def test_nulo(self):
        valid, normalized, _ = validate_spanish_id(None)
        assert valid is False
        assert normalized is None

    def test_cadena_vacia(self):
        valid, _, _ = validate_spanish_id('')
        assert valid is False

    def test_normaliza_minusculas(self):
        valid, normalized, _ = validate_spanish_id('12345678z')
        assert normalized == '12345678Z'

    def test_elimina_guiones(self):
        # Formato con guiones: A-0801549-7
        valid, normalized, tipo = validate_spanish_id('A-0801549-7')
        assert normalized == 'A08015497'


# ── get_entity_type ──────────────────────────────────────────────────────────

class TestGetEntityType:

    def test_sa(self):
        assert get_entity_type('A12345678') == 'SA'

    def test_sl(self):
        assert get_entity_type('B12345678') == 'SL'

    def test_autonomo_dni(self):
        assert get_entity_type('12345678Z') == 'Autónomo'

    def test_autonomo_nie_x(self):
        assert get_entity_type('X1234567L') == 'Autónomo'

    def test_autonomo_nie_y(self):
        assert get_entity_type('Y0000000T') == 'Autónomo'

    def test_autonomo_nie_z(self):
        assert get_entity_type('Z1234567R') == 'Autónomo'

    def test_autonomo_k(self):
        assert get_entity_type('K1234567X') == 'Autónomo'

    def test_autonomo_l(self):
        assert get_entity_type('L1234567X') == 'Autónomo'

    def test_autonomo_m(self):
        assert get_entity_type('M1234567X') == 'Autónomo'

    def test_asociacion(self):
        assert get_entity_type('G12345678') == 'Asociación'

    def test_adm_publica_p(self):
        assert get_entity_type('P1234567A') == 'Adm. Pública'

    def test_adm_publica_q(self):
        assert get_entity_type('Q2866001G') == 'Adm. Pública'

    def test_adm_publica_s(self):
        assert get_entity_type('S1234567X') == 'Adm. Pública'

    def test_ute(self):
        assert get_entity_type('U12345678') == 'UTE'

    def test_cooperativa(self):
        assert get_entity_type('F12345678') == 'Cooperativa'

    def test_comunidad_bienes(self):
        assert get_entity_type('E12345678') == 'Comunidad de Bienes'

    def test_sociedad_civil(self):
        assert get_entity_type('J12345678') == 'Sociedad Civil'

    def test_entidad_extranjera_n(self):
        assert get_entity_type('N12345678') == 'Entidad extranjera'

    def test_entidad_extranjera_w(self):
        assert get_entity_type('W12345678') == 'Entidad extranjera'

    def test_entidad_religiosa(self):
        assert get_entity_type('R12345678') == 'Entidad religiosa'

    def test_otras_sociedades_v(self):
        assert get_entity_type('V12345678') == 'Otras sociedades'

    def test_otras_sociedades_c(self):
        assert get_entity_type('C12345678') == 'Otras sociedades'

    def test_otras_sociedades_d(self):
        assert get_entity_type('D12345678') == 'Otras sociedades'

    def test_otras_sociedades_h(self):
        assert get_entity_type('H12345678') == 'Otras sociedades'

    def test_desconocido_nulo(self):
        assert get_entity_type(None) == 'Desconocido'

    def test_desconocido_vacio(self):
        assert get_entity_type('') == 'Desconocido'

    def test_fallback_letra_desconocida(self):
        assert get_entity_type('T12345678') == 'Otras sociedades'

    def test_minusculas_normalizadas(self):
        assert get_entity_type('b12345678') == 'SL'


# ── clean_tipo ───────────────────────────────────────────────────────────────

class TestCleanTipo:

    def test_servicio(self):
        assert clean_tipo('Contrato de Servicios') == 'Servicio'

    def test_servicio_mayusculas(self):
        assert clean_tipo('SERVICIO DE LIMPIEZA') == 'Servicio'

    def test_suministro(self):
        assert clean_tipo('Suministro de material') == 'Suministro'

    def test_obras(self):
        assert clean_tipo('Obras de reforma') == 'Obras'

    def test_obra_singular(self):
        assert clean_tipo('OBRA MENOR') == 'Obras'

    def test_nulo(self):
        assert clean_tipo(None) == 'Otros'

    def test_nan(self):
        assert clean_tipo(float('nan')) == 'Otros'

    def test_texto_desconocido(self):
        assert clean_tipo('Arrendamiento') == 'Otros'

    def test_texto_vacio(self):
        assert clean_tipo('') == 'Otros'


# ── clean_amount ─────────────────────────────────────────────────────────────

class TestCleanAmount:

    def test_formato_europeo(self):
        assert clean_amount('1.234,56 €') == pytest.approx(1234.56)

    def test_formato_europeo_sin_simbolo(self):
        assert clean_amount('1.234,56') == pytest.approx(1234.56)

    def test_float_directo(self):
        assert clean_amount(1234.56) == pytest.approx(1234.56)

    def test_int_directo(self):
        assert clean_amount(500) == pytest.approx(500.0)

    def test_nulo(self):
        assert clean_amount(None) == 0.0

    def test_nan(self):
        assert clean_amount(float('nan')) == 0.0

    def test_cadena_vacia(self):
        assert clean_amount('') == 0.0

    def test_cero(self):
        assert clean_amount('0') == 0.0

    def test_numero_simple(self):
        assert clean_amount('500') == pytest.approx(500.0)

    # D2: formatos adicionales detectados en ficheros reales

    def test_simbolo_euro_prefijo(self):
        # '€1.234,56' — símbolo delante
        assert clean_amount('€1.234,56') == pytest.approx(1234.56)

    def test_simbolo_euro_sufijo(self):
        assert clean_amount('1.234,56€') == pytest.approx(1234.56)

    def test_coma_como_miles(self):
        # '1,234,567' — coma como separador de miles (formato anglosajón en algunos Excel)
        assert clean_amount('1,234,567') == pytest.approx(1234567.0)

    def test_notacion_cientifica(self):
        # Valor exportado como 1.5e3 por Excel
        assert clean_amount('1,5e3') == pytest.approx(1500.0)

    def test_dos_puntos_decimales(self):
        # '6.705.88' → 6705.88 (dos puntos, último grupo = 2 dígitos → separador decimal)
        assert clean_amount('6.705.88') == pytest.approx(6705.88)

    def test_tres_puntos_ambiguo_nullificado(self):
        # '1.234.5678' — último grupo ≥ 4 dígitos → ambiguo → None
        assert clean_amount('1.234.5678') is None


# ── preprocess_date ───────────────────────────────────────────────────────────

class TestPreprocessDate:

    def test_año_dos_digitos_slash(self):
        assert preprocess_date('15/03/24') == '15/03/2024'

    def test_año_dos_digitos_guion(self):
        assert preprocess_date('5-1-23') == '5/1/2023'

    def test_año_cuatro_digitos_sin_cambio(self):
        # Valor ya con año de 4 dígitos: se devuelve tal cual
        result = preprocess_date('15/03/2024')
        assert result == '15/03/2024'

    def test_nulo(self):
        assert pd.isna(preprocess_date(None))

    def test_nan(self):
        import math
        assert pd.isna(preprocess_date(float('nan')))


# ── parse_adj_dom_cif ────────────────────────────────────────────────────────

class TestParseAdjDomCif:

    def test_formato_con_etiqueta_nif(self):
        s = 'EMPRESA EJEMPLO SL. NIF B12345678. CALLE FALSA 123'
        nombre, cif, direccion = parse_adj_dom_cif(s)
        assert nombre == 'EMPRESA EJEMPLO SL'
        assert cif == 'B12345678'
        assert 'CALLE FALSA' in direccion

    def test_formato_con_etiqueta_cif(self):
        s = 'ASOCIACION VECINOS. CIF G87654321. AV ANDALUCIA 5'
        nombre, cif, direccion = parse_adj_dom_cif(s)
        assert cif == 'G87654321'

    def test_errata_nnif(self):
        s = 'EMPRESA SL. NNIF B12345678. CALLE 1'
        nombre, cif, _ = parse_adj_dom_cif(s)
        assert cif == 'B12345678'

    def test_formato_con_guiones(self):
        s = 'ORGANISMO PUBLICO. CIF Q-2866001-G. CAMPUS UNIVERSITARIO'
        nombre, cif, _ = parse_adj_dom_cif(s)
        assert cif == 'Q2866001G'

    def test_nulo(self):
        nombre, cif, direccion = parse_adj_dom_cif(None)
        assert nombre == ''
        assert cif == ''
        assert direccion == ''

    def test_sin_cif(self):
        s = 'EMPRESA SIN CODIGO FISCAL'
        nombre, cif, _ = parse_adj_dom_cif(s)
        assert cif == ''


# ── find_header_row ──────────────────────────────────────────────────────────

class TestFindHeaderRow:

    def _make_df(self, rows):
        return pd.DataFrame(rows)

    def test_cabecera_en_fila_0(self):
        df = self._make_df([
            ['OBJETO DEL CONTRATO', 'ADJUDICATARIO', 'IMPORTE'],
            ['Limpieza', 'Empresa SL', '500'],
        ])
        assert find_header_row(df) == 0

    def test_cabecera_en_fila_3(self):
        df = self._make_df([
            ['Ayuntamiento de Rincón de la Victoria', None, None],
            ['Contratos Menores 2024', None, None],
            [None, None, None],
            ['OBJETO', 'ADJUDICATARIO', 'IMPORTE'],
            ['Obra menor', 'Empresa SA', '1000'],
        ])
        assert find_header_row(df) == 3

    def test_cabecera_con_adjudicacion(self):
        df = self._make_df([
            ['Título', None],
            ['OBJETO DEL CONTRATO', 'FECHA DE ADJUDICACIÓN'],
        ])
        assert find_header_row(df) == 1

    def test_sin_cabecera(self):
        df = self._make_df([
            ['Sin cabecera válida', 'Columna 2'],
            ['Dato 1', 'Dato 2'],
        ])
        assert find_header_row(df) is None
