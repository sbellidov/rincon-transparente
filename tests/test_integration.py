"""
Tests de integración del pipeline ETL.

Genera un Excel sintético en memoria, lo escribe en data/raw/,
ejecuta process_data() y verifica que contracts.json y
contractors_summary.json contienen los registros esperados.
"""

import pytest
import json
import openpyxl


# ── Fixture: Excel sintético ─────────────────────────────────────────────────

FIXTURE_CONTRACTS = [
    # (fecha, adjudicatario, cif, objeto, area, tipo, importe, expediente)
    # Las áreas deben coincidir con claves del AREA_MAP en process_data.py
    ('15/01/2024', 'LIMPIEZA COSTA SL',    'B29123456', 'Servicio de limpieza viaria',    'Sostenibilidad Medioambiental', 'Contrato de Servicios', '5.000,00', 'EXP-001'),
    ('20/02/2024', 'OBRAS MUNICIPALES SA', 'A29654321', 'Reparación aceras calle Mayor',  'Obras, Infraestructuras y Servicios',  'Obras',      '8.500,00', 'EXP-002'),
    ('10/03/2024', 'AUTONOMO GARCIA',      '12345678Z', 'Asesoría técnica puntual',       'Presidencia',                  'Contrato de Servicios', '1.200,00', 'EXP-003'),
    ('05/04/2024', 'LIMPIEZA COSTA SL',    'B29123456', 'Suministro productos limpieza',  'Sostenibilidad Medioambiental', 'Suministro',            '2.300,00', 'EXP-004'),
]


def _create_fixture_excel(path):
    """Crea un .xlsx con formato 2023+: hoja AYTO con filas de título
    antes de la cabecera y columnas estándar. Usa openpyxl."""
    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = 'AYTO 1-24'

    rows = [
        # Filas de título (como en los Excel reales del ayuntamiento)
        ['Ayuntamiento de Rincón de la Victoria', '', '', '', '', '', '', '', ''],
        ['Relación de Contratos Menores 2024 T1', '', '', '', '', '', '', '', ''],
        ['', '', '', '', '', '', '', '', ''],
        # Cabecera (nombres exactos que usa el column_mapping del pipeline)
        ['FECHA DE ADJUDICACIÓN', 'ADJUDICATARIO', 'CIF/DOMICILIO', 'DOMICILIO',
         'OBJETO', 'ÁREA', 'CONTRATO', 'IMPORTE DEL CONTRATO', 'EXPTE.'],
    ]
    # Datos de contratos
    for fecha, adj, cif, objeto, area, tipo, importe, exp in FIXTURE_CONTRACTS:
        rows.append([fecha, adj, cif, 'Calle Falsa 123', objeto, area, tipo, importe, exp])

    for row in rows:
        ws.append(row)

    wb.save(path)


# ── Fixture pytest: entorno aislado ──────────────────────────────────────────

@pytest.fixture
def pipeline_env(tmp_path):
    """Crea un entorno temporal con raw/ y processed/ y un Excel de fixture."""
    raw_dir = tmp_path / 'data' / 'raw'
    processed_dir = tmp_path / 'data' / 'processed'
    raw_dir.mkdir(parents=True)
    processed_dir.mkdir(parents=True)

    excel_path = raw_dir / '2024_Q1.xlsx'
    _create_fixture_excel(str(excel_path))

    return tmp_path


# ── Tests de integración ──────────────────────────────────────────────────────

class TestPipelineIntegration:

    def test_pipeline_genera_contracts_json(self, pipeline_env):
        """El pipeline produce contracts.json con todos los registros del Excel."""
        from process_data import process_files
        process_files(
            raw_dir=str(pipeline_env / 'data' / 'raw'),
            processed_dir=str(pipeline_env / 'data' / 'processed'),
        )
        contracts_path = pipeline_env / 'data' / 'processed' / 'contracts.json'
        assert contracts_path.exists(), "contracts.json no fue generado"

        contracts = json.loads(contracts_path.read_text())
        assert len(contracts) == len(FIXTURE_CONTRACTS), (
            f"Se esperaban {len(FIXTURE_CONTRACTS)} contratos, se obtuvieron {len(contracts)}"
        )

    def test_pipeline_campos_obligatorios(self, pipeline_env):
        """Cada contrato tiene los campos obligatorios correctamente poblados."""
        from process_data import process_files
        process_files(
            raw_dir=str(pipeline_env / 'data' / 'raw'),
            processed_dir=str(pipeline_env / 'data' / 'processed'),
        )
        contracts = json.loads(
            (pipeline_env / 'data' / 'processed' / 'contracts.json').read_text()
        )
        required_fields = ['adjudicatario', 'cif', 'objeto', 'importe', 'area', 'tipo_contrato_limpio']
        for c in contracts:
            for field in required_fields:
                assert field in c, f"Campo '{field}' faltante en contrato: {c}"

    def test_pipeline_tipo_contrato_normalizado(self, pipeline_env):
        """Los tipos de contrato están normalizados a los valores canónicos."""
        from process_data import process_files
        process_files(
            raw_dir=str(pipeline_env / 'data' / 'raw'),
            processed_dir=str(pipeline_env / 'data' / 'processed'),
        )
        contracts = json.loads(
            (pipeline_env / 'data' / 'processed' / 'contracts.json').read_text()
        )
        valid_types = {'Servicio', 'Suministro', 'Obras', 'Otros'}
        for c in contracts:
            assert c['tipo_contrato_limpio'] in valid_types, (
                f"Tipo '{c['tipo_contrato_limpio']}' no es canónico"
            )

    def test_pipeline_tipo_entidad_inferido(self, pipeline_env):
        """El tipo de entidad se infiere correctamente del primer carácter del NIF."""
        from process_data import process_files
        process_files(
            raw_dir=str(pipeline_env / 'data' / 'raw'),
            processed_dir=str(pipeline_env / 'data' / 'processed'),
        )
        contracts = json.loads(
            (pipeline_env / 'data' / 'processed' / 'contracts.json').read_text()
        )
        by_cif = {c['cif']: c['tipo_entidad'] for c in contracts if c.get('cif')}
        assert by_cif.get('B29123456') == 'SL'
        assert by_cif.get('A29654321') == 'SA'
        assert by_cif.get('12345678Z') == 'Autónomo'

    def test_pipeline_importes_numericos(self, pipeline_env):
        """Los importes se convierten correctamente de formato europeo a float."""
        from process_data import process_files
        process_files(
            raw_dir=str(pipeline_env / 'data' / 'raw'),
            processed_dir=str(pipeline_env / 'data' / 'processed'),
        )
        contracts = json.loads(
            (pipeline_env / 'data' / 'processed' / 'contracts.json').read_text()
        )
        importes = sorted([c['importe'] for c in contracts])
        assert importes == pytest.approx(sorted([5000.0, 8500.0, 1200.0, 2300.0]))

    def test_pipeline_contractors_summary(self, pipeline_env):
        """contractors_summary.json agrupa correctamente los contratos por contratista."""
        from process_data import process_files
        process_files(
            raw_dir=str(pipeline_env / 'data' / 'raw'),
            processed_dir=str(pipeline_env / 'data' / 'processed'),
        )
        summary = json.loads(
            (pipeline_env / 'data' / 'processed' / 'contractors_summary.json').read_text()
        )
        # 3 contratistas únicos (LIMPIEZA aparece 2 veces)
        assert len(summary) == 3

        # LIMPIEZA COSTA SL tiene 2 contratos
        limpieza = next((c for c in summary if 'LIMPIEZA' in c.get('nombre', '')), None)
        assert limpieza is not None, "LIMPIEZA COSTA SL no aparece en el resumen"
        assert len(limpieza['contratos']) == 2
