import pandas as pd
import os
import json

def audit_data():
    processed_dir = 'data/processed'
    contracts_path = os.path.join(processed_dir, 'contracts.csv')
    
    if not os.path.exists(contracts_path):
        print(f"Error: {contracts_path} not found. Run process_data.py first.")
        return

    df = pd.read_csv(contracts_path)
    df['fecha_adjudicacion'] = pd.to_datetime(df['fecha_adjudicacion'], errors='coerce')

    anomalies = []

    def add_anomaly(row, tipo, detalle):
        anomalies.append({
            'source_file': row.get('source_file', 'unknown'),
            'expediente': row.get('expediente', 'N/A'),
            'adjudicatario': row['adjudicatario'],
            'tipo_anomalia': tipo,
            'detalle': detalle,
            'contenido_original': row.get('raw_row', '')
        })

    # 1. NIF Issues (Missing or Malformed)
    no_cif = df[df['cif'].isna() | (df['cif'] == '')]
    for _, row in no_cif.iterrows():
        add_anomaly(row, 'NIF Faltante', 'No se pudo encontrar ningún código de identificación')

    invalid_cif = df[(~df['cif'].isna()) & (df['cif'] != '') & (df['check_cif'] == False)]
    for _, row in invalid_cif.iterrows():
        add_anomaly(row, 'NIF Mal Formulado', f"El código '{row['cif']}' no pasa el algoritmo de validación")

    # 2. Outlier Dates
    missing_dates = df[df['fecha_adjudicacion'].isna()]
    for _, row in missing_dates.iterrows():
        add_anomaly(row, 'Fecha Inválida/Faltante', 'La fecha es nula o estaba fuera del rango 2020-2026')

    # 3. Anomalous Amounts
    invalid_format = df[df['importe'].isna()]
    for _, row in invalid_format.iterrows():
        add_anomaly(row, 'Importe con Formato Inválido',
                    "El importe tenía múltiples puntos con grupo final ≥ 4 dígitos "
                    "(ambiguo) y fue nullificado")

    # Obras > 50.000 € (umbral LCSP art. 118; importes con IVA incluido)
    obras_altas = df[
        df['importe'].notna() &
        (df['tipo_contrato_limpio'] == 'Obras') &
        (df['importe'] > 50000)
    ]
    for _, row in obras_altas.iterrows():
        add_anomaly(row, 'Obra Importe >50K€',
                    f"Contrato de obra de {row['importe']}€ supera el umbral de 50.000€ de contratos menores")

    # Servicios/suministros/otros > 15.000 € (umbral LCSP art. 118; importes con IVA incluido)
    otros_altos = df[
        df['importe'].notna() &
        (df['tipo_contrato_limpio'] != 'Obras') &
        (df['importe'] > 15000)
    ]
    for _, row in otros_altos.iterrows():
        add_anomaly(row, 'Importe >15K€',
                    f"Contrato de {row['importe']}€ supera el umbral de 15.000€ de contratos menores")

    zero_amounts = df[df['importe'].notna() & (df['importe'] <= 0)]
    for _, row in zero_amounts.iterrows():
        add_anomaly(row, 'Importe Cero/Negativo', "El importe registrado es 0 o menor")

    # 3b. Adjudicatario en blanco (el objeto existía pero falta el nombre)
    missing_adj = df[df['adjudicatario'].isna() | (df['adjudicatario'] == '')]
    for _, row in missing_adj.iterrows():
        add_anomaly(row, 'Adjudicatario Faltante', "El nombre del adjudicatario está en blanco")

    # 4. Missing Object
    missing_object = df[df['objeto'].isna() | (df['objeto'] == '')]
    for _, row in missing_object.iterrows():
        add_anomaly(row, 'Objeto Faltante', "No hay descripción del objeto del contrato")

    # 5. Duplicate Expediente (D7)
    if 'expediente' in df.columns:
        exp_not_null = df[df['expediente'].notna() & (df['expediente'] != '')]
        dup_mask = exp_not_null.duplicated(subset=['source_file', 'expediente'], keep=False)
        for _, row in exp_not_null[dup_mask].iterrows():
            add_anomaly(row, 'Expediente Duplicado',
                        f"El expediente '{row['expediente']}' aparece más de una vez en {row['source_file']}")

    # Generate Report
    audit_df = pd.DataFrame(anomalies)
    output_path = os.path.join(processed_dir, 'audit_report.csv')
    
    if not audit_df.empty:
        cols = ['source_file', 'expediente', 'adjudicatario', 'tipo_anomalia', 'detalle', 'contenido_original']
        audit_df = audit_df[cols]
    
    audit_df.to_csv(output_path, index=False)

    print(f"Audit complete.")
    print(f"  - Total records checked: {len(df)}")
    print(f"  - Anomalies found: {len(audit_df)}")
    print(f"  - Report saved to: {output_path}")

    # Summary Statistics
    df['_sin_nif']          = df['cif'].isna() | (df['cif'] == '')
    df['_nif_mal_formulado'] = (~df['cif'].isna()) & (df['cif'] != '') & (df['check_cif'] == False)
    df['_sin_fecha']         = df['fecha_adjudicacion'].isna()
    df['_imp_cero']          = df['importe'].notna() & (df['importe'] <= 0)
    df['_imp_alto_obra']     = df['importe'].notna() & (df['tipo_contrato_limpio'] == 'Obras') & (df['importe'] > 50000)
    df['_imp_alto_otros']    = df['importe'].notna() & (df['tipo_contrato_limpio'] != 'Obras') & (df['importe'] > 15000)
    df['_sin_adj']           = df['adjudicatario'].isna() | (df['adjudicatario'] == '')
    # D7: marcar filas con expediente duplicado dentro del mismo fichero
    if 'expediente' in df.columns:
        exp_not_null = df['expediente'].notna() & (df['expediente'] != '')
        df['_exp_dup'] = exp_not_null & df.duplicated(subset=['source_file', 'expediente'], keep=False)
    else:
        df['_exp_dup'] = False

    by_year = (
        df.groupby('year')
        .agg(
            total=('importe', 'count'),
            importe_total=('importe', 'sum'),
            sin_nif=('_sin_nif', 'sum'),
            nif_mal_formulado=('_nif_mal_formulado', 'sum'),
            sin_fecha=('_sin_fecha', 'sum'),
            importe_cero=('_imp_cero', 'sum'),
            importe_alto_obra=('_imp_alto_obra', 'sum'),
            importe_alto_otros=('_imp_alto_otros', 'sum'),
            exp_duplicado=('_exp_dup', 'sum'),
            sin_adjudicatario=('_sin_adj', 'sum'),
        )
        .reset_index()
        .sort_values('year')
        .assign(year=lambda d: d['year'].astype(int))
        .assign(importe_total=lambda d: d['importe_total'].round(2))
    )
    for col in ['sin_nif', 'nif_mal_formulado', 'sin_fecha', 'importe_cero', 'importe_alto_obra', 'importe_alto_otros', 'exp_duplicado', 'sin_adjudicatario']:
        by_year[col] = by_year[col].astype(int)

    summary = {
        'total_checked': len(df),
        'total_anomalies': len(audit_df),
        'by_type': audit_df['tipo_anomalia'].value_counts().to_dict() if not audit_df.empty else {},
        'by_year': by_year.to_dict('records'),
    }
    
    with open(os.path.join(processed_dir, 'audit_summary.json'), 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

if __name__ == "__main__":
    audit_data()

