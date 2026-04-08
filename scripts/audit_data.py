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
    high_amounts = df[df['importe'] > 50000]
    for _, row in high_amounts.iterrows():
        add_anomaly(row, 'Importe Elevado', f"Importe de {row['importe']}€ supera el umbral común de contratos menores")

    zero_amounts = df[df['importe'] <= 0]
    for _, row in zero_amounts.iterrows():
        add_anomaly(row, 'Importe Cero/Negativo', "El importe registrado es 0 o menor")

    # 4. Missing Object
    missing_object = df[df['objeto'].isna() | (df['objeto'] == '')]
    for _, row in missing_object.iterrows():
        add_anomaly(row, 'Objeto Faltante', "No hay descripción del objeto del contrato")

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
    df['_imp_cero']          = df['importe'] <= 0
    df['_imp_alto']          = df['importe'] > 50000

    by_year = (
        df.groupby('year')
        .agg(
            total=('importe', 'count'),
            importe_total=('importe', 'sum'),
            sin_nif=('_sin_nif', 'sum'),
            nif_mal_formulado=('_nif_mal_formulado', 'sum'),
            sin_fecha=('_sin_fecha', 'sum'),
            importe_cero=('_imp_cero', 'sum'),
            importe_alto=('_imp_alto', 'sum'),
        )
        .reset_index()
        .sort_values('year')
        .assign(year=lambda d: d['year'].astype(int))
        .assign(importe_total=lambda d: d['importe_total'].round(2))
    )
    for col in ['sin_nif', 'nif_mal_formulado', 'sin_fecha', 'importe_cero', 'importe_alto']:
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

