import pandas as pd
import json
import os

def analyze():
    df = pd.read_csv('data/processed/contracts.csv')
    df['fecha_adjudicacion'] = pd.to_datetime(df['fecha_adjudicacion'])
    
    # Use unificated name if possible
    if 'adjudicatario_unificado' in df.columns:
        df['adjudicatario_final'] = df['adjudicatario_unificado'].fillna(df['adjudicatario'])
    else:
        df['adjudicatario_final'] = df['adjudicatario']
    
    valid_dates = df['fecha_adjudicacion'].dropna()
    max_date = valid_dates.max() if not valid_dates.empty else None

    analysis = {
        'summary': {
            'total_contracts': len(df),
            'total_amount': df['importe'].sum(),
            'avg_amount': df['importe'].mean(),
            'min_date': valid_dates.min().isoformat() if not valid_dates.empty else None,
            'max_date': max_date.isoformat() if max_date is not None else None,
            'last_updated': max_date.strftime('%d/%m/%Y') if max_date is not None else None,
        },
        'by_area': df.groupby('area')['importe'].agg(['sum', 'count']).sort_values('sum', ascending=False).head(15).to_dict('index'),
        'top_adjudicatarios': df.groupby('adjudicatario_final')['importe'].agg(['sum', 'count']).sort_values('sum', ascending=False).head(15).to_dict('index'),
        'by_year': df.groupby('year')['importe'].agg(['sum', 'count']).to_dict('index'),
        'by_quarter': (
            df.groupby(['year', 'quarter'])['importe']
            .agg(['sum', 'count'])
            .reset_index()
            .sort_values(['year', 'quarter'])
            .assign(periodo=lambda d: d['year'].astype(int).astype(str) + ' T' + d['quarter'].astype(int).astype(str))
            .rename(columns={'sum': 'importe', 'count': 'contratos'})
            [['year', 'quarter', 'periodo', 'importe', 'contratos']]
            .to_dict('records')
        ),
        'by_type': df.groupby('tipo_contrato_limpio')['importe'].agg(['sum', 'count']).to_dict('index')
    }

    # Deep clean of the analysis dictionary to replace any NaN with None
    def clean_nan(obj):
        if isinstance(obj, dict):
            return {k: clean_nan(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [clean_nan(i) for i in obj]
        elif isinstance(obj, float) and (obj != obj): # check for NaN
            return None
        return obj

    analysis = clean_nan(analysis)

    output_path = 'data/processed/analysis.json'
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(analysis, f, indent=2, ensure_ascii=False)
    
    print(f"Analysis complete. Saved to {output_path}")

if __name__ == "__main__":
    analyze()
