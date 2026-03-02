# Contratos Menores · Rincón de la Victoria

Portal de transparencia para visualizar y analizar los contratos menores del Ayuntamiento de Rincón de la Victoria. Inspirado en el trabajo de [Jaime Gómez-Obregón](https://twitter.com/jaime_gomez) sobre transparencia pública.

Los datos se obtienen directamente del [portal de contratación municipal](https://www.rincondelavictoria.es/areas/contratacion/relaciones-de-contratos-menores), se procesan y normalizan, y se almacenan en una base de datos SQLite local.

## Arquitectura

```
rincon-contratos-menores/
├── data/
│   ├── raw/                  # Excel originales descargados (gitignored)
│   ├── processed/            # JSON intermedios del ETL (gitignored)
│   └── contratos.db          # Base de datos SQLite (gitignored)
├── scripts/
│   ├── monitor.py            # Detecta, descarga y procesa nuevos trimestres
│   ├── process_data.py       # ETL: limpieza, normalización, validación de CIFs
│   ├── init_db.py            # Inicializa el schema SQLite
│   ├── migrate_to_db.py      # Migración inicial desde JSON a SQLite
│   ├── download_data.py      # Descarga manual de XLS (uso puntual)
│   └── audit_data.py         # Auditoría de calidad de datos
├── app.py                    # Aplicación Streamlit (portal web)
├── db.py                     # Módulo de base de datos (schema + helpers)
└── .streamlit/config.toml    # Tema visual
```

## Stack

- **ETL**: Python 3.9 + pandas + xlrd/openpyxl
- **Base de datos**: SQLite (via `db.py`)
- **Web**: Streamlit + Plotly
- **Scraping**: requests + BeautifulSoup4

## Instalación

```bash
git clone https://github.com/sbellidov/rincon-contratos-menores.git
cd rincon-contratos-menores
python -m venv .venv
source .venv/bin/activate
pip install pandas xlrd openpyxl requests beautifulsoup4 streamlit plotly
```

## Primer uso (carga inicial de datos)

```bash
# 1. Inicializar la base de datos
python scripts/init_db.py

# 2. Descargar todos los Excel disponibles en el portal
python scripts/download_data.py

# 3. Procesar, normalizar y cargar en SQLite
python scripts/process_data.py

# 4. Lanzar el portal web
streamlit run app.py
```

## Actualización de datos

### Manual

```bash
python scripts/monitor.py
```

El monitor:
1. Consulta la DB para saber qué trimestres están procesados.
2. Raspa el portal del ayuntamiento buscando ficheros XLS nuevos.
3. Descarga los que faltan a `data/raw/`.
4. Relanza el ETL completo (idempotente: no genera duplicados).

Opciones disponibles:

```bash
python scripts/monitor.py --dry-run   # Muestra qué descargaría, sin actuar
python scripts/monitor.py --force     # Re-descarga aunque ya esté en DB
```

### Automática con cron

Para mantener el portal actualizado sin intervención manual, añade una tarea cron que ejecute el monitor periódicamente. Los contratos se publican trimestralmente (normalmente 1-2 meses después del fin de cada trimestre), así que con comprobarlo una vez por semana es más que suficiente:

```bash
# Abrir el editor de cron
crontab -e
```

```cron
# Comprobar nuevos contratos todos los lunes a las 9:00
0 9 * * 1 cd /ruta/al/proyecto && .venv/bin/python scripts/monitor.py >> logs/monitor.log 2>&1
```

Ajusta `/ruta/al/proyecto` a la ruta real del proyecto en tu máquina.

Si quieres que el log no crezca indefinidamente, añade también una rotación semanal:

```cron
# Rotar log del monitor cada domingo a las 23:59
59 23 * * 0 cp /ruta/al/proyecto/logs/monitor.log /ruta/al/proyecto/logs/monitor.log.bak && > /ruta/al/proyecto/logs/monitor.log
```

## Modelo de datos (SQLite)

| Tabla | Descripción |
|---|---|
| `contratos` | Tabla de hechos: un registro por contrato |
| `contratistas` | Dimensión: contratistas unificados por CIF |
| `areas` | Dimensión: áreas/departamentos municipales |
| `tipos_contrato` | Dimensión: Servicio / Suministro / Obras / Otros |
| `trimestres` | Tracking de descargas y procesados |

## Calidad de datos

Los Excel del ayuntamiento contienen errores frecuentes: CIFs inválidos, fechas mal formateadas, nombres del mismo contratista escritos de formas distintas, áreas con nombres inconsistentes, etc.

El ETL aplica correcciones automáticas (propagación de CIFs, modo estadístico para nombres canónicos), pero siempre quedará trabajo manual de depuración. La sección **Calidad de datos** de la app Streamlit muestra en todo momento los registros pendientes de revisión.

---

*Datos públicos obtenidos del portal de transparencia del Ayuntamiento de Rincón de la Victoria.*
