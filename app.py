"""
app.py — Portal de Transparencia: Contratos Menores de Rincón de la Victoria

Streamlit app que consulta directamente data/contratos.db (SQLite).

Uso:
    streamlit run app.py
"""

import os
import sys

import pandas as pd
import plotly.express as px
import streamlit as st

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from db import get_connection

# ── Configuración de página ───────────────────────────────────────────────────
st.set_page_config(
    page_title="Contratos Menores · Rincón de la Victoria",
    page_icon="📋",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── CSS personalizado ─────────────────────────────────────────────────────────
st.markdown("""
<style>
/* Cabecera de página reducida */
[data-testid="stHeader"] { height: 0; }

/* Badges de tipo de entidad */
.badge {
    display: inline-block;
    padding: 2px 10px;
    border-radius: 99px;
    font-size: 0.72rem;
    font-weight: 600;
    white-space: nowrap;
}
.badge-sl       { background:#1e3a5f; color:#38bdf8; }
.badge-sa       { background:#14532d; color:#4ade80; }
.badge-autonomo { background:#431407; color:#fb923c; }
.badge-admin    { background:#312e81; color:#818cf8; }
.badge-otros    { background:#1f2937; color:#9ca3af; }

/* Alerta de calidad */
.quality-ok  { color: #4ade80; font-weight: 600; }
.quality-bad { color: #f87171; font-weight: 600; }
</style>
""", unsafe_allow_html=True)

# ── Helpers de formato ────────────────────────────────────────────────────────
def fmt_eur(v) -> str:
    try:
        s = f"{float(v):,.2f} €"
        return s.replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return "—"


def badge_entidad(tipo: str) -> str:
    css = {
        "SL": "sl", "SA": "sa", "Autónomo": "autonomo",
        "Adm. Pública": "admin",
    }.get(tipo or "", "otros")
    return f'<span class="badge badge-{css}">{tipo or "?"}</span>'


# ── Estilo de gráficos Plotly ─────────────────────────────────────────────────
_PLOT = dict(
    paper_bgcolor="rgba(0,0,0,0)",
    plot_bgcolor="rgba(0,0,0,0)",
    font_color="#f8fafc",
    font_size=12,
    margin=dict(l=0, r=10, t=30, b=0),
)
ACCENT = "#38bdf8"
GRID   = "#1e293b"


# ── Capa de acceso a datos ────────────────────────────────────────────────────
@st.cache_data(ttl=300)
def _sql(sql: str, params: tuple = ()) -> pd.DataFrame:
    conn = get_connection()
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df


def stats_globales() -> dict:
    row = _sql("""
        SELECT
            COUNT(*)                           AS total_contratos,
            ROUND(SUM(importe), 2)             AS total_importe,
            ROUND(AVG(importe), 2)             AS media_importe,
            COUNT(DISTINCT cif)                AS total_contratistas,
            MIN(fecha_adjudicacion)            AS fecha_min,
            MAX(fecha_adjudicacion)            AS fecha_max
        FROM contratos
    """).iloc[0]
    return row.to_dict()


def datos_por_trimestre() -> pd.DataFrame:
    df = _sql("""
        SELECT year, quarter,
               ROUND(SUM(importe), 2) AS importe,
               COUNT(*)               AS contratos
        FROM contratos
        GROUP BY year, quarter
        ORDER BY year, quarter
    """)
    df["periodo"] = df.apply(lambda r: f"{int(r.year)} T{int(r.quarter)}", axis=1)
    return df


def datos_por_area() -> pd.DataFrame:
    return _sql("""
        SELECT a.nombre                    AS area,
               ROUND(SUM(c.importe), 2)   AS importe,
               COUNT(*)                   AS contratos
        FROM contratos c
        JOIN areas a ON a.id = c.area_id
        GROUP BY a.nombre
        ORDER BY importe DESC
    """)


def datos_por_tipo() -> pd.DataFrame:
    return _sql("""
        SELECT tc.nombre                   AS tipo,
               ROUND(SUM(c.importe), 2)   AS importe,
               COUNT(*)                   AS contratos
        FROM contratos c
        JOIN tipos_contrato tc ON tc.id = c.tipo_contrato_id
        GROUP BY tc.nombre
        ORDER BY importe DESC
    """)


def lista_areas() -> list:
    return _sql("SELECT nombre FROM areas ORDER BY nombre")["nombre"].tolist()


def lista_tipos() -> list:
    return _sql("SELECT nombre FROM tipos_contrato ORDER BY nombre")["nombre"].tolist()


def lista_anios() -> list:
    return _sql("SELECT DISTINCT year FROM contratos ORDER BY year")["year"].astype(int).tolist()


def contratos_filtrados(
    anio=None, trimestre=None, area=None, tipo=None, busqueda=None
) -> pd.DataFrame:
    where, params = ["1=1"], []
    if anio:
        where.append("c.year = ?");  params.append(int(anio))
    if trimestre:
        where.append("c.quarter = ?");  params.append(int(trimestre))
    if area:
        where.append("a.nombre = ?");  params.append(area)
    if tipo:
        where.append("tc.nombre = ?");  params.append(tipo)
    if busqueda:
        where.append("(c.objeto LIKE ? OR COALESCE(cnt.nombre,'') LIKE ? OR c.cif LIKE ? OR c.expediente LIKE ?)")
        s = f"%{busqueda}%"
        params.extend([s, s, s, s])
    return _sql(f"""
        SELECT
            c.fecha_adjudicacion            AS fecha,
            COALESCE(cnt.nombre, c.cif, '') AS contratista,
            c.cif,
            cnt.tipo_entidad,
            c.objeto,
            a.nombre                        AS area,
            tc.nombre                       AS tipo,
            c.importe,
            c.expediente
        FROM contratos c
        LEFT JOIN contratistas cnt ON cnt.cif = c.cif
        LEFT JOIN areas a          ON a.id = c.area_id
        LEFT JOIN tipos_contrato tc ON tc.id = c.tipo_contrato_id
        WHERE {' AND '.join(where)}
        ORDER BY c.fecha_adjudicacion DESC
    """, tuple(params))


def datos_por_tipo_entidad() -> pd.DataFrame:
    return _sql("""
        SELECT
            COALESCE(cnt.tipo_entidad, 'Desconocido')  AS tipo_entidad,
            COUNT(DISTINCT cnt.cif)                    AS contratistas,
            COUNT(c.contrato_id)                       AS contratos,
            ROUND(SUM(c.importe), 2)                   AS importe
        FROM contratistas cnt
        JOIN contratos c ON c.cif = cnt.cif
        GROUP BY cnt.tipo_entidad
        ORDER BY importe DESC
    """)


def ranking_contratistas(tipo_entidad=None) -> pd.DataFrame:
    where = "1=1"
    params: tuple = ()
    if tipo_entidad:
        where = "cnt.tipo_entidad = ?"
        params = (tipo_entidad,)
    return _sql(f"""
        SELECT
            cnt.cif,
            COALESCE(cnt.nombre, cnt.cif)  AS nombre,
            cnt.tipo_entidad,
            cnt.direccion,
            COUNT(c.contrato_id)           AS contratos,
            ROUND(SUM(c.importe), 2)       AS total_importe,
            MIN(c.fecha_adjudicacion)      AS primer_contrato,
            MAX(c.fecha_adjudicacion)      AS ultimo_contrato
        FROM contratistas cnt
        JOIN contratos c ON c.cif = cnt.cif
        WHERE {where}
        GROUP BY cnt.cif
        ORDER BY total_importe DESC
    """, params)


def contratos_de_contratista(cif: str) -> pd.DataFrame:
    return _sql("""
        SELECT
            c.fecha_adjudicacion  AS fecha,
            c.objeto,
            tc.nombre             AS tipo,
            a.nombre              AS area,
            c.importe,
            c.expediente
        FROM contratos c
        LEFT JOIN tipos_contrato tc ON tc.id = c.tipo_contrato_id
        LEFT JOIN areas a           ON a.id  = c.area_id
        WHERE c.cif = ?
        ORDER BY c.fecha_adjudicacion DESC
    """, (cif,))


def issues_calidad() -> dict:
    conn = get_connection()
    r = {
        "sin_fecha":       conn.execute("SELECT COUNT(*) FROM contratos WHERE fecha_adjudicacion IS NULL").fetchone()[0],
        "sin_cif":         conn.execute("SELECT COUNT(*) FROM contratos WHERE cif IS NULL").fetchone()[0],
        "importe_cero":    conn.execute("SELECT COUNT(*) FROM contratos WHERE importe = 0").fetchone()[0],
        "importe_alto":    conn.execute("SELECT COUNT(*) FROM contratos WHERE importe > 50000").fetchone()[0],
        "sin_expediente":  conn.execute("SELECT COUNT(*) FROM contratos WHERE expediente = '' OR expediente IS NULL").fetchone()[0],
        "sin_objeto":      conn.execute("SELECT COUNT(*) FROM contratos WHERE objeto = '' OR objeto IS NULL").fetchone()[0],
    }
    conn.close()
    return r


def contratos_sin_fecha() -> pd.DataFrame:
    return _sql("""
        SELECT COALESCE(cnt.nombre, c.cif, '?') AS contratista,
               c.cif, c.objeto, c.importe, c.source_file
        FROM contratos c
        LEFT JOIN contratistas cnt ON cnt.cif = c.cif
        WHERE c.fecha_adjudicacion IS NULL
        ORDER BY c.importe DESC
    """)


def contratos_sin_cif() -> pd.DataFrame:
    return _sql("""
        SELECT c.fecha_adjudicacion AS fecha,
               c.objeto, c.importe, c.expediente, c.source_file
        FROM contratos c
        WHERE c.cif IS NULL
        ORDER BY c.fecha_adjudicacion DESC
    """)


def calidad_por_anio() -> pd.DataFrame:
    return _sql("""
        SELECT
            year                                                        AS año,
            COUNT(*)                                                    AS total,
            SUM(CASE WHEN cif IS NULL OR cif = '' THEN 1 ELSE 0 END)   AS sin_cif,
            SUM(CASE WHEN fecha_adjudicacion IS NULL THEN 1 ELSE 0 END) AS sin_fecha,
            SUM(CASE WHEN area_id IS NULL THEN 1 ELSE 0 END)            AS sin_area,
            SUM(CASE WHEN importe = 0 THEN 1 ELSE 0 END)                AS importe_cero,
            ROUND(SUM(importe), 2)                                      AS importe_total
        FROM contratos
        GROUP BY year
        ORDER BY year
    """)


def contratos_importe_alto() -> pd.DataFrame:
    return _sql("""
        SELECT c.fecha_adjudicacion               AS fecha,
               COALESCE(cnt.nombre, c.cif, '?')  AS contratista,
               c.cif, c.objeto, c.importe, c.expediente
        FROM contratos c
        LEFT JOIN contratistas cnt ON cnt.cif = c.cif
        WHERE c.importe > 50000
        ORDER BY c.importe DESC
    """)


# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## 📋 Contratos Menores")
    st.caption("Ayuntamiento de Rincón de la Victoria")
    st.divider()

    pagina = st.radio(
        "Sección",
        ["📊 Dashboard", "📋 Contratos", "🏢 Contratistas", "🗂️ Por área", "⚠️ Calidad de datos"],
        label_visibility="collapsed",
    )

    st.divider()
    s = stats_globales()
    st.caption(f"**{int(s['total_contratos']):,}** contratos")
    st.caption(f"**{int(s['total_contratistas']):,}** contratistas")
    st.caption(f"Inversión: **{fmt_eur(s['total_importe'])}**")
    if s["fecha_min"] and s["fecha_max"]:
        st.caption(f"{str(s['fecha_min'])[:7]} → {str(s['fecha_max'])[:7]}")


# ══════════════════════════════════════════════════════════════════════════════
# 📊  DASHBOARD
# ══════════════════════════════════════════════════════════════════════════════
if pagina == "📊 Dashboard":
    st.title("📋 Contratos Menores · Rincón de la Victoria")
    st.caption("Transparencia en la contratación municipal pública")
    st.divider()

    # KPIs
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total contratos",      f"{int(s['total_contratos']):,}")
    c2.metric("Inversión total",      fmt_eur(s["total_importe"]))
    c3.metric("Media por contrato",   fmt_eur(s["media_importe"]))
    c4.metric("Contratistas únicos",  f"{int(s['total_contratistas']):,}")

    st.divider()

    # Evolución trimestral + Tipología
    col_evol, col_tipo = st.columns([3, 2])

    with col_evol:
        st.subheader("Evolución trimestral")
        df_q = datos_por_trimestre()
        fig = px.bar(
            df_q, x="periodo", y="importe",
            color_discrete_sequence=[ACCENT],
            hover_data={"contratos": True, "importe": ":.2f"},
        )
        fig.update_layout(**_PLOT, xaxis_title="", yaxis_title="Importe (€)")
        fig.update_yaxes(showgrid=True, gridcolor=GRID)
        fig.update_xaxes(showgrid=False)
        st.plotly_chart(fig, use_container_width=True)

    with col_tipo:
        st.subheader("Por tipo de contrato")
        df_t = datos_por_tipo()
        fig2 = px.bar(
            df_t, x="importe", y="tipo", orientation="h",
            color_discrete_sequence=[ACCENT],
            text=df_t["importe"].apply(fmt_eur),
        )
        fig2.update_traces(textposition="outside", textfont_size=10)
        fig2.update_layout(**_PLOT, xaxis_title="€", yaxis_title="")
        fig2.update_xaxes(showgrid=True, gridcolor=GRID)
        fig2.update_yaxes(showgrid=False)
        st.plotly_chart(fig2, use_container_width=True)

    # Top áreas
    st.subheader("Top 15 áreas por inversión")
    df_a = datos_por_area().head(15)
    fig3 = px.bar(
        df_a, x="importe", y="area", orientation="h",
        color_discrete_sequence=[ACCENT],
        text=df_a["importe"].apply(fmt_eur),
        hover_data={"contratos": True},
    )
    fig3.update_traces(textposition="outside", textfont_size=10)
    fig3.update_layout(**_PLOT, height=480, xaxis_title="€", yaxis_title="")
    fig3.update_yaxes(categoryorder="total ascending", showgrid=False)
    fig3.update_xaxes(showgrid=True, gridcolor=GRID)
    st.plotly_chart(fig3, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# 📋  CONTRATOS
# ══════════════════════════════════════════════════════════════════════════════
elif pagina == "📋 Contratos":
    st.title("📋 Contratos")

    # Filtros
    with st.expander("🔎 Filtros", expanded=True):
        f1, f2, f3, f4 = st.columns(4)
        anios    = [None] + lista_anios()
        areas    = [None] + lista_areas()
        tipos    = [None] + lista_tipos()

        sel_anio  = f1.selectbox("Año",        anios,  format_func=lambda x: "Todos" if x is None else str(x))
        sel_trim  = f2.selectbox("Trimestre",  [None, 1, 2, 3, 4], format_func=lambda x: "Todos" if x is None else f"T{x}")
        sel_area  = f3.selectbox("Área",       areas,  format_func=lambda x: "Todas" if x is None else x)
        sel_tipo  = f4.selectbox("Tipo",       tipos,  format_func=lambda x: "Todos" if x is None else x)
        busqueda  = st.text_input("Búsqueda libre (contratista, objeto, CIF, expediente)", placeholder="Ej: sonido, B12345678, 2024/1234 …")

    df = contratos_filtrados(sel_anio, sel_trim, sel_area, sel_tipo, busqueda or None)

    # Resultados
    st.caption(f"**{len(df):,}** contratos encontrados  ·  Total: **{fmt_eur(df['importe'].sum())}**")

    # Descarga CSV
    csv = df.to_csv(index=False).encode("utf-8")
    st.download_button("⬇️ Descargar CSV", csv, "contratos.csv", "text/csv", use_container_width=False)

    # Tabla
    st.dataframe(
        df.rename(columns={
            "fecha": "Fecha", "contratista": "Contratista", "cif": "CIF",
            "tipo_entidad": "Entidad", "objeto": "Objeto", "area": "Área",
            "tipo": "Tipo", "importe": "Importe (€)", "expediente": "Expediente",
        }),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Importe (€)": st.column_config.NumberColumn(format="%.2f €"),
            "Fecha": st.column_config.TextColumn(width="small"),
            "CIF": st.column_config.TextColumn(width="small"),
            "Entidad": st.column_config.TextColumn(width="small"),
            "Tipo": st.column_config.TextColumn(width="small"),
            "Expediente": st.column_config.TextColumn(width="small"),
        },
    )


# ══════════════════════════════════════════════════════════════════════════════
# 🏢  CONTRATISTAS
# ══════════════════════════════════════════════════════════════════════════════
elif pagina == "🏢 Contratistas":
    st.title("🏢 Ranking de contratistas")

    # Gráfico de distribución por tipo de entidad
    df_te = datos_por_tipo_entidad()
    col_g1, col_g2 = st.columns(2)

    with col_g1:
        st.subheader("Inversión por tipo de entidad")
        fig_te1 = px.bar(
            df_te, x="importe", y="tipo_entidad", orientation="h",
            color_discrete_sequence=[ACCENT],
            text=df_te["importe"].apply(fmt_eur),
        )
        fig_te1.update_traces(textposition="outside", textfont_size=10)
        fig_te1.update_layout(**_PLOT, xaxis_title="€", yaxis_title="")
        fig_te1.update_yaxes(categoryorder="total ascending", showgrid=False)
        fig_te1.update_xaxes(showgrid=True, gridcolor=GRID)
        st.plotly_chart(fig_te1, use_container_width=True)

    with col_g2:
        st.subheader("Contratistas y contratos por tipo")
        fig_te2 = px.bar(
            df_te.melt(id_vars="tipo_entidad", value_vars=["contratistas", "contratos"],
                       var_name="métrica", value_name="valor"),
            x="valor", y="tipo_entidad", color="métrica", orientation="h",
            barmode="group",
            color_discrete_map={"contratistas": ACCENT, "contratos": "#818cf8"},
        )
        fig_te2.update_layout(**_PLOT, xaxis_title="Unidades", yaxis_title="")
        fig_te2.update_yaxes(categoryorder="total ascending", showgrid=False)
        fig_te2.update_xaxes(showgrid=True, gridcolor=GRID)
        st.plotly_chart(fig_te2, use_container_width=True)

    st.divider()

    tipos_entidad = [None, "Autónomo", "SL", "SA", "Adm. Pública", "Asociación",
                     "Cooperativa", "Empresa/Otros", "UTE", "Sociedad Civil"]
    sel_tipo_e = st.selectbox(
        "Filtrar por tipo de entidad",
        tipos_entidad,
        format_func=lambda x: "Todos" if x is None else x,
    )

    df_r = ranking_contratistas(sel_tipo_e)
    st.caption(f"**{len(df_r):,}** contratistas  ·  Inversión total: **{fmt_eur(df_r['total_importe'].sum())}**")

    # Vista rápida: tabla resumen
    df_display = df_r[["nombre", "tipo_entidad", "contratos", "total_importe",
                        "primer_contrato", "ultimo_contrato"]].copy()
    df_display.index = range(1, len(df_display) + 1)

    st.dataframe(
        df_display.rename(columns={
            "nombre": "Contratista", "tipo_entidad": "Entidad",
            "contratos": "Nº contratos", "total_importe": "Total (€)",
            "primer_contrato": "Primer contrato", "ultimo_contrato": "Último contrato",
        }),
        use_container_width=True,
        column_config={
            "Total (€)": st.column_config.NumberColumn(format="%.2f €"),
            "Nº contratos": st.column_config.NumberColumn(width="small"),
        },
    )

    st.divider()
    st.subheader("Detalle por contratista")
    st.caption("Selecciona un contratista para ver todos sus contratos.")

    nombres = df_r["nombre"].tolist()
    cifs    = df_r["cif"].tolist()
    opciones = {f"{n}  ({c})": c for n, c in zip(nombres, cifs)}

    seleccion = st.selectbox("Contratista", list(opciones.keys()))
    if seleccion:
        cif_sel = opciones[seleccion]
        fila = df_r[df_r["cif"] == cif_sel].iloc[0]

        col_a, col_b, col_c = st.columns(3)
        col_a.metric("Total invertido",  fmt_eur(fila["total_importe"]))
        col_b.metric("Nº contratos",     int(fila["contratos"]))
        col_c.metric("Tipo de entidad",  fila["tipo_entidad"] or "—")
        if fila["direccion"]:
            st.caption(f"📍 {fila['direccion']}")

        df_c = contratos_de_contratista(cif_sel)
        st.dataframe(
            df_c.rename(columns={
                "fecha": "Fecha", "objeto": "Objeto", "tipo": "Tipo",
                "area": "Área", "importe": "Importe (€)", "expediente": "Expediente",
            }),
            use_container_width=True,
            hide_index=True,
            column_config={"Importe (€)": st.column_config.NumberColumn(format="%.2f €")},
        )

        # Gráfico de evolución del contratista
        if not df_c.empty and df_c["fecha"].notna().any():
            df_c["fecha"] = pd.to_datetime(df_c["fecha"], errors="coerce")
            df_mes = df_c.dropna(subset=["fecha"]).copy()
            df_mes["mes"] = df_mes["fecha"].dt.to_period("Q").dt.to_timestamp()
            df_mes = df_mes.groupby("mes", as_index=False).agg(importe=("importe", "sum"), contratos=("importe", "count"))
            if len(df_mes) > 1:
                fig = px.bar(df_mes, x="mes", y="importe",
                             color_discrete_sequence=[ACCENT],
                             title="Evolución trimestral del contratista")
                fig.update_layout(**_PLOT, xaxis_title="", yaxis_title="€")
                st.plotly_chart(fig, use_container_width=True)


# ══════════════════════════════════════════════════════════════════════════════
# 🗂️  POR ÁREA
# ══════════════════════════════════════════════════════════════════════════════
elif pagina == "🗂️ Por área":
    st.title("🗂️ Gasto por área municipal")

    df_a = datos_por_area()

    # Gráfico completo (barras horizontales)
    fig = px.bar(
        df_a, x="importe", y="area", orientation="h",
        color_discrete_sequence=[ACCENT],
        text=df_a["importe"].apply(fmt_eur),
        hover_data={"contratos": True},
    )
    fig.update_traces(textposition="outside", textfont_size=9)
    fig.update_layout(**_PLOT, height=max(400, len(df_a) * 22),
                      xaxis_title="€", yaxis_title="")
    fig.update_yaxes(categoryorder="total ascending", showgrid=False, tickfont_size=10)
    fig.update_xaxes(showgrid=True, gridcolor=GRID)
    st.plotly_chart(fig, use_container_width=True)

    # Tabla detallada
    st.subheader("Tabla detallada")
    df_a.index = range(1, len(df_a) + 1)
    st.dataframe(
        df_a.rename(columns={"area": "Área", "importe": "Inversión (€)", "contratos": "Nº contratos"}),
        use_container_width=True,
        column_config={"Inversión (€)": st.column_config.NumberColumn(format="%.2f €")},
    )


# ══════════════════════════════════════════════════════════════════════════════
# ⚠️  CALIDAD DE DATOS
# ══════════════════════════════════════════════════════════════════════════════
elif pagina == "⚠️ Calidad de datos":
    st.title("⚠️ Calidad de datos")
    st.caption("Contratos e identificadores que requieren revisión manual.")

    issues = issues_calidad()
    total  = int(s["total_contratos"])

    def pct(n): return f"{n/total*100:.1f}%"
    def ok_or_bad(n): return "quality-ok" if n == 0 else "quality-bad"

    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Sin fecha de adjudicación", issues["sin_fecha"],
                  delta=f"{pct(issues['sin_fecha'])} del total", delta_color="inverse")
        st.metric("Sin CIF / identificador",   issues["sin_cif"],
                  delta=f"{pct(issues['sin_cif'])} del total", delta_color="inverse")
    with c2:
        st.metric("Importe = 0 €",             issues["importe_cero"],
                  delta=f"{pct(issues['importe_cero'])} del total", delta_color="inverse")
        st.metric("Importe > 50.000 €",        issues["importe_alto"],
                  delta=f"{pct(issues['importe_alto'])} del total", delta_color="inverse")
    with c3:
        st.metric("Sin expediente",            issues["sin_expediente"],
                  delta=f"{pct(issues['sin_expediente'])} del total", delta_color="inverse")
        st.metric("Sin objeto / descripción",  issues["sin_objeto"],
                  delta=f"{pct(issues['sin_objeto'])} del total", delta_color="inverse")

    st.divider()
    st.subheader("Resumen por año")

    df_qa = calidad_por_anio()
    # Columnas de porcentaje calculadas
    df_qa["% sin CIF"]   = (df_qa["sin_cif"]   / df_qa["total"] * 100).round(1)
    df_qa["% sin fecha"] = (df_qa["sin_fecha"]  / df_qa["total"] * 100).round(1)
    df_qa["% sin área"]  = (df_qa["sin_area"]   / df_qa["total"] * 100).round(1)

    st.dataframe(
        df_qa.rename(columns={
            "año": "Año", "total": "Contratos", "importe_total": "Inversión (€)",
            "sin_cif": "Sin CIF", "sin_fecha": "Sin fecha", "sin_area": "Sin área",
            "importe_cero": "Importe = 0 €",
        }),
        use_container_width=True,
        hide_index=True,
        column_config={
            "Año":          st.column_config.NumberColumn(width="small", format="%d"),
            "Contratos":    st.column_config.NumberColumn(width="small"),
            "Inversión (€)": st.column_config.NumberColumn(format="%.2f €"),
            "Sin CIF":      st.column_config.NumberColumn(width="small"),
            "% sin CIF":    st.column_config.ProgressColumn(format="%.1f%%", min_value=0, max_value=100, width="medium"),
            "Sin fecha":    st.column_config.NumberColumn(width="small"),
            "% sin fecha":  st.column_config.ProgressColumn(format="%.1f%%", min_value=0, max_value=100, width="medium"),
            "Sin área":     st.column_config.NumberColumn(width="small"),
            "% sin área":   st.column_config.ProgressColumn(format="%.1f%%", min_value=0, max_value=100, width="medium"),
            "Importe = 0 €": st.column_config.NumberColumn(width="small"),
        },
    )

    st.divider()

    # Detalles por tipo de problema
    tab1, tab2, tab3 = st.tabs(["📅 Sin fecha", "🆔 Sin CIF", "💶 Importes anómalos"])

    with tab1:
        df_sf = contratos_sin_fecha()
        if df_sf.empty:
            st.success("No hay contratos sin fecha.")
        else:
            st.dataframe(df_sf.rename(columns={
                "contratista": "Contratista", "cif": "CIF",
                "objeto": "Objeto", "importe": "Importe (€)", "source_file": "Fichero",
            }), use_container_width=True, hide_index=True,
            column_config={"Importe (€)": st.column_config.NumberColumn(format="%.2f €")})

    with tab2:
        df_sc = contratos_sin_cif()
        if df_sc.empty:
            st.success("Todos los contratos tienen CIF asociado.")
        else:
            st.dataframe(df_sc.rename(columns={
                "fecha": "Fecha", "objeto": "Objeto",
                "importe": "Importe (€)", "expediente": "Expediente", "source_file": "Fichero",
            }), use_container_width=True, hide_index=True,
            column_config={"Importe (€)": st.column_config.NumberColumn(format="%.2f €")})

    with tab3:
        df_ia = contratos_importe_alto()
        if df_ia.empty:
            st.success("No hay contratos con importe superior a 50.000 €.")
        else:
            st.warning(f"⚠️ {len(df_ia)} contratos superan el límite legal de 50.000 € para contratos menores.")
            st.dataframe(df_ia.rename(columns={
                "fecha": "Fecha", "contratista": "Contratista", "cif": "CIF",
                "objeto": "Objeto", "importe": "Importe (€)", "expediente": "Expediente",
            }), use_container_width=True, hide_index=True,
            column_config={"Importe (€)": st.column_config.NumberColumn(format="%.2f €")})
