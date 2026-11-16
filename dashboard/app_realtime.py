"""Real-Time Streaming Dashboard (Streamlit & Plotly).

Connects to PostgreSQL streaming store (streaming_metrics_store, streaming_events_log,
and streaming_municipal_metrics) with configurable auto-refresh to visualize:
  - Metric 1: Real-time operational volume (events & victims)
  - Metric 2: Lethality rate & child victim proportion
  - Metric 3: Institutional deficit saturation alerts (Ley 2126)
  - Audit log and Dead-Letter Queue (DLQ) health status

Complies with Section 2.7 of the Final Project Rubric.
"""

from datetime import datetime
import os
import time

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import psycopg2
import streamlit as st

import sqlalchemy
from sqlalchemy import create_engine

# Page configuration
st.set_page_config(
    page_title="Streaming ETL — Violencia Intrafamiliar & Déficit Institucional",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for styling
st.markdown("""
<style>
    .metric-card {
        background-color: #1e222d;
        border-radius: 10px;
        padding: 15px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
        border-left: 5px solid #ff4b4b;
    }
    .status-badge-crit {
        background-color: #d32f2f;
        color: white;
        padding: 4px 10px;
        border-radius: 4px;
        font-weight: bold;
    }
    .status-badge-ok {
        background-color: #388e3c;
        color: white;
        padding: 4px 10px;
        border-radius: 4px;
        font-weight: bold;
    }
</style>
""", unsafe_allow_html=True)


@st.cache_resource
def get_db_engine():
    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", 5432))
    dbname = os.getenv("DB_NAME", "etl_dw")
    user = os.getenv("DB_USER", "postgres")
    password = os.getenv("DB_PASSWORD", "postgres123")
    url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(url, pool_pre_ping=True)


def load_streaming_data():
    engine = get_db_engine()
    with engine.connect() as conn:
        # 1. Global KPIs
        metrics_df = pd.read_sql("SELECT * FROM streaming_metrics_store WHERE id = 1;", conn)

        # 2. Recent Events Audit Log (Top 15)
        events_df = pd.read_sql(
            """
            SELECT
                event_id,
                to_char(event_timestamp, 'HH24:MI:SS') as hora,
                municipio,
                departamento,
                genero,
                grupo_etario,
                medio_agresion,
                es_arma_letal,
                cantidad_victimas,
                tiene_deficit_ley_2126
            FROM streaming_events_log
            ORDER BY id DESC
            LIMIT 15;
            """,
            conn
        )

        # 3. Municipal Ranking
        municipal_df = pd.read_sql(
            """
            SELECT
                municipio,
                departamento,
                victimas_recientes,
                casos_arma_letal,
                victimas_menores,
                tiene_deficit_ley_2126
            FROM streaming_municipal_metrics
            ORDER BY victimas_recientes DESC
            LIMIT 10;
            """,
            conn
        )

        # 4. Dead-Letter Queue (DLQ)
        dlq_df = pd.read_sql(
            """
            SELECT id, event_key, error_reason, to_char(received_at, 'HH24:MI:SS') as hora_rechazo
            FROM streaming_invalid_events_log
            ORDER BY id DESC
            LIMIT 5;
            """,
            conn
        )

    return metrics_df, events_df, municipal_df, dlq_df


# Sidebar Controls
st.sidebar.title("⚡ Control de Streaming")
st.sidebar.markdown("**Pipeline:** Apache Kafka ➔ Consumer ➔ PostgreSQL 16")
refresh_rate = st.sidebar.slider("Intervalo de Refresco (segundos)", min_value=1, max_value=10, value=3)
auto_refresh = st.sidebar.checkbox("Auto-refresco activo", value=True)

st.sidebar.markdown("---")
st.sidebar.markdown("### 📋 Arquitectura de Streaming")
st.sidebar.info("""
- **Tópico Kafka:** `violencia-intrafamiliar-events`
- **Contrato:** `ViolenciaIntrafamiliarEvent` (Pydantic V2)
- **Persistencia:** Tabla `streaming_metrics_store`
- **DLQ:** Tabla `streaming_invalid_events_log`
""")

# Main Header
st.title("⚡ Tablero en Tiempo Real: Violencia Intrafamiliar & Sobrecarga Institucional")
st.caption("Visualización de baja latencia alimentada continuamente por eventos Kafka validados contra el Contrato Formal de Eventos.")

try:
    metrics_df, events_df, municipal_df, dlq_df = load_streaming_data()
    m = metrics_df.iloc[0] if not metrics_df.empty else None
except Exception as e:
    st.error(f"⚠️ Error conectando con la base de datos de streaming: {e}")
    st.info("Asegúrese de que el contenedor 'postgres_dw' y las tablas de streaming estén inicializadas.")
    st.stop()

# Top 3 Streaming Metrics Display
if m is not None:
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric(
            label="Métrica 1: Eventos Procesados",
            value=f"{m['total_eventos_procesados']:,}",
            delta=f"{m['total_victimas_procesadas']} víctimas totales"
        )

    with col2:
        st.metric(
            label="Métrica 2a: Tasa de Letalidad",
            value=f"{m['tasa_letalidad_pct']:.1f}%",
            delta=f"{m['total_casos_arma_letal']} armas letales",
            delta_color="inverse"
        )

    with col3:
        st.metric(
            label="Métrica 2b: Afectación a Menores",
            value=f"{m['tasa_menores_pct']:.1f}%",
            delta=f"{m['total_victimas_menores']} niños/adolescentes",
            delta_color="inverse"
        )

    with col4:
        presion = m['indice_presion_operativa']
        badge_color = "status-badge-crit" if "CRÍTICA" in presion or "ALTA" in presion else "status-badge-ok"
        st.metric(
            label="Métrica 3: Alertas Déficit Ley 2126",
            value=f"{m['total_alertas_deficit_ley2126']:,}",
            delta=f"Estado: {presion}",
            delta_color="off"
        )

st.markdown("---")

# Visual Analytics Row
chart_col1, chart_col2 = st.columns([6, 4])

with chart_col1:
    st.subheader("📍 Top Municipios con Mayor Volumen en Vivo")
    if not municipal_df.empty:
        fig_bar = px.bar(
            municipal_df,
            x="victimas_recientes",
            y="municipio",
            color="tiene_deficit_ley_2126",
            orientation="h",
            color_discrete_map={True: "#e53935", False: "#1e88e5"},
            labels={
                "victimas_recientes": "Víctimas Recientes",
                "municipio": "Municipio",
                "tiene_deficit_ley_2126": "¿Déficit Ley 2126?"
            },
            title="Víctimas Recientes por Municipio (Rojo = Déficit Estatutario de Comisarías)",
            template="plotly_dark"
        )
        fig_bar.update_layout(yaxis={'categoryorder': 'total ascending'}, height=350, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig_bar, use_container_width=True)
    else:
        st.info("Esperando eventos para generar el ranking municipal...")

with chart_col2:
    st.subheader("⚖️ Distribución de Vulnerabilidad en Vivo")
    if m is not None and m['total_victimas_procesadas'] > 0:
        labels = ['Mujeres Adultas', 'Menores de Edad', 'Hombres Adultos']
        mujeres_adultas = max(0, m['total_victimas_mujeres'] - m['total_victimas_menores'])
        values = [mujeres_adultas, m['total_victimas_menores'], max(0, m['total_victimas_procesadas'] - m['total_victimas_mujeres'])]
        fig_donut = px.pie(
            names=labels,
            values=values,
            hole=0.5,
            color_discrete_sequence=['#8e24aa', '#fb8c00', '#039be5'],
            template="plotly_dark",
            title="Cohortes Demográficas Afectadas"
        )
        fig_donut.update_layout(height=350, margin=dict(l=10, r=10, t=40, b=10))
        st.plotly_chart(fig_donut, use_container_width=True)
    else:
        st.info("Esperando eventos para la distribución...")

# Feed of Live Incoming Events
st.subheader("📋 Flujo de Eventos Entrantes (Últimos 15 procesados)")
if not events_df.empty:
    st.dataframe(
        events_df,
        column_config={
            "hora": "Hora UTC",
            "municipio": "Municipio",
            "departamento": "Departamento",
            "genero": "Género",
            "grupo_etario": "Ciclo Vital",
            "medio_agresion": "Mecanismo",
            "es_arma_letal": st.column_config.CheckboxColumn("¿Arma Letal?"),
            "cantidad_victimas": "Víctimas",
            "tiene_deficit_ley_2126": st.column_config.CheckboxColumn("¿Déficit Ley 2126?"),
        },
        hide_index=True,
        use_container_width=True
    )
else:
    st.info("No se han recibido eventos todavía. Ejecute el productor para emitir registros.")

# Dead-Letter Queue (Resilience Proof)
st.subheader("🛡️ Resiliencia & Dead-Letter Queue (Eventos Malformados Rechazados)")
if not dlq_df.empty:
    st.warning("Se han detectado eventos que violaron el Contrato Formal de Eventos. El consumidor los aisló sin colapsar:")
    st.dataframe(dlq_df, hide_index=True, use_container_width=True)
else:
    st.success("✅ Cero fallas en cola de eventos muertos. Todos los eventos procesados cumplen el contrato formal.")

# Auto-refresh loop
if auto_refresh:
    time.sleep(refresh_rate)
    st.rerun()

