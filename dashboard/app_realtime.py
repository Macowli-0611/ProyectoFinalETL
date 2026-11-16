"""Real-time dashboard (Streamlit + Plotly) for the streaming layer.

Consumer -> dashboard integration: the Kafka consumer commits every processed
event and the updated metrics to PostgreSQL (streaming_metrics_store,
streaming_events_log, streaming_municipal_metrics, streaming_invalid_events_log).
This page re-queries those tables every N seconds (default 3 s, sidebar slider)
using an auto-refreshing Streamlit fragment, so a new event is visible at most
N seconds after the consumer commits it.

It is intentionally different from the Power BI batch dashboard: it only shows
what arrived through Kafka since the streaming store was (re)created - volume,
severity and institutional pressure *right now* - not the 2010-2026 history.
"""

from datetime import datetime, timezone
import os
import sys

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)
from streaming.db import db_params  # noqa: E402

st.set_page_config(page_title="Streaming - Violencia intrafamiliar", page_icon="⚡", layout="wide")


@st.cache_resource
def engine():
    p = db_params()
    url = URL.create("postgresql+psycopg2", username=p["user"], password=p["password"],
                     host=p["host"], port=p["port"], database=p["dbname"])
    return create_engine(url, pool_pre_ping=True, pool_size=2)


def q(sql: str) -> pd.DataFrame:
    with engine().connect() as conn:
        return pd.read_sql(sql, conn)


# ----------------------------------------------------------------------------- sidebar
st.sidebar.title("⚡ Streaming")
refresh = st.sidebar.slider("Intervalo de refresco (s)", 1, 10, 3)
st.sidebar.markdown(
    """
**Flujo:** Producer (réplica del DW) → Kafka `violencia-intrafamiliar-events`
(3 particiones, key = DANE) → Consumer (contrato v2) → PostgreSQL → este tablero.

**Origen de eventos:** filas de `fact_violencia_intrafamiliar` reproducidas en orden
cronológico — *simulación* de llegada (`origen = DW_REPLAY_SIMULATION`).

**Diferencia con Power BI:** Power BI analiza la historia 2010-2026 del DW;
este tablero sólo muestra lo que llega por Kafka y cambia con cada evento.
"""
)

st.title("Tablero en tiempo real — violencia intrafamiliar y presión institucional")


@st.fragment(run_every=refresh)
def live():
    try:
        m = q("SELECT * FROM streaming_metrics_store WHERE id = 1;").iloc[0]
    except Exception as e:  # noqa: BLE001
        st.error(f"No se pudo leer el streaming store: {e}")
        return

    now = datetime.now(timezone.utc)
    st.caption(
        f"Última consulta: {now:%H:%M:%S} UTC · último evento procesado: "
        f"{m['ultimo_evento_id'] or '—'} ({m['ultimo_municipio_reportado'] or '—'}) · "
        f"refresco cada {refresh} s"
    )

    # ------------------------------------------------------------ 3 streaming metrics
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Métrica 1 · Eventos procesados", f"{int(m['total_eventos_procesados']):,}",
              f"{int(m['total_victimas_procesadas']):,} víctimas", delta_color="off")
    c2.metric("Métrica 2 · Eventos con arma letal", f"{float(m['tasa_letalidad_pct']):.1f} %",
              f"{int(m['total_casos_arma_letal']):,} casos", delta_color="off")
    c3.metric("Métrica 2 · Víctimas menores de edad", f"{float(m['tasa_menores_pct']):.1f} %",
              f"{int(m['total_victimas_menores']):,} niñas, niños y adolescentes", delta_color="off")
    estado = m["indice_presion_operativa"]
    icon = {"NORMAL": "🟢", "ALTA": "🟠", "CRITICA": "🔴"}.get(estado, "⚪")
    c4.metric("Métrica 3 · Presión institucional",
              f"{icon} {estado}", f"{float(m['pct_eventos_deficit_ventana']):.0f} % de los últimos 50 en municipios con déficit",
              delta_color="off")

    h1, h2, h3, h4 = st.columns(4)
    h1.metric("Víctimas mujeres", f"{float(m['tasa_mujeres_pct']):.1f} %")
    h2.metric("Latencia media publicación → BD", f"{float(m['latencia_promedio_ms']):,.0f} ms")
    h3.metric("Eventos rechazados (DLQ)", f"{int(m['total_eventos_invalidos']):,}")
    h4.metric("Duplicados descartados", f"{int(m['total_duplicados_descartados']):,}")

    st.divider()

    # ------------------------------------------------------------ charts
    left, right = st.columns([3, 2])
    with left:
        rate = q("""
            SELECT to_timestamp(floor(extract(epoch FROM processed_at) / 10) * 10) AT TIME ZONE 'UTC' AS ventana,
                   count(*) AS eventos, sum(cantidad_victimas) AS victimas,
                   sum(CASE WHEN es_arma_letal THEN 1 ELSE 0 END) AS letales
            FROM streaming_events_log
            WHERE processed_at > now() - interval '10 minutes'
            GROUP BY 1 ORDER BY 1;
        """)
        st.subheader("Eventos procesados por ventana de 10 s (últimos 10 min)")
        if rate.empty:
            st.info("Sin eventos en los últimos 10 minutos. Ejecute el productor.")
        else:
            fig = px.bar(rate, x="ventana", y="eventos", hover_data=["victimas", "letales"],
                         labels={"ventana": "Hora (UTC)", "eventos": "Eventos"})
            fig.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)

    with right:
        st.subheader("Top municipios en vivo")
        mun = q("""SELECT municipio, victimas_recientes, casos_arma_letal,
                          CASE WHEN tiene_deficit_ley_2126 THEN 'Con déficit Ley 2126' ELSE 'Sin déficit' END AS deficit
                   FROM streaming_municipal_metrics ORDER BY victimas_recientes DESC, ultima_alerta DESC LIMIT 10;""")
        if mun.empty:
            st.info("Esperando eventos…")
        else:
            fig = px.bar(mun, x="victimas_recientes", y="municipio", color="deficit", orientation="h",
                         color_discrete_map={"Con déficit Ley 2126": "#d64545", "Sin déficit": "#3b7dd8"},
                         labels={"victimas_recientes": "Víctimas", "municipio": "", "deficit": ""})
            fig.update_layout(height=320, margin=dict(l=10, r=10, t=10, b=10),
                              yaxis={"categoryorder": "total ascending"}, legend=dict(orientation="h", y=-0.2))
            st.plotly_chart(fig, use_container_width=True)

    a, b = st.columns(2)
    with a:
        st.subheader("Víctimas por cohorte")
        coh = q("""SELECT genero || ' · ' || CASE WHEN es_menor_de_edad THEN 'menor' ELSE 'adulto' END AS cohorte,
                          sum(cantidad_victimas) AS victimas
                   FROM streaming_events_log GROUP BY 1 ORDER BY 2 DESC;""")
        if not coh.empty:
            fig = px.pie(coh, names="cohorte", values="victimas", hole=0.5)
            fig.update_layout(height=300, margin=dict(l=10, r=10, t=10, b=10))
            st.plotly_chart(fig, use_container_width=True)
    with b:
        st.subheader("Mecanismo de agresión")
        med = q("""SELECT medio_agresion, count(*) AS eventos FROM streaming_events_log
                   GROUP BY 1 ORDER BY 2 DESC;""")
        if not med.empty:
            fig = px.bar(med, x="eventos", y="medio_agresion", orientation="h",
                         labels={"eventos": "Eventos", "medio_agresion": ""})
            fig.update_layout(height=300, margin=dict(l=10, r=10, t=10, b=10),
                              yaxis={"categoryorder": "total ascending"})
            st.plotly_chart(fig, use_container_width=True)

    st.subheader("Últimos 15 eventos procesados")
    feed = q("""SELECT to_char(processed_at AT TIME ZONE 'UTC', 'HH24:MI:SS') AS procesado_utc, event_id,
                       fecha_hecho, municipio, departamento, genero, grupo_etario, medio_agresion,
                       es_arma_letal, cantidad_victimas, tiene_deficit_ley_2126, kafka_partition, latencia_ms
                FROM streaming_events_log ORDER BY id DESC LIMIT 15;""")
    st.dataframe(feed, hide_index=True, use_container_width=True)

    st.subheader("Dead-Letter Queue (eventos que violan el contrato)")
    dlq = q("""SELECT id, to_char(received_at AT TIME ZONE 'UTC', 'HH24:MI:SS') AS recibido_utc, event_key,
                      kafka_partition, kafka_offset, left(error_reason, 220) AS motivo
               FROM streaming_invalid_events_log ORDER BY id DESC LIMIT 5;""")
    if dlq.empty:
        st.success("Sin eventos rechazados.")
    else:
        st.dataframe(dlq, hide_index=True, use_container_width=True)


live()
