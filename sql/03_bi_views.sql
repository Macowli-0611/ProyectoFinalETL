-- ============================================================================
-- 03_bi_views.sql
-- Semantic Views for BI & Real-Time Analytics
-- ============================================================================

DROP VIEW IF EXISTS vw_bi_hechos_completos CASCADE;
DROP VIEW IF EXISTS vw_bi_presion_institucional CASCADE;
DROP VIEW IF EXISTS vw_bi_brecha_riesgo_menores CASCADE;
DROP VIEW IF EXISTS vw_r6_sobrecarga_comisarias CASCADE;
DROP VIEW IF EXISTS vw_r7_prioridad_intervencion CASCADE;
DROP VIEW IF EXISTS vw_streaming_resumen_vivo CASCADE;

-- 1. vw_bi_hechos_completos: Denormalized star schema for Power BI historical analysis
CREATE VIEW vw_bi_hechos_completos AS
SELECT
    f.id_hecho,
    -- Time Dimension
    t.fecha,
    t.anio,
    t.mes,
    t.nombre_mes,
    t.trimestre,
    t.semestre,
    t.es_fin_semana,
    t.es_periodo_completo,
    -- Geographic Dimension (Enriched)
    g.codigo_dane_municipio,
    g.municipio,
    g.codigo_dane_departamento,
    g.departamento,
    g.es_capital,
    g.categoria_municipio,
    g.comisarias_instaladas,
    g.comisarias_requeridas_ley,
    g.brecha_comisarias,
    g.tiene_comisaria,
    -- Victim Demographics
    v.genero,
    v.grupo_etario,
    v.perfil_demografico,
    v.es_menor_de_edad,
    -- Weapon / Mechanism
    m.medio_agresion,
    m.categoria_mecanismo,
    m.es_arma_letal,
    -- Measures
    f.cantidad_victimas,
    f.numero_registros,
    f.fecha_carga
FROM fact_violencia_intrafamiliar f
JOIN dim_tiempo t ON f.id_tiempo = t.id_tiempo
JOIN dim_geografia g ON f.id_geografia = g.id_geografia
JOIN dim_victima v ON f.id_victima = v.id_victima
JOIN dim_medio m ON f.id_medio = m.id_medio;

-- 2. vw_r6_sobrecarga_comisarias: Supports R6
CREATE VIEW vw_r6_sobrecarga_comisarias AS
SELECT
    g.codigo_dane_municipio,
    g.municipio,
    g.departamento,
    g.categoria_municipio,
    g.comisarias_instaladas,
    SUM(f.cantidad_victimas) AS total_victimas,
    ROUND(
        SUM(f.cantidad_victimas)::NUMERIC / NULLIF(g.comisarias_instaladas, 0), 1
    ) AS victimas_por_comisaria,
    CASE
        WHEN g.comisarias_instaladas = 0 THEN 'SIN COMISARÍA'
        WHEN (SUM(f.cantidad_victimas)::NUMERIC / g.comisarias_instaladas) >= 2000 THEN 'SOBRECARGA CRÍTICA'
        WHEN (SUM(f.cantidad_victimas)::NUMERIC / g.comisarias_instaladas) >= 1000 THEN 'SOBRECARGA ALTA'
        ELSE 'CARGA MODERADA'
    END AS nivel_presion
FROM fact_violencia_intrafamiliar f
JOIN dim_geografia g ON f.id_geografia = g.id_geografia
GROUP BY
    g.codigo_dane_municipio, g.municipio, g.departamento,
    g.categoria_municipio, g.comisarias_instaladas;

-- 3. vw_r7_prioridad_intervencion: Supports R7
CREATE VIEW vw_r7_prioridad_intervencion AS
SELECT
    g.codigo_dane_municipio,
    g.municipio,
    g.departamento,
    g.categoria_municipio,
    g.comisarias_instaladas,
    g.comisarias_requeridas_ley,
    g.brecha_comisarias,
    SUM(f.cantidad_victimas) AS total_victimas,
    SUM(CASE WHEN v.es_menor_de_edad AND m.es_arma_letal THEN f.cantidad_victimas ELSE 0 END) AS victimas_menores_arma_letal,
    SUM(CASE WHEN v.genero = 'FEMENINO' THEN f.cantidad_victimas ELSE 0 END) AS victimas_mujeres,
    CASE
        WHEN g.brecha_comisarias >= 5 AND SUM(CASE WHEN v.es_menor_de_edad AND m.es_arma_letal THEN f.cantidad_victimas ELSE 0 END) > 20
            THEN 'PRIORIDAD 1 (URGENTE)'
        WHEN g.brecha_comisarias >= 1 AND SUM(CASE WHEN v.es_menor_de_edad AND m.es_arma_letal THEN f.cantidad_victimas ELSE 0 END) > 0
            THEN 'PRIORIDAD 2 (ALTA)'
        WHEN g.brecha_comisarias >= 1
            THEN 'PRIORIDAD 3 (ESTATUTARIA)'
        ELSE 'CONFORME A LEY'
    END AS indice_prioridad
FROM fact_violencia_intrafamiliar f
JOIN dim_geografia g ON f.id_geografia = g.id_geografia
JOIN dim_victima v ON f.id_victima = v.id_victima
JOIN dim_medio m ON f.id_medio = m.id_medio
GROUP BY
    g.codigo_dane_municipio, g.municipio, g.departamento,
    g.categoria_municipio, g.comisarias_instaladas,
    g.comisarias_requeridas_ley, g.brecha_comisarias;

-- 4. vw_streaming_resumen_vivo: Unifies live indicators with municipal context
CREATE VIEW vw_streaming_resumen_vivo AS
SELECT
    m.codigo_dane_municipio,
    m.municipio,
    m.departamento,
    m.categoria_municipio,
    m.eventos_recientes,
    m.victimas_recientes,
    m.casos_arma_letal,
    m.victimas_menores,
    m.tiene_deficit_ley_2126,
    m.ultima_alerta
FROM streaming_municipal_metrics m
ORDER BY m.victimas_recientes DESC, m.ultima_alerta DESC;

