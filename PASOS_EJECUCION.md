# 🚀 Guía de Ejecución Paso a Paso — Entrega Final ETL
## Sistema Híbrido Batch (Airflow + DW) & Event Streaming (Apache Kafka + Streamlit)
### Universidad Autónoma de Occidente — Maestría / Pregrado en Ingeniería de Datos e IA

Esta guía describe el procedimiento exacto y ordenado para levantar, ejecutar, verificar y probar todo el sistema de datos desde cero en tu computador.

---

## 📋 1. Prerrequisitos del Sistema

Antes de iniciar, asegúrate de tener instalado y en ejecución en tu equipo:
1. **Docker Desktop** (versión 20.10+ con soporte para Docker Compose v2). Debe estar abierto y corriendo.
2. **Python 3.10, 3.11 o 3.12**.
3. **Git** (para control de versiones).
4. Navegador web moderno (Chrome, Edge, Safari, Firefox).
5. Opcional: **Power BI Desktop** (en Windows o mediante máquina virtual) para abrir el archivo `.pbix` de la capa histórica.

---

## 📂 2. Ubicación en la Carpeta del Proyecto

Abre tu terminal y ubícate en la raíz del repositorio de la entrega final:

```bash
cd /Users/usuario/Desktop/ProyectoFinalETL/Proyecto_etl_Final
```

Verifica la estructura de archivos principales:
```bash
ls -la
```
Deberías ver `docker-compose.yaml`, `Dockerfile`, `requirements.txt`, las carpetas `src/`, `sql/`, `streaming/`, `dashboard/` y los documentos explicativos.

---

## ⚙️ 3. Configuración del Archivo de Entorno (`.env`)

Crea tu archivo `.env` a partir de la plantilla segura:

```bash
cp .env.example .env
```

Los valores por defecto configurados en `.env.example` son suficientes para el entorno de desarrollo local:
* Base de datos: `localhost:5432` / DB: `etl_dw` / Usuario: `postgres` / Contraseña: `postgres123`
* Kafka: `localhost:9092`
* API SODA MinJusticia: pública de datos.gov.co (no requiere token obligatorio).

---

## 🐳 4. Levantamiento de la Infraestructura en Docker

Inicia los dos contenedores principales del sistema (PostgreSQL 16 y Apache Kafka 3.7 en modo KRaft):

```bash
docker compose up -d
```

### Verificación de Salud:
Ejecuta el siguiente comando para confirmar que los servicios levantaron correctamente:

```bash
docker ps --filter "name=etl_final"
```

**Resultado esperado:**
* `etl_final_postgres_dw`: Puerto `0.0.0.0:5432->5432`, estado `Up (healthy)`.
* `etl_final_kafka`: Puerto `0.0.0.0:9092->9092`, estado `Up`.

*(Nota: PostgreSQL tarda entre 5 y 10 segundos en pasar a estado `healthy`).*

---

## 🐍 5. Entorno Virtual de Python e Instalación de Dependencias

Se recomienda activar un entorno virtual para aislar las dependencias:

```bash
# Crear entorno virtual (si aún no lo tienes)
python3 -m venv venv

# Activar en macOS / Linux:
source venv/bin/activate

# Instalar dependencias del proyecto:
pip install -r requirements.txt
```

---

## 🔄 6. Ejecución del Pipeline Batch Completo (Extracción, DW y Calidad)

Ejecuta el pipeline de orquestación de datos:

```bash
python3 src/run_pipeline.py --test A
```

### ¿Qué hace este script automáticamente?
1. **Extracción API:** Conecta con la API SODA de datos.gov.co y obtiene el directorio nacional de 1.247 Comisarías de Familia (`data/raw/comisarias_api_raw.json`).
2. **Creación de Esquema DDL:** Ejecuta `sql/01_create_dw.sql` y `sql/02_create_streaming_store.sql` en PostgreSQL, creando las dimensiones (`dim_tiempo`, `dim_geografia`, `dim_victima`, `dim_medio`, `dim_comisaria`), la tabla de hechos `fact_violencia_intrafamiliar` y las tablas de métricas de streaming.
3. **Carga y Reconciliación Masiva:** Carga **701.714 filas de hechos** que representan exactamente a **1.501.809 víctimas** (con reconciliación matemática delta = 0).
4. **Compuertas de Calidad:** Valida las reglas de Great Expectations (nulos, integridad referencial y no negatividad).
5. **Vistas Analíticas:** Materializa las vistas de negocio `vw_r6_sobrecarga_comisarias` y `vw_r7_prioridad_intervencion`.

### Verificación rápida del Data Warehouse:
Puedes comprobar que los datos están listos ejecutando:
```bash
docker exec -it etl_final_postgres_dw psql -U postgres -d etl_dw -c "
SELECT 
    (SELECT COUNT(*) FROM fact_violencia_intrafamiliar) AS total_hechos,
    (SELECT SUM(cantidad_victimas) FROM fact_violencia_intrafamiliar) AS total_victimas,
    (SELECT COUNT(*) FROM dim_comisaria) AS total_comisarias;
"
```
**Salida esperada:** `701714 | 1501809 | 1247`.

---

## ⚡ 7. Ejecución del Pipeline de Streaming en Tiempo Real

Para ver el flujo de eventos de extremo a extremo, abre **3 terminales independientes** en la carpeta `Proyecto_etl_Final`:

```text
┌────────────────────────────────────────────────────────────────────────┐
│ TERMINAL 1: Dashboard Web Streamlit (Visualización en vivo)            │
│ TERMINAL 2: Consumidor Kafka (Aduana de contrato Pydantic + DLQ)       │
│ TERMINAL 3: Productor Kafka (Simulación analítica de denuncias)         │
└────────────────────────────────────────────────────────────────────────┘
```

### Paso A — Iniciar el Dashboard en Vivo (Terminal 1)
```bash
cd /Users/usuario/Desktop/ProyectoFinalETL/Proyecto_etl_Final
python3 -m streamlit run dashboard/app_realtime.py
```
* Se abrirá automáticamente tu navegador en **`http://localhost:8501`**.
* Verás el panel con 3 tarjetas métricas, un gráfico de barras de municipios críticos y la tabla de incidentes recientes.

### Paso B — Iniciar el Consumidor de Eventos (Terminal 2)
```bash
cd /Users/usuario/Desktop/ProyectoFinalETL/Proyecto_etl_Final
python3 streaming/consumer.py --servers localhost:9092
```
* Verás en consola: `[KAFKA-CONSUMER] Conectado a Kafka en ['localhost:9092']. Escuchando tópico: violencia-intrafamiliar-events...`
* El consumidor queda en espera activa de mensajes.

### Paso C — Emitir Eventos con el Productor (Terminal 3)
```bash
cd /Users/usuario/Desktop/ProyectoFinalETL/Proyecto_etl_Final
python3 streaming/producer.py --servers localhost:9092 --count 25 --interval 1.0
```
* El productor generará 25 denuncias simuladas (1 por segundo) con variación analítica (armas de fuego, agresiones a menores y municipios con déficit según la Ley 2126).
* **En la Terminal 2:** Observarás cómo cada mensaje es validado por Pydantic y guardado en PostgreSQL en tiempo real.
* **En el Navegador (Streamlit):** Observarás cómo el contador de víctimas se actualiza cada 3 segundos, la tasa de letalidad se recalcula en vivo y las barras de ciudades en déficit (Cúcuta, Soacha) se iluminan en rojo.

---

## 🛡️ 8. Ejecución Automatizada de Pruebas de Resiliencia

Para demostrar formalmente ante el profesor que la arquitectura tolera fallos sin pérdida de datos ni caídas de servicio:

En la **Terminal 3**, ejecuta:
```bash
cd /Users/usuario/Desktop/ProyectoFinalETL/Proyecto_etl_Final
python3 streaming/test_resilience.py --servers localhost:9092
```

### ¿Qué evalúa este script?
1. **Experimento 1 (Dead-Letter Queue - DLQ):**
   * Inyecta intencionalmente un payload corrupto (`cantidad_victimas = -999`, fecha basura `FECHA_INVALIDA_2026`).
   * El consumidor captura la excepción del contrato Pydantic (`EventContractViolationError`), aísla el registro en la tabla `streaming_invalid_events_log` y continúa operando sin caerse.
2. **Experimento 2 (Recuperación ante caída de consumidor):**
   * Detiene el proceso del consumidor.
   * Envía 5 eventos válidos al broker de Kafka (quedan encolados).
   * Vuelve a encender el consumidor: este lee desde el último offset confirmado y procesa el 100% de los mensajes encolados con **0% de pérdida de datos**.

**Resultado esperado al final del script:**
```text
============================================================
RESUMEN DE PRUEBAS DE RESILIENCIA Y RECUPERACIÓN:
  - Experimento 1 (Dead-Letter Queue / DLQ): PASÓ CON ÉXITO
  - Experimento 2 (Recuperación tras Caída): PASÓ CON ÉXITO
Tasa de pérdida de datos: 0.00%
============================================================
```

---

## 📊 9. Visualización de Dashboards y Presentación Interactiva

### 1. Dashboard Histórico en Power BI (Capa Batch)
* Ubicación: [`dashboard/batch/Dashboard-Poryecto2.pbix`](file:///Users/usuario/Desktop/ProyectoFinalETL/Proyecto_etl_Final/dashboard/batch/Dashboard-Poryecto2.pbix).
* Ábrelo en **Power BI Desktop**.
* Responde los 7 requerimientos de negocio (R1 a R7), incluyendo la pirámide de género (77% mujeres), concentración de armas letales y el déficit nacional de 114 comisarías bajo la Ley 2126 de 2021.
* Las capturas exportadas en alta resolución están en `docs/evidences/` y `dashboard/batch/`.

### 2. Presentación Interactiva de Sustentación
* Ubicación: [`presentacion_final.html`](file:///Users/usuario/Desktop/ProyectoFinalETL/presentacion_final.html) (también disponible en la raíz del proyecto).
* Ábrelo haciendo doble clic o arrastrándolo a cualquier navegador web.
* **Atajos de teclado:**
  * Presiona `F` para activar pantalla completa profesional.
  * Presiona `G` para abrir el índice de diapositivas.
  * Clic en cualquier imagen para abrir el **Lightbox Zoom** y ampliar diagramas en alta resolución.
  * Usa las pestañas interactivas en la diapositiva 4 para ver las 3 compuertas de Great Expectations.

---

## 🛠️ 10. Comandos Útiles de Diagnóstico y Monitoreo

### Ver las métricas vivas calculadas en la base de datos:
```bash
docker exec -it etl_final_postgres_dw psql -U postgres -d etl_dw -c "SELECT * FROM vw_streaming_resumen_vivo;"
```

### Ver los eventos corruptos atrapados en la Dead-Letter Queue (DLQ):
```bash
docker exec -it etl_final_postgres_dw psql -U postgres -d etl_dw -c "SELECT id, error_reason, received_at FROM streaming_invalid_events_log ORDER BY id DESC LIMIT 5;"
```

### Ver las métricas por municipio en streaming:
```bash
docker exec -it etl_final_postgres_dw psql -U postgres -d etl_dw -c "SELECT * FROM streaming_municipal_metrics ORDER BY total_victimas DESC LIMIT 5;"
```

### Ver los logs de Kafka:
```bash
docker logs etl_final_kafka --tail 50
```

---

## 🛑 11. Detención y Limpieza de Servicios

Cuando termines tu sesión de trabajo o sustentación:

```bash
# Detener los contenedores preservando los datos:
docker compose stop

# O apagar y desmontar la red de contenedores:
docker compose down

# ADVERTENCIA: Solo si deseas borrar todos los volúmenes y empezar desde cero:
# docker compose down -v
```

---

## 📚 Documentos de Apoyo Relacionados
* 🎙️ **Guion de sustentación verbal:** [`GUION_SUSTENTACION_FINAL.md`](file:///Users/usuario/Desktop/ProyectoFinalETL/GUION_SUSTENTACION_FINAL.md)
* 📖 **Explicación integral para estudio:** [`EXPLICACION_ENTREGA_FINAL.md`](file:///Users/usuario/Desktop/ProyectoFinalETL/EXPLICACION_ENTREGA_FINAL.md)
* 📑 **Contexto de negocio y ODS:** [`CONTEXTO.md`](file:///Users/usuario/Desktop/ProyectoFinalETL/CONTEXTO.md)
*  Master README técnico en 12 secciones: [`Proyecto_etl_Final/README.md`](file:///Users/usuario/Desktop/ProyectoFinalETL/Proyecto_etl_Final/README.md)
