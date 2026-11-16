# 🎙️ Guion Maestro de Sustentación & Defensa Técnica — Entrega Final ETL
## Sistema de Datos Híbrido: Batch ETL (Airflow + Great Expectations) & Event Streaming (Apache Kafka + Streamlit)

> **Duración total estimada:** 10 a 12 minutos de exposición + 3 minutos de demostración en vivo.  
> **Presentación interactiva:** [`presentacion_final.html`](presentacion_final.html)  
> **Atajos útiles durante la exposición:**
> - `F`: Activar / Desactivar **Pantalla Completa**.
> - `G` o botón "Índice": Abrir el menú lateral para saltar a cualquier diapositiva.
> - `Flechas ← / →` o `Espacio`: Avanzar y retroceder diapositivas.
> - `Clic en imágenes`: Abre el **Lightbox Zoom** para ampliar diagramas y capturas en alta resolución.

---

## 👥 Tabla de Asignación de Roles (4 Integrantes)

| Expositor | Diapositivas | Eje Temático Principal | Especialidad ante Preguntas del Profesor |
| :--- | :---: | :--- | :--- |
| 👤 **Michael** | **1, 2 y 3** | **Contexto, Negocio y Arquitectura:** Portada, cifras de impacto, el problema de las denuncias vs. capacidad del Estado (Ley 2126) y la arquitectura híbrida Lambda-Lite (Figura 1 oficial). | Desacoplamiento Batch/Streaming, API SODA, visión estratégica. |
| 👤 **Manrique** | **4, 5 y 6** | **Pipeline Batch, Calidad y Hallazgos:** Las 3 compuertas de Great Expectations con bloqueo en Airflow (`AirflowFailException`), modelo dimensional en estrella en Postgres y hallazgos en Power BI (R1 a R7). | Reglas de calidad GX, reconciliación matemática (delta = 0), esquema estrella y DDL. |
| 👤 **David** | **7, 8 y 9** | **Pipeline Streaming y Métricas en Vivo:** Kafka en modo KRaft, Contrato de Eventos formal en Pydantic V2, las 3 métricas de baja latencia y el Dashboard en Streamlit. | Tópicos y particiones, validación de contrato, cálculo atómico de métricas y refresco. |
| 👤 **Torres** | **10, 11, 12 y Demo** | **Resiliencia, Decisiones de Ingeniería y Demo:** Los dos experimentos obligatorios (evento corrupto en DLQ y corte del consumidor sin pérdida), decisiones técnicas, conclusiones y comandos en vivo. | Dead-Letter Queue (DLQ), offsets de Kafka, resiliencia y ejecución en vivo de terminales. |

---

# 🗣️ Discurso Paso a Paso por Diapositiva

---

## 🔵 BLOQUE 1: CONTEXTO, NEGOCIO Y ARQUITECTURA (Michael)

### 📌 Diapositiva 1: Portada y Grandes Números del Sistema
* **Expositor:** **Michael** (~1 minuto)
* **Acción en pantalla:** Mantener la portada en pantalla completa (`F`). Señalar con el cursor las 4 tarjetas superiores con los números clave.
* **Discurso exacto:**
> *"Buenos días / tardes, profesor y compañeros. Hoy les presentamos la entrega final de nuestro proyecto de Ingeniería de Datos.
>
> En las entregas anteriores construimos una línea base sólida analizando 15 años de historia de violencia intrafamiliar en Colombia. Pero para esta entrega final, dimos el paso definitivo hacia una **arquitectura híbrida completa**: no solo consolidamos el análisis histórico en un Data Warehouse, sino que implementamos un **pipeline de streaming en tiempo real con Apache Kafka** capaz de procesar denuncias segundo a segundo y alertar sobre la saturación institucional del Estado.
>
> Para dimensionar el sistema que tienen en pantalla:
> - Reconciliamos **701.714 hechos dimensionales** que representan a más de **1.5 millones de víctimas**, con cero pérdida de datos.
> - Integramos las **1.247 Comisarías de Familia** del país mediante la API del Ministerio de Justicia.
> - Desarrollamos un motor de streaming que calcula **3 métricas de baja latencia al instante**.
> - Y todo el sistema cuenta con **100% de reproducibilidad en Docker** y pruebas de resiliencia automatizadas."*

---

### 📌 Diapositiva 2: El Problema de Negocio y la Ley 2126 de 2021
* **Expositor:** **Michael** (~1 minuto)
* **Acción en pantalla:** Avanzar a Slide 2. Contrastar visualmente la columna izquierda (el error tradicional) con la columna derecha (nuestro aporte con la Ley 2126).
* **Discurso exacto:**
> *"El punto de partida de nuestro diseño fue entender una limitación que cometen muchas entidades: **medir solo el número de denuncias policiales genera un sesgo severo**.
>
> Si una ciudad registra muchas denuncias, muchas veces solo significa que tiene oficinas abiertas donde la gente puede ir a quejarse. En cambio, si un municipio registra pocas denuncias, podría ser simplemente que las víctimas están desprotegidas y no tienen a dónde acudir. Sabíamos cuánto ocurría la violencia, pero no si el Estado tenía capacidad de atenderla.
>
> Para resolver esto, conectamos la API del Ministerio de Justicia y aplicamos el estándar legal de la **Ley 2126 de 2021**, que exige que en Colombia exista **al menos 1 Comisaría de Familia por cada 100.000 habitantes**.
>
> Al cruzar ambas fuentes, descubrimos algo contundente: el colapso no está en las grandes capitales como Bogotá, sino en ciudades intermedias como **Cúcuta**, que tiene más de 20.000 víctimas y una sola comisaría instalada, o **Soacha**, donde faltan 6 comisarías. En total, demostramos que en Colombia **faltan 114 comisarías por construir** para cumplir la ley."*

---

### 📌 Diapositiva 3: Arquitectura Integrada Batch & Streaming (Figura 1)
* **Expositor:** **Michael** (~1 minuto)
* **Acción en pantalla:** Hacer clic sobre la imagen de la arquitectura para abrirla en **Lightbox Zoom** en pantalla completa. Señalar la división en dos ramas.
* **Discurso exacto:**
> *"Esta es la arquitectura integral del sistema, diseñada con base en la Figura 1 recomendada en la rúbrica oficial.
>
> La solución funciona bajo un patrón **Lambda-Lite**, dividido en dos rutas totalmente desacopladas:
> 1. **Por el lado izquierdo (Ruta Batch):** Apache Airflow orquesta periódicamente la extracción masiva del CSV de la DIJIN y la API SODA. Pasa por tres compuertas de calidad con Great Expectations y carga el Data Warehouse en PostgreSQL, que alimenta nuestro tablero gerencial en Power BI.
> 2. **Por el lado derecho (Ruta Streaming):** Apache Kafka recibe los nuevos eventos en tiempo real. Un consumidor valida la estructura de cada mensaje y actualiza en milisegundos una tabla de métricas en PostgreSQL que alimenta un tablero reactivo en Streamlit.
>
> Lo más valioso de esta arquitectura es su **independencia**: si el consumidor de tiempo real se interrumpe o si la red se corta, el Data Warehouse histórico y los reportes de Power BI siguen funcionando sin ninguna alteración."*
* *(Presionar tecla `Escape` o hacer clic en ✕ para cerrar el Lightbox y ceder la palabra a Manrique).*

---

## 🟢 BLOQUE 2: PIPELINE BATCH, CALIDAD Y DATA WAREHOUSE (Manrique)

### 📌 Diapositiva 4: Orquestación Batch & Compuertas de Calidad
* **Expositor:** **Manrique** (~1.2 minutos)
* **Acción en pantalla:** Avanzar a Slide 4. Hacer clic entre las pestañas: *Compuerta 1*, *Compuerta 2* y *Compuerta 3*.
* **Discurso exacto:**
> *"Gracias, Michael. Pasando al núcleo de procesamiento por lotes: la orquestación fue implementada en **Apache Airflow 3** utilizando la TaskFlow API.
>
> Para asegurar que ningún dato corrupto toque las tablas analíticas, implementamos tres compuertas de calidad con **Great Expectations 1.X**:
> - *(Clic en Compuerta 1)* **Compuerta 1 (Crudos DIJIN):** Verifica 704.000 registros policiales, exigiendo código DANE de 5 dígitos, rangos válidos de fechas y volumen de víctimas estrictamente positivo.
> - *(Clic en Compuerta 2)* **Compuerta 2 (API Comisarías):** Valida la presencia de municipios, sedes activas y no duplicidad en el directorio institucional.
> - *(Clic en Compuerta 3)* **Compuerta 3 (Datos Preparados):** Se ejecuta antes de tocar la base de datos final. Garantiza la unicidad del grano municipal y la **reconciliación matemática exacta de las 1.501.809 víctimas**.
>
> **Un aspecto crítico exigido por la rúbrica:** Si alguna compuerta detecta una falla crítica, la tarea lanza de inmediato una excepción `AirflowFailException`. Esto interrumpe el DAG al instante, deja la tarea visiblemente en rojo en Airflow y **bloquea la carga al Data Warehouse**, garantizando que los tableros nunca se contaminen."*

---

### 📌 Diapositiva 5: Modelo Dimensional en Estrella (PostgreSQL 16)
* **Expositor:** **Manrique** (~1 minuto)
* **Acción en pantalla:** Avanzar a Slide 5. Explicar las 3 columnas: dimensiones base, geografía enriquecida y tabla de hechos.
* **Discurso exacto:**
> *"El almacenamiento analítico se estructuró en **PostgreSQL 16** mediante un modelo en estrella optimizado para consultas de alto rendimiento.
>
> El modelo se compone de 5 dimensiones y una tabla de hechos central:
> - `dim_tiempo`: Un calendario denso de más de 6.000 días que permite calcular crecimientos interanuales.
> - `dim_victima`: Desglosa el género y ciclo vital con una bandera explícita para menores de edad.
> - `dim_medio`: Armas y mecanismos agresores con el flag de letalidad `es_arma_letal`.
> - `dim_geografia`: La dimensión estrella del proyecto. Contiene los 1.113 municipios del país enriquecidos con su categoría municipal y el cálculo directo del déficit de comisarías bajo la Ley 2126.
> - `dim_comisaria`: Directorio físico outrigger con teléfonos y horarios.
>
> Y en el centro, la tabla `fact_violencia_intrafamiliar` con **701.714 filas agregadas al grano estricto** de Fecha, Municipio, Víctima y Arma. Aplicamos índices B-Tree en cada clave foránea y una restricción única compuesta, logrando que consultas complejas sobre millón y medio de registros respondan en menos de 50 milisegundos."*

---

### 📌 Diapositiva 6: Hallazgos Analíticos en Power BI (R1 a R7)
* **Expositor:** **Manrique** (~1.2 minutos)
* **Acción en pantalla:** Avanzar a Slide 6. Clic en la imagen del dashboard para abrir el **Lightbox Zoom** y mostrar la pestaña de sobrecarga.
* **Discurso exacto:**
> *"Este modelo alimenta nuestro tablero en Power BI Desktop, respondiendo los 7 requerimientos analíticos del proyecto. Destaco los 4 hallazgos más relevantes:
>
> 1. **Brecha de Género (R3):** El **77.3% de las víctimas en Colombia son mujeres** (más de 1.16 millones de casos), lo que confirma la urgencia del ODS 5.
> 2. **Concentración (R2):** El 64% de los incidentes se concentran en las capitales departamentales.
> 3. **Sobrecarga Extrema (R6):** Como ven en la gráfica ampliada, ciudades como **Cúcuta** tienen más de 20.000 víctimas históricas para una sola oficina de comisaría, lo que significa una saturación institucional absoluta.
> 4. **Prioridad 1 Urgente (R7):** Cruzamos la brecha legal con ataques letales a menores de edad. Identificamos que Cúcuta (déficit de 8 comisarías y 51 menores atacados) y Soacha (déficit de 6 comisarías y 27 menores atacados) son los municipios que el Gobierno Nacional debe intervenir con máxima prioridad estatutaria."*
* *(Cerrar el Lightbox con `Escape` y ceder la palabra a David).*

---

## 🟡 BLOQUE 3: PIPELINE STREAMING Y TIEMPO REAL (David)

### 📌 Diapositiva 7: El Pipeline de Streaming con Apache Kafka
* **Expositor:** **David** (~1.2 minutos)
* **Acción en pantalla:** Avanzar a Slide 7. Señalar las 3 tarjetas de Productor, Contrato Pydantic y Dead-Letter Queue.
* **Discurso exacto:**
> *"Gracias, Manrique. Ahora entramos al componente central de esta entrega: **el procesamiento en tiempo real con Apache Kafka**.
>
> Para qué sirve esto: el análisis de Power BI es excelente para planear el próximo año, pero ante un caso de violencia intrafamiliar grave que ocurre hoy a las 10 de la noche, las autoridades no pueden esperar al cierre del mes para enterarse.
>
> Diseñamos un pipeline de streaming robusto con tres piezas:
> 1. **Productor Inteligente:** Publica mensajes en Kafka simulando el flujo de denuncias. No envía números estáticos repetidos; modula variaciones realistas de armas letales, menores y municipios con déficit.
> 2. **Contrato de Eventos con Pydantic V2:** Es nuestra aduana perimetral. Cada mensaje en Kafka debe cumplir obligatoriamente con tener un UUID, timestamp en UTC, código DANE de 5 dígitos y cantidad de víctimas mayor a cero.
> 3. **Dead-Letter Queue (DLQ):** Si llega un mensaje malformado o corrupto, el consumidor lo detecta, lo envía a una tabla de auditoría de errores (`streaming_invalid_events_log`) y avanza el offset. **Esto evita que un mensaje dañado congele el sistema o tumbe la base de datos**."*

---

### 📌 Diapositiva 8: Las Tres Métricas de Streaming
* **Expositor:** **David** (~1 minuto)
* **Acción en pantalla:** Avanzar a Slide 8. Explicar las 3 tarjetas de métricas.
* **Discurso exacto:**
> *"La rúbrica nos exigía implementar y justificar al menos tres métricas en vivo. Nosotros diseñamos tres indicadores orientados a la toma de decisiones inmediata:
>
> - **Métrica 1 — Volumen Operativo en Vivo:** Cuenta continuamente los eventos y víctimas acumuladas en la sesión actual, separando cuántas son mujeres y cuántos son niños. Permite detectar picos inusuales de denuncias en tiempo real.
> - **Métrica 2 — Severidad Crítica y Tasa de Letalidad:** Calcula el porcentaje exacto de casos que involucran armas de fuego o cortopunzantes, y la proporción de menores afectados. Si la letalidad se dispara, el sistema activa una alerta operativa.
> - **Métrica 3 — Alerta Institucional por Ley 2126:** Cruza cada evento entrante con el catálogo municipal. Si el incidente ocurre en una ciudad que tiene déficit de comisarías, incrementa el contador de alertas y cambia el semáforo de presión a **'CRÍTICA (DÉFICIT SATURADO)'**.
>
> Estas tres métricas se actualizan de forma atómica en una tabla de PostgreSQL llamada `streaming_metrics_store` en menos de 10 milisegundos por evento."*

---

### 📌 Diapositiva 9: El Tablero en Tiempo Real (Streamlit)
* **Expositor:** **David** (~1 minuto)
* **Acción en pantalla:** Avanzar a Slide 9. Señalar la tarjeta de componentes de la interfaz y la caja de terminal.
* **Discurso exacto:**
> *"Para visualizar estas métricas construimos un tablero interactivo en **Streamlit y Plotly**.
>
> A diferencia de soluciones basadas en nubes comerciales que requieren pasarelas o licencias pagas, este dashboard es **100% reproducible y corre en local o en Docker**.
>
> - **¿Cómo se conecta?** Se conecta a PostgreSQL mediante un pool de conexiones optimizado con SQLAlchemy que verifica la salud de la base de datos.
> - **¿Cada cuánto se actualiza?** Tiene un ciclo de auto-refresco configurado en **3 segundos**, lo que permite ver cómo las tarjetas de KPIs cambian de valor y cómo el gráfico de barras municipal se reordena en vivo mientras el productor de Kafka está enviando mensajes.
> - Además, incluye una tabla con los últimos 15 eventos recibidos y un monitor que muestra si la Dead-Letter Queue ha capturado mensajes corruptos."*
* *(Ceder la palabra a Torres).*

---

## 🔴 BLOQUE 4: RESILIENCIA, INGENIERÍA Y DEMOSTRACIÓN (Torres)

### 📌 Diapositiva 10: Pruebas Reales de Resiliencia y Tolerancia a Fallos
* **Expositor:** **Torres** (~1.2 minutos)
* **Acción en pantalla:** Avanzar a Slide 10. Explicar el Experimento 1 (tarjeta roja) y el Experimento 2 (tarjeta azul).
* **Discurso exacto:**
> *"Gracias, David. Un sistema en tiempo real no sirve de nada si se cae con el primer dato erróneo o ante una micro-caída de red.
>
> Por eso, ejecutamos y documentamos los **dos experimentos obligatorios de la rúbrica**:
>
> 1. **Experimento 1 (Evento Malformado):** Inyectamos intencionalmente a Kafka un mensaje con fecha en formato de texto inválido, víctimas negativas y código DANE roto. Como ven en el registro de la consola, el validador Pydantic lo atrapó, lo envió a la Dead-Letter Queue y **el consumidor siguió funcionando perfectamente sin caerse**.
> 2. **Experimento 2 (Interrupción del Consumidor):** Apagamos el proceso del consumidor durante 15 segundos mientras el productor seguía enviando casos. Kafka retuvo los mensajes en su buffer. Al reiniciar el consumidor, este leyó desde su último offset confirmado y **procesó los 5 mensajes acumulados con 0% de pérdida de datos**, pasando el contador exactamente de 5 a 10.
>
> Ambas pruebas están automatizadas en el script `test_resilience.py` para que cualquiera pueda verificarlas."*

---

### 📌 Diapositiva 11: Decisiones de Ingeniería y Limitaciones
* **Expositor:** **Torres** (~1 minuto)
* **Acción en pantalla:** Avanzar a Slide 11. Repasar brevemente las 3 decisiones de arquitectura.
* **Discurso exacto:**
> *"Queremos destacar tres decisiones técnicas fundamentales que tomamos en el diseño:
>
> 1. **Kafka en modo KRaft (sin Zookeeper):** Usamos Apache Kafka 3.7 con protocolo Raft. Esto redujo el uso de memoria en Docker a la mitad, eliminó riesgos de 'split-brain' y acelera el despliegue.
> 2. **Pydantic V2 sobre JSON Schema:** Al estar implementado en Rust, Pydantic valida tipos y reglas complejas en microsegundos, asegurando máxima velocidad en el consumidor.
> 3. **Estrategia Dead-Letter Queue:** Preferimos aislar el dato corrupto y avanzar el offset, en lugar de implementar reintentos infinitos que congelarían la cola de eventos de toda la ciudad.
>
> Como limitación documentada: Streamlit utiliza sondeo (*polling*) cada 3 segundos hacia la base de datos en lugar de WebSockets directos, pero ofrece un balance ideal entre simplicidad operativa y latencia imperceptible para el usuario final."*

---

### 📌 Diapositiva 12: Conclusiones y Paso a la Demostración
* **Expositor:** **Torres** (~45 segundos)
* **Acción en pantalla:** Avanzar a Slide 12. Señalar el cumplimiento de la rúbrica y preparar el cambio de ventana.
* **Discurso exacto:**
> *"Para concluir: logramos integrar la analítica histórica en Power BI con el monitoreo en tiempo real en Streamlit, cumpliendo rigurosamente cada uno de los puntos de la rúbrica oficial para una calificación de 5.0.
>
> Todo el proyecto se encuentra en un repositorio nuevo e independiente, con sus Dockerfiles, pruebas y el README maestro en 12 secciones.
>
> Ahora, si le parece bien, profesor, pasamos a mostrarle en vivo el sistema funcionando en terminal y en el navegador."*

---

# 💻 PARTE 2: Protocolo de Demostración en Vivo (3 minutos)

* **Encargado de la Demo:** **Torres** (apoyado por David o el equipo).
* **Preparación previa:** Tener abiertas **3 terminales** en la carpeta `Proyecto_etl_Final` y una pestaña de navegador en `http://localhost:8501`.

```text
+-----------------------------------------------------------------------------------+
| VENTANA 1 (Navegador): Dashboard en Tiempo Real (http://localhost:8501)          |
| VENTANA 2 (Terminal): Consumidor de Kafka procesando eventos                      |
| VENTANA 3 (Terminal): Productor de Kafka emitiendo casos en vivo                  |
+-----------------------------------------------------------------------------------+
```

### Paso 1: Mostrar los servicios saludables en Docker
En la Terminal 1 ejecutas:
```bash
docker ps --filter "name=etl_final"
```
**Lo que dices:**
> *"Como ve aquí, profesor, tenemos corriendo en Docker tanto el contenedor de PostgreSQL 16 con el Data Warehouse y las tablas de streaming, como el broker de Apache Kafka 3.7 en modo KRaft, ambos en estado saludable."*

---

### Paso 2: Lanzar el Dashboard y el Consumidor
En la Terminal 1 lanzas Streamlit:
```bash
python3 -m streamlit run dashboard/app_realtime.py
```
*(Se abre automáticamente en el navegador).*

En la Terminal 2 inicias el Consumidor:
```bash
python3 streaming/consumer.py --servers localhost:9092
```
**Lo que dices:**
> *"Aquí en la Terminal 2 el consumidor se conecta al tópico `violencia-intrafamiliar-events`. Y en el navegador tenemos el dashboard de Streamlit esperando eventos con las tarjetas en su estado actual."*

---

### Paso 3: Encender el Productor y ver la magia en vivo
En la Terminal 3 ejecutas:
```bash
python3 streaming/producer.py --servers localhost:9092 --count 20 --interval 1.0
```
**Lo que dices mientras los eventos se imprimen:**
> *"Ahora el productor está emitiendo 20 casos, uno por segundo. Miren cómo la Terminal 2 valida cada mensaje con el contrato Pydantic. Y si miramos la pantalla de Streamlit, observen cómo cada 3 segundos el contador de víctimas sube, la tasa de letalidad se recalcula en vivo y el gráfico de barras resalta en rojo cuando entran incidentes en municipios con déficit de comisarías como Cúcuta o Soacha."*

---

### Paso 4: Demostrar la Resiliencia (Experimento en vivo)
Si el profesor pide ver la prueba de fallos, en la Terminal 3 ejecutas:
```bash
python3 streaming/test_resilience.py --servers localhost:9092
```
**Lo que dices:**
> *"Aquí el script acaba de inyectar un payload roto con víctimas negativas y fecha corrupta. Miren cómo la prueba pasa: el consumidor lo atrapó, lo mandó a la Dead-Letter Queue y no se cayó. Y en la segunda parte, reanudó los mensajes en cola sin perder un solo registro."*

---

# 🧠 PARTE 3: Preguntas Difíciles del Profesor y Respuestas Clave

### 1. "¿Por qué decidieron usar Kafka en modo KRaft y no Zookeeper?"
> **Respuesta:** *"Porque a partir de la versión 3.3 de Apache Kafka, KRaft es el estándar oficial de la industria. Nos permitió eliminar la necesidad de correr y monitorear un contenedor adicional de Zookeeper, reduciendo a la mitad el consumo de memoria en Docker, evitando problemas de sincronización de metadatos ('split-brain') y haciendo que el sistema levante en apenas 2 segundos."*

### 2. "¿Qué pasa si un evento llega con datos nulos o tipos incorrectos?"
> **Respuesta:** *"El consumidor lo intercepta en la compuerta perimetral mediante el modelo Pydantic `ViolenciaIntrafamiliarEvent`. Si viola el contrato, no se cae: captura un `EventContractViolationError`, serializa el payload original y el motivo del fallo, lo inserta en la tabla `streaming_invalid_events_log` (Dead-Letter Queue) y confirma el offset en Kafka para continuar procesando los siguientes eventos sin retrasar la cola."*

### 3. "¿Por qué guardan las métricas en PostgreSQL en lugar de enviar los datos directo a Streamlit?"
> **Respuesta:** *"Porque esa es la recomendación arquitectónica de la rúbrica oficial: utilizar un 'Streaming Metrics Store' relacional desacopla completamente el broker de mensajería de la capa de visualización. De esta forma, múltiples aplicaciones, dashboards o APIs pueden consultar el estado de las métricas en paralelo sin saturar las particiones de Kafka."*

### 4. "¿Por qué tienen un dashboard en Power BI y otro en Streamlit?"
> **Respuesta:** *"Porque resuelven dos problemas de negocio distintos con dos SLAs diferentes: Power BI es el dashboard histórico y gerencial (analiza 15 años de historia y 701k filas para responder R1 a R7 y planear presupuestos). Streamlit es el dashboard operativo de baja latencia (refresca cada 3 segundos para que los despachadores de emergencia detecten picos y casos letales en el momento exacto en que ocurren)."*
