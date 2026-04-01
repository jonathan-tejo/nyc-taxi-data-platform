# NYC Taxi Data Platform

Una plataforma de ingeniería de datos de nivel productivo en **Google Cloud Platform** que ingesta, procesa y sirve datos de viajes en taxi amarillo de Nueva York bajo una arquitectura medallón (bronze → silver → gold). La infraestructura es completamente reproducible mediante **Terraform**; el pipeline es orquestado por **Google Workflows**.

---

## El Problema

La Comisión de Taxis y Limusinas de Nueva York (TLC) publica mensualmente archivos Parquet con decenas de millones de registros de viajes. El objetivo es hacer que estos datos sean consultables, analíticamente confiables y listos para el negocio — con ingesta automatizada, control de calidad de datos y tablas de KPIs que sirven dashboards y analistas.

---

## Arquitectura

```
TLC Public API
     │
     ▼  (Python — ingest.py)
┌──────────────────────┐
│   GCS Raw Bucket     │  ← particionado por YYYY-MM, idempotente
│   yellow/YYYY-MM/    │
└──────────┬───────────┘
           │ BQ Load Job
           ▼
┌─────────────────────────────────────────────────────────────┐
│                      BigQuery                               │
│                                                             │
│  BRONZE (raw, tipado)  →  SILVER (limpio)  →  GOLD (KPIs)  │
│                                                             │
│  trips                    trips               kpi_daily_revenue    │
│                           dim_zones            kpi_zone_performance │
│                                                kpi_hourly_patterns  │
└─────────────────────────────────────────────────────────────┘
           ▲
           │  Google Workflows orquesta cada paso
           │  Cloud Scheduler dispara mensualmente (opcional)
```

Detalles completos de arquitectura: [docs/architecture.md](docs/architecture.md)

---

## Stack Tecnológico

| Componente | Tecnología |
|-----------|-----------|
| Infraestructura como Código | Terraform >= 1.5 |
| Proveedor cloud | Google Cloud Platform |
| Data warehouse | BigQuery |
| Almacenamiento de objetos | Cloud Storage |
| Orquestación | Google Workflows |
| Programación | Cloud Scheduler |
| Ingesta | Python 3.11 |
| Procesamiento de datos | BigQuery SQL (CTAS) |
| Observabilidad | Cloud Logging + tablas de metadatos en BigQuery |

---

## Estructura del Repositorio

```
.
├── Makefile                        # Todas las operaciones comunes
├── terraform/
│   ├── main.tf                     # Módulo raíz — conecta todos los módulos
│   ├── variables.tf
│   ├── outputs.tf
│   ├── terraform.tfvars.example    # Copiar a terraform.tfvars
│   └── modules/
│       ├── storage/                # Buckets de GCS
│       ├── bigquery/               # Datasets + tablas de metadatos
│       ├── iam/                    # Cuenta de servicio + bindings de IAM
│       └── workflows/              # Definición del workflow + Scheduler
│           └── workflow_definition.yaml
├── ingestion/
│   ├── ingest.py                   # Descarga TLC → GCS → BQ bronze
│   ├── utils.py                    # Helpers de GCS/BQ, logging estructurado
│   └── requirements.txt
├── transformations/
│   ├── silver/
│   │   ├── 01_clean_trips.sql      # Dedup + limpieza + casteo de tipos
│   │   └── 02_dim_zones.sql        # Dimensión estática de zonas
│   └── gold/
│       ├── 01_kpi_daily_revenue.sql
│       ├── 02_kpi_zone_performance.sql
│       └── 03_kpi_hourly_patterns.sql
├── quality/
│   ├── run_checks.py               # Suite de 8 controles de calidad
│   └── requirements.txt
└── docs/
    └── architecture.md
```

---

## Modelo de Datos

### Bronze — `nyc_taxi_{env}_bronze.trips`

Datos crudos de TLC cargados desde Parquet en GCS. El esquema replica exactamente la fuente TLC, más tres columnas de auditoría: `_ingested_at`, `_source_file`, `_execution_date`.

- **Partición**: MONTH sobre `tpep_pickup_datetime`
- **Cluster**: `VendorID`, `payment_type`

### Silver — `nyc_taxi_{env}_silver.trips`

Limpio y enriquecido. Transformaciones clave:
- Deduplicación por `(VendorID, pickup_datetime, dropoff_datetime)`
- Renombrado de columnas y casteo de tipos
- Campos derivados: `trip_duration_min`, `tip_rate`, `is_airport_trip`, `time_of_day_segment`
- Se excluyen filas con ubicaciones inválidas, montos negativos o duraciones fuera de rango
- Solo se conservan filas del mes correspondiente a `execution_date`

**Partición**: DAY sobre `pickup_date` | **Cluster**: `pickup_location_id`, `payment_type`

### Silver — `dim_zones`

Lookup estático de zonas TLC de NYC (265 zonas). Agrega campos derivados `borough_group` e `is_airport`.

### Tablas KPI Gold

| Tabla | Granularidad | Métricas clave |
|-------|-------|-------------|
| `kpi_daily_revenue` | Día | viajes, ingresos, tasa de propina, p50/p90/p99 ingresos, mix de pago |
| `kpi_zone_performance` | Mes × Zona | total_pickups, ingresos, ranking por borough, participación en ingresos |
| `kpi_hourly_patterns` | Mes × Día semana × Hora | índice de demanda, segmento horario, ingreso promedio |

---

## Prerequisitos

- Proyecto GCP con facturación habilitada
- `gcloud` CLI autenticado (`gcloud auth application-default login`)
- Terraform >= 1.5 ([instalar](https://developer.hashicorp.com/terraform/downloads))
- Python 3.11+
- `make` (Linux/Mac) o WSL/Git Bash (Windows)

---

## Despliegue

### 1. Habilitar APIs de GCP

```bash
export PROJECT_ID=tu-gcp-project-id
make setup-gcp PROJECT_ID=$PROJECT_ID
```

### 2. Crear bucket para el estado de Terraform

```bash
gsutil mb -p $PROJECT_ID -l us-central1 gs://${PROJECT_ID}-tf-state
```

Actualiza el bloque `backend "gcs"` en [terraform/main.tf](terraform/main.tf):

```hcl
backend "gcs" {
  bucket = "tu-proyecto-tf-state"   # ← cambiar esto
  prefix = "nyc-taxi-platform/tfstate"
}
```

### 3. Configurar variables

```bash
cp terraform/terraform.tfvars.example terraform/terraform.tfvars
# Editar terraform.tfvars con tu project_id y preferencias
```

### 4. Desplegar infraestructura

```bash
make tf-init
make tf-plan ENV=dev
make tf-apply ENV=dev
```

Esto crea:
- 3 buckets GCS (raw, staging, pipeline-logs)
- 4 datasets BigQuery (bronze, silver, gold, metadata)
- 2 tablas de metadatos (pipeline_runs, quality_checks)
- 1 tabla bronze de viajes (particionada + con cluster)
- 1 cuenta de servicio con permisos IAM mínimos
- 1 pipeline de Google Workflows

### 5. Instalar dependencias Python

```bash
make install
```

### 6. Ejecutar el pipeline

```bash
# Ingestar y procesar un mes específico
make run-pipeline DATE=2024-01

# O disparar solo el paso de ingesta
make ingest DATE=2024-01

# Ejecutar controles de calidad de forma independiente
make quality-check DATE=2024-01
```

---

## Ejemplo de Ejecución

```bash
$ make run-pipeline DATE=2024-03 ENV=dev

→ Triggering pipeline for 2024-03...

Workflow execution started: projects/my-project/locations/us-central1/workflows/nyc-taxi-pipeline-dev/executions/abc123

{
  "status": "COMPLETED",
  "execution_date": "2024-03",
  "total_trips": 3_451_208,
  "total_revenue": 52_847_331.42,
  "duration_seconds": 187
}
```

```bash
$ make quality-check DATE=2024-03

╭──────────┬──────────────────────────────────┬────────┬────────────┬──────────────┬───────────╮
│ Capa     │ Control                          │ Estado │ Filas Eval │ Filas Fallas │ % Falla   │
├──────────┼──────────────────────────────────┼────────┼────────────┼──────────────┼───────────┤
│ bronze   │ not_null_pickup_datetime         │ PASSED │  3,614,799 │            0 │     0.00% │
│ bronze   │ minimum_row_count                │ PASSED │  3,614,799 │            0 │     0.00% │
│ bronze   │ no_future_pickup_dates           │ PASSED │  3,614,799 │            0 │     0.00% │
│ silver   │ valid_location_ids               │ PASSED │  3,451,208 │           12 │     0.00% │
│ silver   │ non_negative_amounts             │ PASSED │  3,451,208 │            0 │     0.00% │
│ silver   │ trip_duration_range_1_to_300_min │ PASSED │  3,451,208 │         2,18 │     0.06% │
│ gold     │ daily_revenue_completeness       │ PASSED │         31 │            0 │     0.00% │
│ gold     │ positive_daily_revenue           │ PASSED │         31 │            0 │     0.00% │
╰──────────┴──────────────────────────────────┴────────┴────────────┴──────────────┴───────────╯

[PASS] Todos los controles pasaron (0 advertencias).
```

---

## Monitoreo y Solución de Problemas

### Historial de ejecuciones del pipeline

```bash
make pipeline-status
```

O en la Consola de GCP: **Workflows → nyc-taxi-pipeline-dev → Executions**

### Consultar logs de ejecución en BigQuery

```sql
SELECT
  execution_date,
  status,
  rows_ingested,
  duration_seconds,
  TIMESTAMP_DIFF(completed_at, run_date, SECOND) AS total_duration_s,
  error_message
FROM `tu-proyecto.nyc_taxi_dev_metadata.pipeline_runs`
ORDER BY run_date DESC
LIMIT 20;
```

### Consultar tendencias de controles de calidad

```sql
SELECT
  execution_date,
  layer,
  check_name,
  status,
  rows_failed,
  failure_rate
FROM `tu-proyecto.nyc_taxi_dev_metadata.quality_checks`
WHERE status != 'PASSED'
ORDER BY check_timestamp DESC;
```

### Problemas comunes

| Problema | Causa probable | Solución |
|-------|-------------|-----|
| `404` en descarga TLC | Mes aún no publicado | TLC publica ~2 meses después. Verificar archivos disponibles. |
| Falla de carga BQ por esquema | TLC cambió tipos de columnas | Actualizar esquema de tabla bronze en `modules/bigquery/main.tf` |
| Control de calidad falla en conteo | Ingesta parcialmente fallida | Re-ejecutar `make ingest DATE=YYYY-MM` luego `make run-pipeline` |
| Workflow sin permisos | SA falta un rol | Revisar módulo IAM, re-aplicar Terraform |

---

## Decisiones Técnicas

**¿Por qué Google Workflows en lugar de Airflow/Composer?**
Workflows es serverless, no tiene costo de infraestructura en reposo, se integra nativamente con las APIs de GCP (BigQuery, GCS) y es más simple de operar. Para un pipeline batch mensual, la sobrecarga de Composer no está justificada.

**¿Por qué CTAS para silver/gold en lugar de MERGE?**
Para cargas batch mensuales, `CREATE OR REPLACE TABLE` sobre una partición es más simple, más barato (un solo paso) y más fácil de razonar que MERGE. MERGE se reserva para el paso de deduplicación en bronze donde se necesita semántica de upsert.

**¿Por qué particionar bronze por MES pero silver por DÍA?**
Bronze replica la fuente (archivos mensuales). Silver se consulta a granularidad diaria en las construcciones gold, por lo que la partición por DÍA evita escaneos del mes completo al construir los KPIs de un solo día durante backfills.

**¿Por qué un dataset de metadatos separado?**
Separar los datos de observabilidad de los datos del negocio hace más limpia la gestión de IAM y la atribución de costos. Un rol de analista de solo lectura en gold no necesita acceso a los internos del pipeline.

---

## Mejoras Futuras

- [ ] **Integración con dbt** — reemplazar archivos SQL crudos con modelos dbt para linaje, documentación y testing
- [ ] **CLI de Backfill** — `make backfill FROM=2023-01 TO=2024-12` para cargas históricas
- [ ] **Dashboard en Looker Studio** — conectar a tablas gold para una demo pública en vivo
- [ ] **Evolución de esquema** — agregar actualización automática de esquema en BigQuery cuando TLC agrega columnas
- [ ] **Datos de taxis Green/FHV** — extender la ingesta a otros tipos de vehículos TLC
- [ ] **Capa de streaming** — agregar un camino en tiempo real via Pub/Sub + Dataflow para datos de viajes en vivo
- [ ] **Monitoreo de costos** — alertas de slots y almacenamiento de BigQuery via Cloud Monitoring

---

## Licencia

MIT
