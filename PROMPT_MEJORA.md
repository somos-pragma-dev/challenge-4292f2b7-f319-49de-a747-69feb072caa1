# Prompt para Mejorar el Codigo Base

Copia y pega el contenido del bloque de abajo en un asistente de IA (Claude, ChatGPT)
para obtener un ZIP con el proyecto completo y arrancable.

Si preferis trabajar en tu editor con un agente local (Claude Code, Cursor, Copilot), usa `AGENTS.md` en vez de este archivo: dice lo mismo pero para que escriba los archivos en disco.

## Las dos reglas que no se negocian

1. **Completa el boilerplate.** Todo lo que el proyecto necesita para compilar y arrancar: manifiesto de dependencias, punto de entrada, configuracion, capa de interfaz, y las capas del patron arquitectonico declarado. Eso es andamiaje y es tu trabajo.
2. **NO resuelvas el reto.** Los entregables de las fases son el trabajo de la persona. El hueco pedagogico se deja como esta: el proyecto arranca, pero lo que el reto pide implementar NO esta implementado.

Dicho de otra forma: si algo impide compilar, arreglalo. Si algo es logica de negocio incompleta, validaciones ausentes, un secreto hardcodeado o un patron mejorable, dejalo exactamente como esta — es lo que la persona tiene que encontrar.

## Lo que le falta a este proyecto

Esto NO lo tenes que adivinar: salio de comparar el proyecto contra la arquitectura declarada del reto y de un analisis estatico del codigo. Completalo TODO.

### Archivos que la arquitectura del reto declara y no estan

Creálos con implementacion real, en la capa que les corresponde:

- `src/utils/idempotency_manager.py`
- `tests/test_transaction_transformer.py`

### Referencias colgando en el codigo que si esta

Cada una rompe la compilacion:

- `dags/consolidate_transactions_dag.py` — `AnalyticalModelWriter.xcom_pull`: Se invoca `xcom_pull` sobre `AnalyticalModelWriter`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- `dags/consolidate_transactions_dag.py` — `AnalyticalModelWriter.xcom_push`: Se invoca `xcom_push` sobre `AnalyticalModelWriter`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- `src/transform/transaction_transformer.py` — `IdempotencyManager.get_processed_hashes`: Se invoca `get_processed_hashes` sobre `IdempotencyManager`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- `tests/test_idempotency_manager.py` — `IdempotencyManager.filter_duplicates`: Se invoca `filter_duplicates` sobre `IdempotencyManager`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- `tests/test_idempotency_manager.py` — `IdempotencyManager.mark_as_processed`: Se invoca `mark_as_processed` sobre `IdempotencyManager`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- `tests/test_idempotency_manager.py` — `IdempotencyManager.generate_idempotency_key`: Se invoca `generate_idempotency_key` sobre `IdempotencyManager`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- `tests/test_idempotency_manager.py` — `IdempotencyManager.load_state`: Se invoca `load_state` sobre `IdempotencyManager`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.
- `tests/test_idempotency_manager.py` — `IdempotencyManager.remove_duplicates_within_batch`: Se invoca `remove_duplicates_within_batch` sobre `IdempotencyManager`, pero esa clase no declara ese metodo. Agregalo con su implementacion real, o usa uno de los que si declara.

## Como saber que terminaste

```bash
pip install -r requirements.txt && pytest -q
```

Ese comando corriendo sin errores es la definicion de "listo".

---

```
## Briefing del reto (autoridad)
Este bloque manda sobre los archivos adjuntos. El stack y el rol salen de AQUÍ, no de un topic genérico ni de markdown placeholder.

### Perfil
Chapter Ciencia de Datos, Especialidad Ingeniero de Datos, Tecnología PySpark, Advanced

### Brecha de conocimiento
Construye procesos de transformacion y carga idempotentes con reglas de calidad y cuarentena de registros invalidos

### Misión / candidato
Consolidar las transacciones diarias en el modelo analitico

### Reto
- Tema: pipelines de transformacion y carga
- Seniority: advanced-l2
- Tipo: practical
- Título: Consolidación de transacciones diarias en el modelo analítico
- Tiempo estimado: 10 horas

### Fases (trabajo del HUMANO — PROHIBIDO completarlas)
No implementes estos entregables. Dejalos como hueco pedagógico. El asistente solo materializa el proyecto arrancable para que el participante pueda trabajar.
- Fase 1: Exploración del sistema y definición de reglas de calidad — objetivo: Identificar las restricciones y ambigüedades del sistema y definir las reglas de calidad para las transacciones. — entregable (NO resolver): Documento que describe las restricciones, ambigüedades y reglas de calidad definidas.
- Fase 2: Implementación de la transformación idempotente — objetivo: Implementar la transformación de las transacciones de manera idempotente y aplicar las reglas de calidad definidas. — entregable (NO resolver): Proceso de transformación idempotente que aplica las reglas de calidad y cuarentena los registros inválidos.
- Fase 3: Consolidación en el modelo analítico — objetivo: Consolidar las transacciones válidas en el modelo analítico. — entregable (NO resolver): Modelo analítico actualizado con las transacciones válidas consolidadas.

Eres un asistente experto en análisis, corrección y generación de archivos de cualquier tipo:
código fuente, documentación, hojas de cálculo, documentos Word, configuraciones, entre otros.
Voy a enviarte una cadena de texto que contiene uno o más archivos. Cada archivo está delimitado por un marcador con el siguiente formato:
// === ARCHIVO: ruta/del/archivo.extension ===
o también puede aparecer como:
## === ARCHIVO: ruta/del/archivo.extension ===
Lo que sigue al marcador puede ser:

El contenido real del archivo (código, texto, YAML, etc.)
Una descripción en lenguaje natural de lo que debe contener el archivo


TU TAREA
PASO 0 — ¿Esto es un proyecto o una carcasa?
Antes de extraer archivos, leé el Briefing (si está) y diagnosticá el adjunto.

Es CARCASA si ocurre CUALQUIERA de estas:
- No hay manifiesto de dependencias del stack del briefing (manifest.json de VTEX IO / package.json / pom.xml / build.gradle / requirements.txt / go.mod / *.tf / *.csproj, según corresponda)
- Hay un "binario" que en realidad es un comentario ("no puede ser mostrado como texto plano", placeholder .fig/.docx vacío)
- Los markdowns ya completan entregables de fases posteriores ("se implementó fade-in", lista de áreas ya resuelta)

Si es CARCASA:
- MATERIALIZÁ un proyecto que arranca en el stack del briefing (VTEX IO Store Framework, Angular, Terraform, pytest, Nest, etc.). Incluí manifiesto, punto de entrada y capa de interfaz reales.
- NO copies los markdowns de "solución" como si fueran el producto. Son ruido de generación.
- NO resuelvas las fases del briefing (están marcadas PROHIBIDO). Dejá el hueco pedagógico: el flujo existe, las microinteracciones/calidad/infra que el reto pide NO están hechas.
- Después seguí al PASO 5 (ZIP).

Si es un proyecto REAL (manifiesto + código que compila o arranca):
- Seguí PASO 1 en adelante. 🔴 compilación sí. 🟡 pedagógico no.

PASO 1 — Detección y extracción
Identifica todos los archivos presentes en la cadena. Para cada archivo extrae:

Su ruta completa (ej: src/main/java/com/pragma/Service.java)
Su contenido o descripción

PASO 2 — Clasificación por tipo
Clasifica cada archivo en una de estas categorías:
A) Código fuente (Java, Python, TypeScript, JavaScript, Kotlin, etc.)
B) Configuración / documentación (YAML, properties, Markdown, JSON, txt, etc.)
C) Excel (.xlsx, .xls, .csv)
D) Word (.docx, .doc)
E) Otro tipo de archivo binario o especial
PASO 3 — Clasificación de errores en código fuente

Objetivo prioritario: que el proyecto compile. No corrijas flujo de negocio ni lógica funcional.

Antes de modificar cualquier archivo de código fuente, clasifica cada problema encontrado en una de estas dos categorías:
🔴 ERROR DE COMPILACIÓN — corregir siempre
Son errores que impiden que el proyecto arranque, sin valor pedagógico:

Import faltante o incorrecto
Clase, método o variable referenciada que no existe en ningún archivo del proyecto
Error de sintaxis
Anotación con atributos inválidos
Dependencia ausente en pom.xml, package.json, etc.
Archivo referenciado que no existe y debe ser creado con implementación mínima

→ CORREGIR estos errores.
🟡 PROBLEMA FUNCIONAL O DE CALIDAD — preservar siempre
Son problemas que no impiden compilar. Pueden ser intencionales para el aprendizaje:

Clave secreta hardcodeada ("secret", "password123")
API deprecada que funciona pero tiene reemplazo moderno
Lógica de negocio incorrecta o incompleta
Código redundante o de baja legibilidad
Falta de validaciones en flujo de negocio
Patrones de diseño incorrectos pero funcionales
Concurrencia no segura
Configuración funcional pero no óptima

→ PRESERVAR tal cual. No corregir, no mejorar, no comentar.
PASO 4 — Procesamiento según tipo de archivo
Tipo A — Código fuente
Aplica únicamente las correcciones clasificadas como 🔴 ERROR DE COMPILACIÓN.
No alteres ningún elemento clasificado como 🟡 PROBLEMA FUNCIONAL O DE CALIDAD.
Si falta un archivo referenciado, créalo con la implementación mínima necesaria para compilar.
Tipo B — Configuración / documentación
Extrae el contenido tal cual, sin modificaciones salvo errores evidentes de sintaxis
(ej: YAML mal indentado).
Tipo C — Excel (.xlsx)
Si viene con contenido real, genera el archivo respetando ese contenido.
Si viene con descripción en lenguaje natural, genera un archivo Excel funcional con:

Fila de encabezados en negrita con color de fondo distintivo
Columnas con ancho ajustado al contenido
Tipos de dato correctos por columna
Validaciones si la descripción lo indica
Hojas nombradas descriptivamente si hay más de una
Filas de ejemplo si no hay datos reales

Tipo D — Word (.docx)
Si viene con contenido real, genera el archivo respetando ese contenido.
Si viene con descripción en lenguaje natural, genera un documento Word funcional con:

Estilos de título (Título 1, Título 2) para jerarquía de secciones
Fuente legible (Calibri o equivalente), tamaño 11-12pt para cuerpo
Márgenes estándar
Tabla de contenido si tiene múltiples secciones
Tablas con encabezados en negrita si aplica

Tipo E — Otro
Genera el archivo con el contenido o estructura más apropiada según la descripción.
PASO 5 — Exportación en ZIP
Empaqueta todos los archivos en un único archivo ZIP descargable respetando exactamente
la estructura de rutas indicada por los marcadores.
El ZIP debe incluir:

Archivos de código con únicamente los errores de compilación corregidos
Archivos de configuración y documentación sin cambios
Archivos nuevos creados para resolver dependencias de compilación faltantes
Archivos Excel y Word generados desde descripción

IMPORTANTE: El ZIP debe estar listo para descargar al finalizar. No preguntes si el usuario
quiere generarlo. Simplemente genera el archivo y proporciona el enlace de descarga; No debes desplegar en el chat el resumen de lo que arreglaste al Zip, solo entregalo.

REGLAS IMPORTANTES

No omitas ningún archivo aunque no tenga errores ni modificaciones
Respeta los nombres y rutas exactas indicadas por los marcadores
Si un archivo no tiene marcador claro, infiere el nombre desde su contenido
Si la cadena contiene solo documentación, placeholders o binarios fake, NO la reproduzcas:
aplicá PASO 0 (materializar el proyecto del briefing). Reproducir la carcasa es un fallo.
No agregues texto después del enlace de descarga del ZIP
No preguntes si el usuario quiere el ZIP: simplemente generalo siempre
Si detectas que falta un archivo de configuración necesario para compilar
(pom.xml, package.json, requirements.txt, build.gradle, etc.), créalo e inclúyelo
inferiendo su contenido desde los imports y frameworks detectados en el código
Nunca corrijas problemas 🟡 aunque parezcan obvios o fáciles de mejorar.
El participante que recibirá este proyecto los debe encontrar y resolver él mismo.


INPUT
Aquí está la cadena con los archivos:

// === ARCHIVO: pyproject.toml ===
[project]
name = "transaction-consolidation-pipeline"
version = "1.0.0"
description = "ETL pipeline para consolidación diaria de transacciones en modelo analítico"
requires-python = ">=3.13"
authors = [
    {name = "Data Engineering Team", email = "data-eng@empresa.com"}
]
keywords = ["etl", "pyspark", "airflow", "delta-lake", "data-quality"]
classifiers = [
    "Development Status :: 4 - Beta",
    "Intended Audience :: Developers",
    "Programming Language :: Python :: 3.13",
    "Topic :: Software Development :: Libraries :: Python Modules",
]

[project.optional-dependencies]
dev = [
    "pytest>=8.1.1",
    "pytest-cov>=4.1.0",
    "black>=24.2.0",
    "ruff>=0.3.0",
    "mypy>=1.8.0",
]
test = [
    "pytest>=8.1.1",
    "pandas>=2.2.2",
    "pyarrow>=14.0.2",
]

[project.urls]
Homepage = "https://github.com/empresa/transaction-consolidation"
Documentation = "https://docs.empresa.com/transaction-consolidation"
Repository = "https://github.com/empresa/transaction-consolidation"

[project.scripts]
consolidate-transactions = "src.main:main"
validate-transactions = "src.utils.validator:main"

[build-system]
requires = ["setuptools>=68.0", "wheel"]
build-backend = "setuptools.build_meta"

[tool.setuptools]
packages = ["src"]
package-dir = {"" = "."}

[tool.setuptools.package-data]
src = ["schemas/*.json", "**/*.yaml"]

[tool.pytest.ini_options]
minversion = "8.0"
addopts = "-ra -q --strict-markers --cov=src --cov-report=term-missing:skip-covered"
testpaths = ["tests"]
python_files = "test_*.py"
python_classes = "Test*"
python_functions = "test_*"
markers = [
    "unit: Unit tests",
    "integration: Integration tests",
    "quality: Data quality tests",
    "slow: Tests that take more than 10 seconds",
]

[tool.coverage.run]
source = ["src"]
omit = [
    "tests/*",
    "**/__pycache__/*",
    "**/site-packages/*",
]

[tool.coverage.report]
exclude_lines = [
    "pragma: no cover",
    "def __repr__",
    "raise AssertionError",
    "raise NotImplementedError",
    "if __name__ == .__main__.:",
    "if TYPE_CHECKING:",
]

[tool.black]
line-length = 100
target-version = ["py313"]
include = '\.pyi?$'
extend-exclude = '''
/(
  \.git
  | \.venv
  | build
  | dist
)/
'''

[tool.ruff]
line-length = 100
target-version = "py313"
select = [
    "E",   # pycodestyle errors
    "W",   # pycodestyle warnings
    "F",   # pyflakes
    "I",   # isort
    "B",   # flake8-bugbear
    "C4",  # flake8-comprehensions
    "UP",  # pyupgrade
]
ignore = [
    "E501",  # line too long (handled by black)
    "B008",  # do not perform function calls in argument defaults
]

[tool.mypy]
python_version = "3.13"
warn_return_any = true
warn_unused_configs = true
disallow_untyped_defs = false
disallow_incomplete_defs = false
check_untyped_defs = true
no_implicit_optional = true
warn_redundant_casts = true
warn_unused_ignores = true
warn_no_return = true
strict_equality = true

[tool.pyspark]
spark_version = "3.5.0"

[tool.delta]
engine = "rust"

[tool.airflow]
version = "2.9.0"
dag_folder = "dags"

[[tool.hatch.metadata.overrides]]
dependencies = [
    "pyspark==3.5.0",
    "apache-airflow==2.9.0",
    "delta-rs==0.17.0",
    "pyyaml==6.0.1",
    "pandas==2.2.2",
    "pyarrow==14.0.2",
]

[dependency-groups]
main = [
    "pyspark>=3.5.0",
    "apache-airflow>=2.9.0",
    "delta-rs>=0.17.0",
    "pyyaml>=6.0.1",
    "pandas>=2.2.2",
    "pyarrow>=14.0.2",
]
test = [
    "pytest>=8.1.1",
    "pytest-cov>=4.1.0",
]

// === ARCHIVO: src/schemas/transaction_schema.json ===
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Transaction Schema for Analytical Model",
  "description": "Schema definition for financial transactions from credit originator, risk bureau, and accounting consolidator",
  "type": "object",
  "required": [
    "transaction_id",
    "transaction_date",
    "transaction_type",
    "amount",
    "currency",
    "originator_id",
    "originator_name",
    "risk_score",
    "accounting_consolidator_id",
    "consolidation_date",
    "created_at",
    "updated_at",
    "processing_status"
  ],
  "properties": {
    "transaction_id": {
      "type": "string",
      "pattern": "^[A-Z0-9]{8,32}$",
      "description": "Unique identifier for the transaction, used as primary key for idempotency",
      "examples": ["TXN20240315000001", "CRD987654321ABC"],
      "maxLength": 32
    },
    "transaction_date": {
      "type": "string",
      "format": "date",
      "description": "Date when the transaction was originally created at the originator",
      "minimum": "2020-01-01",
      "maximum": "2030-12-31"
    },
    "transaction_timestamp": {
      "type": "string",
      "format": "date-time",
      "description": "Full timestamp with timezone for precise transaction timing"
    },
    "transaction_type": {
      "type": "string",
      "enum": [
        "CREDIT_DISBURSEMENT",
        "CREDIT_PAYMENT",
        "REFUND",
        "FEE_CHARGE",
        "INTEREST_ACCRUAL",
        "PENALTY_CHARGE",
        "ADJUSTMENT",
        "WRITE_OFF"
      ],
      "description": "Type of financial transaction being recorded"
    },
    "amount": {
      "type": "number",
      "minimum": -999999999.99,
      "maximum": 999999999.99,
      "multipleOf": 0.01,
      "description": "Transaction amount in specified currency"
    },
    "currency": {
      "type": "string",
      "enum": ["USD", "EUR", "GBP", "MXN", "CAD", "AUD"],
      "description": "ISO 4217 currency code",
      "default": "USD"
    },
    "originator_id": {
      "type": "string",
      "pattern": "^[A-Z]{3}[0-9]{4,10}$",
      "description": "Unique identifier for the credit originator entity",
      "minLength": 7,
      "maxLength": 14
    },
    "originator_name": {
      "type": "string",
      "minLength": 2,
      "maxLength": 200,
      "description": "Legal name of the credit originator"
    },
    "originator_category": {
      "type": "string",
      "enum": [
        "BANK",
        "CREDIT_UNION",
        "MICROFINANCE",
        "FINTECH",
        "CONSUMER_FINANCE"
      ],
      "description": "Category classification of the originator"
    },
    "risk_bureau_id": {
      "type": "string",
      "pattern": "^[A-Z0-9]{6,20}$",
      "description": "Identifier from the risk bureau service"
    },
    "risk_score": {
      "type": "integer",
      "minimum": 0,
      "maximum": 1000,
      "description": "Risk score from bureau, used for quality validation"
    },
    "risk_category": {
      "type": "string",
      "enum": ["LOW", "MEDIUM", "HIGH", "VERY_HIGH", "DEFAULT"],
      "description": "Categorized risk level derived from score"
    },
    "dti_ratio": {
      "type": "number",
      "minimum": 0.0,
      "maximum": 10.0,
      "multipleOf": 0.001,
      "description": "Debt-to-income ratio from risk assessment"
    },
    "accounting_consolidator_id": {
      "type": "string",
      "pattern": "^ACC[0-9]{6,12}$",
      "description": "Identifier for accounting consolidation record"
    },
    "consolidation_date": {
      "type": "string",
      "format": "date",
      "description": "Date when transaction was consolidated in accounting"
    },
    "accounting_code": {
      "type": "string",
      "pattern": "^[0-9]{4,8}$",
      "description": "General ledger account code"
    },
    "cost_center": {
      "type": "string",
      "pattern": "^CC[0-9]{3,6}$",
      "description": "Cost center for accounting allocation"
    },
    "created_at": {
      "type": "string",
      "format": "date-time",
      "description": "Timestamp when record was first created in the system"
    },
    "updated_at": {
      "type": "string",
      "format": "date-time",
      "description": "Timestamp of last modification"
    },
    "processing_status": {
      "type": "string",
      "enum": [
        "PENDING",
        "VALIDATED",
        "APPROVED",
        "REJECTED",
        "QUARANTINED",
        "PROCESSED"
      ],
      "description": "Current processing state in the pipeline"
    },
    "validation_errors": {
      "type": "array",
      "items": {
        "type": "object",
        "required": ["rule", "field", "message"],
        "properties": {
          "rule": {
            "type": "string",
            "description": "Identifier of the validation rule that failed"
          },
          "field": {
            "type": "string",
            "description": "Field name that triggered the validation error"
          },
          "message": {
            "type": "string",
            "description": "Human-readable error message"
          },
          "severity": {
            "type": "string",
            "enum": ["ERROR", "WARNING", "INFO"]
          },
          "timestamp": {
            "type": "string",
            "format": "date-time"
          }
        }
      },
      "description": "List of validation errors if transaction was quarantined"
    },
    "metadata": {
      "type": "object",
      "description": "Additional metadata for audit and lineage",
      "properties": {
        "source_system": {
          "type": "string",
          "enum": ["CREDIT_ORIGINATOR", "RISK_BUREAU", "ACCOUNTING_CONSOLIDATOR", "ETL_PIPELINE"]
        },
        "batch_id": {
          "type": "string",
          "pattern": "^BATCH[0-9]{8,16}$"
        },
        "pipeline_version": {
          "type": "string",
          "pattern": "^[0-9]+\\.[0-9]+\\.[0-9]+$"
        },
        "environment": {
          "type": "string",
          "enum": ["dev", "staging", "prod"]
        },
        "partition_date": {
          "type": "string",
          "format": "date",
          "description": "Date used for partitioning in analytical model"
        },
        "retry_count": {
          "type": "integer",
          "minimum": 0,
          "maximum": 5
        },
        "processing_time_ms": {
          "type": "integer",
          "minimum": 0
        }
      }
    }
  },
  "additionalProperties": false,
  "$defs": {
    "transaction_date_range": {
      "type": "object",
      "required": ["start", "end"],
      "properties": {
        "start": {"type": "string", "format": "date"},
        "end": {"type": "string", "format": "date"}
      }
    },
    "amount_bounds": {
      "type": "object",
      "properties": {
        "min_amount": {"type": "number"},
        "max_amount": {"type": "number"},
        "currency": {"type": "string"}
      }
    }
  }
}

// === ARCHIVO: dags/consolidate_transactions_dag.py ===
"""
DAG de Airflow para consolidación diaria de transacciones.
Orquesta las etapas de extracción, transformación y carga con manejo de idempotencia.
"""

from datetime import datetime, timedelta
from typing import Any

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.models import Variable

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _get_config() -> dict:
    """Carga configuración desde Variable de Airflow o archivo YAML."""
    import yaml
    
    config_path = Variable.get("pipeline_config_path", default_var="/opt/airflow/conf/config.yaml")
    try:
        with open(config_path, 'r') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.warning(f"Config file not found at {config_path}, using defaults")
        return {
            "sources": {
                "credit_originator": {"path": "/data/raw/credit_originator"},
                "risk_bureau": {"path": "/data/raw/risk_bureau"},
                "accounting_consolidator": {"path": "/data/raw/accounting"}
            },
            "sink": {
                "path": "/data/analytical/model",
                "partition_by": "transaction_date"
            },
            "quarantine": {
                "path": "/data/quarantine"
            }
        }


def _extract_source(source_name: str, execution_date: str, **context: Any) -> dict:
    """
    Extrae datos de una fuente específica.
    Utiliza el módulo reader correspondiente según el origen.
    """
    from src.extract.credit_originator_reader import CreditOriginatorReader
    from src.extract.risk_bureau_reader import RiskBureauReader
    from src.extract.accounting_consolidator_reader import AccountingConsolidatorReader
    
    config = _get_config()
    source_config = config["sources"].get(source_name)
    
    if not source_config:
        raise ValueError(f"Configuration not found for source: {source_name}")
    
    logger.info(f"Starting extraction from {source_name} for date {execution_date}")
    
    readers = {
        "credit_originator": CreditOriginatorReader,
        "risk_bureau": RiskBureauReader,
        "accounting_consolidator": AccountingConsolidatorReader
    }
    
    reader_class = readers.get(source_name)
    if not reader_class:
        raise ValueError(f"Unknown source: {source_name}")
    
    reader = reader_class(source_config["path"])
    df = reader.read(execution_date)
    
    row_count = df.count() if df else 0
    logger.info(f"Extracted {row_count} records from {source_name}")
    
    context['ti'].xcom_push(key=f'{source_name}_row_count', value=row_count)
    context['ti'].xcom_push(key=f'{source_name}_data', value=df.toPandas().to_dict())
    
    return {"source": source_name, "row_count": row_count, "execution_date": execution_date}


def _transform_transactions(**context: Any) -> dict:
    """
    Transforma las transacciones extraídas.
    Aplica reglas de calidad y maneja cuarentena de registros inválidos.
    """
    from src.transform.transaction_transformer import TransactionTransformer
    from src.transform.quality_rules import QualityRules
    
    ti = context['ti']
    
    credit_data = ti.xcom_pull(key='credit_originator_data', task_ids='extract.credit_originator_extract')
    risk_data = ti.xcom_pull(key='risk_bureau_data', task_ids='extract.risk_bureau_extract')
    accounting_data = ti.xcom_pull(key='accounting_consolidator_data', task_ids='extract.accounting_extract')
    
    import pandas as pd
    
    credit_df = pd.DataFrame(credit_data) if credit_data else pd.DataFrame()
    risk_df = pd.DataFrame(risk_data) if risk_data else pd.DataFrame()
    accounting_df = pd.DataFrame(accounting_data) if accounting_data else pd.DataFrame()
    
    logger.info(f"Starting transformation with {len(credit_df)} credit, {len(risk_df)} risk, {len(accounting_df)} accounting records")
    
    transformer = TransactionTransformer()
    
    merged_df = transformer.merge_sources(credit_df, risk_df, accounting_df)
    
    quality_rules = QualityRules()
    valid_df, quarantine_df = quality_rules.apply_rules(merged_df)
    
    valid_count = len(valid_df)
    quarantine_count = len(quarantine_df)
    
    logger.info(f"Transformation complete: {valid_count} valid, {quarantine_count} quarantined")
    
    ti.xcom_push(key='valid_transactions', value=valid_df.to_dict())
    ti.xcom_push(key='quarantined_transactions', value=quarantine_df.to_dict())
    
    return {"valid_count": valid_count, "quarantine_count": quarantine_count}


def _load_to_analytical_model(**context: Any) -> dict:
    """
    Carga las transacciones válidas al modelo analítico.
    Utiliza Delta Lake para garantizar consistencia y particiona por fecha.
    """
    from src.load.analytical_model_writer import AnalyticalModelWriter
    from src.utils.idempotency_manager import IdempotencyManager
    
    ti = context['ti']
    execution_date = context['execution_date']
    
    valid_data = ti.xcom_pull(key='valid_transactions', task_ids='transform.apply_quality_rules')
    
    import pandas as pd
    valid_df = pd.DataFrame(valid_data)
    
    config = _get_config()
    sink_config = config["sink"]
    
    logger.info(f"Loading {len(valid_df)} transactions to analytical model")
    
    idempotency = IdempotencyManager()
    writer = AnalyticalModelWriter(sink_config["path"])
    
    if idempotency.check_run(execution_date):
        logger.info(f"Run for {execution_date} already processed, skipping load")
        return {"status": "skipped", "row_count": 0}
    
    partition_col = sink_config.get("partition_by", "transaction_date")
    writer.write(valid_df, partition_column=partition_col, mode="overwrite")
    
    idempotency.mark_run(execution_date, len(valid_df))
    
    logger.info(f"Successfully loaded {len(valid_df)} transactions")
    
    return {"status": "completed", "row_count": len(valid_df)}


def _load_to_quarantine(**context: Any) -> dict:
    """
    Carga los registros en cuarentena al bucket de quarantine.
    Mantiene metadata de error para auditoría.
    """
    from src.load.analytical_model_writer import AnalyticalModelWriter
    
    ti = context['ti']
    execution_date = context['execution_date']
    
    quarantine_data = ti.xcom_pull(key='quarantined_transactions', task_ids='transform.apply_quality_rules')
    
    import pandas as pd
    quarantine_df = pd.DataFrame(quarantine_data)
    
    if quarantine_df.empty:
        logger.info("No quarantined records to load")
        return {"status": "no_records", "row_count": 0}
    
    config = _get_config()
    quarantine_path = config["quarantine"]["path"]
    
    logger.info(f"Loading {len(quarantine_df)} quarantined records")
    
    writer = AnalyticalModelWriter(quarantine_path)
    quarantine_df['quarantine_date'] = execution_date
    quarantine_df['quarantine_timestamp'] = datetime.now().isoformat()
    
    writer.write(quarantine_df, partition_column="quarantine_date", mode="append")
    
    logger.info(f"Quarantine load complete")
    
    return {"status": "completed", "row_count": len(quarantine_df)}


def _check_data_quality(**context: Any) -> str:
    """
    Verifica que los datos extraídos cumplen con umbrales mínimos de calidad.
    Retorna 'proceed' si pasa la validación, 'handle_quality_alert' si no.
    """
    ti = context['ti']
    
    credit_count = ti.xcom_pull(key='credit_originator_row_count', task_ids='extract.credit_originator_extract')
    risk_count = ti.xcom_pull(key='risk_bureau_row_count', task_ids='extract.risk_bureau_extract')
    accounting_count = ti.xcom_pull(key='accounting_consolidator_row_count', task_ids='extract.accounting_extract')
    
    min_expected = int(Variable.get("min_expected_records", default_var="1000"))
    
    logger.info(f"Quality check: credit={credit_count}, risk={risk_count}, accounting={accounting_count}")
    
    if credit_count and credit_count < min_expected:
        logger.warning(f"Credit records {credit_count} below threshold {min_expected}")
        return 'handle_quality_alert'
    
    if risk_count and risk_count < min_expected:
        logger.warning(f"Risk records {risk_count} below threshold {min_expected}")
        return 'handle_quality_alert'
    
    if accounting_count and accounting_count < min_expected:
        logger.warning(f"Accounting records {accounting_count} below threshold {min_expected}")
        return 'handle_quality_alert'
    
    return 'proceed'


def _handle_quality_alert(**context: Any) -> None:
    """
    Maneja alertas de calidad de datos.
    Envía notificación y continúa con procesamiento.
    """
    logger.error("Data quality alert triggered - below minimum threshold")
    # Here you would integrate with monitoring/alerting system
    pass


default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

with DAG(
    dag_id='consolidate_transactions',
    default_args=default_args,
    description='DAG para consolidación diaria de transacciones en modelo analítico',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=['etl', 'transactions', 'consolidation', 'data-quality'],
    params={
        'execution_date': '{{ ds }}',
        'min_expected_records': 1000
    }
) as dag:
    
    start = EmptyOperator(task_id='start')
    
    with TaskGroup('extract') as extract_group:
        credit_extract = PythonOperator(
            task_id='credit_originator_extract',
            python_callable=_extract_source,
            op_kwargs={
                'source_name': 'credit_originator',
                'execution_date': '{{ ds }}'
            },
            provide_context=True,
        )
        
        risk_extract = PythonOperator(
            task_id='risk_bureau_extract',
            python_callable=_extract_source,
            op_kwargs={
                'source_name': 'risk_bureau',
                'execution_date': '{{ ds }}'
            },
            provide_context=True,
        )
        
        accounting_extract = PythonOperator(
            task_id='accounting_extract',
            python_callable=_extract_source,
            op_kwargs={
                'source_name': 'accounting_consolidator',
                'execution_date': '{{ ds }}'
            },
            provide_context=True,
        )
    
    quality_check = BranchPythonOperator(
        task_id='check_data_quality',
        python_callable=_check_data_quality,
        provide_context=True,
    )
    
    quality_alert = PythonOperator(
        task_id='handle_quality_alert',
        python_callable=_handle_quality_alert,
        provide_context=True,
    )
    
    with TaskGroup('transform') as transform_group:
        apply_quality_rules = PythonOperator(
            task_id='apply_quality_rules',
            python_callable=_transform_transactions,
            provide_context=True,
        )
    
    with TaskGroup('load') as load_group:
        load_analytical = PythonOperator(
            task_id='load_to_analytical',
            python_callable=_load_to_analytical_model,
            provide_context=True,
        )
        
        load_quarantine = PythonOperator(
            task_id='load_quarantine',
            python_callable=_load_to_quarantine,
            provide_context=True,
        )
    
    end = EmptyOperator(task_id='end', trigger_rule='none_failed_or_skipped')
    
    start >> extract_group >> quality_check
    quality_check >> quality_alert >> transform_group
    quality_check >> transform_group
    transform_group >> load_group >> end


// === ARCHIVO: src/extract/credit_originator_reader.py ===
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, when, coalesce
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

logger = logging.getLogger(__name__)


class CreditOriginatorReader:
    """Lector de transacciones desde el originador de créditos."""

    def __init__(self, spark: SparkSession, source_path: str, batch_date: str):
        self.spark = spark
        self.source_path = source_path
        self.batch_date = batch_date
        self._schema = self._build_schema()

    def _build_schema(self) -> StructType:
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("credit_id", StringType(), False),
            StructField("customer_id", StringType(), False),
            StructField("transaction_type", StringType(), False),
            StructField("amount", DoubleType(), False),
            StructField("currency", StringType(), True),
            StructField("transaction_timestamp", StringType(), False),
            StructField("status", StringType(), True),
            StructField("originator_code", StringType(), True),
            StructField("channel", StringType(), True),
            StructField("reference_number", StringType(), True),
        ])

    def read(self) -> DataFrame:
        logger.info(f"Iniciando lectura de transacciones del originador para fecha {self.batch_date}")
        try:
            df = self.spark.read.format("parquet").schema(self._schema).load(self.source_path)
            df = self._apply_transformations(df)
            self._validate_schema_compliance(df)
            logger.info(f"Lectura completada: {df.count()} transacciones procesadas")
            return df
        except Exception as e:
            logger.error(f"Error al leer transacciones del originador: {str(e)}")
            raise

    def _apply_transformations(self, df: DataFrame) -> DataFrame:
        df = df.withColumn("transaction_timestamp", 
                          to_timestamp(col("transaction_timestamp"), "yyyy-MM-dd'T'HH:mm:ss"))
        df = df.withColumn("ingestion_date", lit(datetime.now()))
        df = df.withColumn("source_system", lit("CREDIT_ORIGINATOR"))
        df = df.withColumn("currency", coalesce(col("currency"), lit("USD")))
        df = df.withColumn("status", coalesce(col("status"), lit("PENDING")))
        df = df.withColumn("amount_usd", 
                          when(col("currency") == "USD", col("amount"))
                          .otherwise(col("amount") * 1.0))
        return df

    def _validate_schema_compliance(self, df: DataFrame) -> None:
        required_columns = ["transaction_id", "credit_id", "customer_id", "amount"]
        actual_columns = df.columns
        missing_columns = [c for c in required_columns if c not in actual_columns]
        if missing_columns:
            raise ValueError(f"Columnas requeridas faltantes en el origen: {missing_columns}")
        null_counts = df.select(required_columns).summary("count").collect()
        logger.info(f"Validación de schema completada para {self.batch_date}")

    def read_with_retry(self, max_retries: int = 3) -> Optional[DataFrame]:
        for attempt in range(max_retries):
            try:
                return self.read()
            except Exception as e:
                if attempt == max_retries - 1:
                    logger.error(f"Falló después de {max_retries} intentos: {str(e)}")
                    raise
                logger.warning(f"Intento {attempt + 1} fallido, reintentando en 5 segundos...")
        return None

    def get_transaction_summary(self, df: DataFrame) -> Dict[str, Any]:
        total = df.count()
        amount_sum = df.agg({"amount": "sum"}).collect()[0][0] or 0.0
        status_dist = df.groupBy("status").count().collect()
        return {
            "total_transactions": total,
            "total_amount": amount_sum,
            "by_status": {row["status"]: row["count"] for row in status_dist},
            "batch_date": self.batch_date,
            "source": "CREDIT_ORIGINATOR"
        }


def create_reader(spark: SparkSession, config: Dict[str, Any], batch_date: str) -> CreditOriginatorReader:
    source_path = config.get("credit_originator", {}).get("path", "/data/raw/credit_originator")
    return CreditOriginatorReader(spark, source_path, batch_date)


// === ARCHIVO: src/extract/risk_bureau_reader.py ===
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, coalesce, md5, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

logger = logging.getLogger(__name__)

RISK_SCHEMA_VERSION = "2.1"


class RiskBureauReader:
    """Lector de datos desde el buró de riesgos con validación de schema."""

    def __init__(self, spark: SparkSession, source_path: str, batch_date: str):
        self.spark = spark
        self.source_path = source_path
        self.batch_date = batch_date
        self._expected_schema = self._define_expected_schema()

    def _define_expected_schema(self) -> StructType:
        return StructType([
            StructField("customer_id", StringType(), False),
            StructField("risk_score", IntegerType(), True),
            StructField("risk_category", StringType(), True),
            StructField("credit_limit", DoubleType(), True),
            StructField("utilized_credit", DoubleType(), True),
            StructField("delinquency_days", IntegerType(), True),
            StructField("last_evaluation_date", StringType(), True),
            StructField("bureau_report_date", StringType(), True),
            StructField("inquiries_count", IntegerType(), True),
            StructField("public_records_count", IntegerType(), True),
            StructField("account_count", IntegerType(), True),
        ])

    def read(self) -> DataFrame:
        logger.info(f"Iniciando lectura del buró de riesgos para fecha {self.batch_date}")
        try:
            df = self.spark.read.format("parquet").load(self.source_path)
            df = self._validate_and_enrich_schema(df)
            logger.info(f"Lectura del buró completada: {df.count()} registros de riesgo")
            return df
        except Exception as e:
            logger.error(f"Error al leer datos del buró de riesgos: {str(e)}")
            raise

    def _validate_and_enrich_schema(self, df: DataFrame) -> DataFrame:
        actual_columns = set(df.columns)
        expected_columns = set(f.name for f in self._expected_schema.fields)
        missing = expected_columns - actual_columns
        if missing:
            logger.warning(f"Columnas faltantes en datos del buró: {missing}")
            for col_name in missing:
                field = next((f for f in self._expected_schema.fields if f.name == col_name), None)
                if field:
                    df = df.withColumn(col_name, lit(None).cast(field.dataType))
        df = df.withColumn("ingestion_timestamp", to_timestamp(lit(datetime.now())))
        df = df.withColumn("schema_version", lit(RISK_SCHEMA_VERSION))
        df = df.withColumn("customer_risk_hash", 
                          md5(concat_ws("-", col("customer_id"), col("bureau_report_date"))))
        df = df.withColumn("credit_utilization_pct", 
                          (coalesce(col("utilized_credit"), lit(0.0)) / 
                           coalesce(col("credit_limit"), lit(1.0))) * 100)
        return df

    def validate_data_quality(self, df: DataFrame) -> Dict[str, Any]:
        total = df.count()
        valid_risk_scores = df.filter(col("risk_score").isNotNull() & 
                                      (col("risk_score") >= 0) & 
                                      (col("risk_score") <= 1000)).count()
        valid_dates = df.filter(col("bureau_report_date").isNotNull()).count()
        return {
            "total_records": total,
            "valid_risk_scores": valid_risk_scores,
            "valid_dates": valid_dates,
            "schema_version": RISK_SCHEMA_VERSION,
            "quality_score": (valid_risk_scores / total * 100) if total > 0 else 0
        }

    def get_risk_summary(self, df: DataFrame) -> Dict[str, Any]:
        summary = df.agg(
            {"risk_score": "avg", "credit_limit": "sum", "utilized_credit": "sum"}
        ).collect()[0]
        risk_dist = df.groupBy("risk_category").count().collect()
        return {
            "avg_risk_score": summary["avg(risk_score)"] or 0,
            "total_credit_limit": summary["sum(credit_limit)"] or 0.0,
            "total_utilized": summary["sum(utilized_credit)"] or 0.0,
            "by_risk_category": {row["risk_category"]: row["count"] for row in risk_dist},
            "batch_date": self.batch_date,
            "source": "RISK_BUREAU"
        }

    def read_with_date_filter(self, min_date: Optional[str] = None) -> DataFrame:
        df = self.read()
        if min_date:
            df = df.filter(col("bureau_report_date") >= min_date)
        return df


def create_risk_reader(spark: SparkSession, config: Dict[str, Any], batch_date: str) -> RiskBureauReader:
    source_path = config.get("risk_bureau", {}).get("path", "/data/raw/risk_bureau")
    return RiskBureauReader(spark, source_path, batch_date)


// === ARCHIVO: src/extract/accounting_consolidator_reader.py ===
import logging
from datetime import datetime
from typing import Optional, Dict, Any, List
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, to_timestamp, lit, coalesce, sum as spark_sum, count
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, IntegerType

logger = logging.getLogger(__name__)


class AccountingConsolidatorReader:
    """Lector de datos desde el consolidador contable con verificación de consistencia."""

    def __init__(self, spark: SparkSession, source_path: str, batch_date: str):
        self.spark = spark
        self.source_path = source_path
        self.batch_date = batch_date
        self._consolidation_schema = self._build_consolidation_schema()

    def _build_consolidation_schema(self) -> StructType:
        return StructType([
            StructField("accounting_entry_id", StringType(), False),
            StructField("transaction_id", StringType(), True),
            StructField("customer_id", StringType(), False),
            StructField("account_code", StringType(), False),
            StructField("account_description", StringType(), True),
            StructField("debit_amount", DoubleType(), True),
            StructField("credit_amount", DoubleType(), True),
            StructField("net_amount", DoubleType(), False),
            StructField("currency", StringType(), True),
            StructField("accounting_date", StringType(), False),
            StructField("posting_date", StringType(), True),
            StructField("consolidation_period", StringType(), True),
            StructField("legal_entity", StringType(), True),
            StructField("cost_center", StringType(), True),
            StructField("gl_account_type", StringType(), True),
        ])

    def read(self) -> DataFrame:
        logger.info(f"Iniciando lectura del consolidador contable para fecha {self.batch_date}")
        try:
            df = self.spark.read.format("parquet").schema(self._consolidation_schema).load(self.source_path)
            df = self._enrich_and_validate(df)
            self._verify_accounting_consistency(df)
            logger.info(f"Lectura del consolidador completada: {df.count()} entradas contables")
            return df
        except Exception as e:
            logger.error(f"Error al leer datos del consolidador contable: {str(e)}")
            raise

    def _enrich_and_validate(self, df: DataFrame) -> DataFrame:
        df = df.withColumn("accounting_date", 
                          to_timestamp(col("accounting_date"), "yyyy-MM-dd"))
        df = df.withColumn("posting_date", 
                          to_timestamp(col("posting_date"), "yyyy-MM-dd"))
        df = df.withColumn("ingestion_timestamp", lit(datetime.now()))
        df = df.withColumn("source_system", lit("ACCOUNTING_CONSOLIDATOR"))
        df = df.withColumn("currency", coalesce(col("currency"), lit("USD")))
        df = df.withColumn("is_balanced", 
                          (coalesce(col("debit_amount"), lit(0.0)) - 
                           coalesce(col("credit_amount"), lit(0.0)) == col("net_amount")))
        df = df.withColumn("accounting_period", 
                          col("consolidation_period").cast("integer"))
        return df

    def _verify_accounting_consistency(self, df: DataFrame) -> None:
        total_debits = df.agg(spark_sum(coalesce(col("debit_amount"), lit(0.0)))).collect()[0][0] or 0.0
        total_credits = df.agg(spark_sum(coalesce(col("credit_amount"), lit(0.0)))).collect()[0][0] or 0.0
        difference = abs(total_debits - total_credits)
        if difference > 0.01:
            logger.warning(f"Inconsistencia detectada: débitos {total_debits} vs créditos {total_credits}")
        balanced_count = df.filter(col("is_balanced") == True).count()
        total_count = df.count()
        logger.info(f"Verificación de consistencia: {balanced_count}/{total_count} entradas balanceadas")
        if balanced_count < total_count:
            logger.warning(f"{total_count - balanced_count} entradas no balanceadas encontradas")

    def get_consistency_report(self, df: DataFrame) -> Dict[str, Any]:
        total_entries = df.count()
        balanced = df.filter(col("is_balanced") == True).count()
        unbalanced = total_entries - balanced
        totals = df.agg(
            spark_sum("debit_amount").alias("total_debits"),
            spark_sum("credit_amount").alias("total_credits"),
            spark_sum("net_amount").alias("total_net"),
            count("accounting_entry_id").alias("unique_entries")
        ).collect()[0]
        entity_summary = df.groupBy("legal_entity").agg(
            spark_sum("net_amount").alias("net_by_entity"),
            count("accounting_entry_id").alias("entries_by_entity")
        ).collect()
        return {
            "total_entries": total_entries,
            "balanced_entries": balanced,
            "unbalanced_entries": unbalanced,
            "total_debits": totals["total_debits"] or 0.0,
            "total_credits": totals["total_credits"] or 0.0,
            "total_net": totals["total_net"] or 0.0,
            "unique_entry_ids": totals["unique_entries"],
            "by_legal_entity": {row["legal_entity"]: {
                "net": row["net_by_entity"] or 0.0,
                "entries": row["entries_by_entity"]
            } for row in entity_summary},
            "batch_date": self.batch_date,
            "source": "ACCOUNTING_CONSOLIDATOR"
        }

    def read_by_period(self, period: int) -> DataFrame:
        df = self.read()
        return df.filter(col("accounting_period") == period)

    def read_with_transaction_link(self) -> DataFrame:
        df = self.read()
        linked = df.filter(col("transaction_id").isNotNull())
        logger.info(f"Registros con link a transacción: {linked.count()}")
        return linked


def create_accounting_reader(spark: SparkSession, config: Dict[str, Any], batch_date: str) -> AccountingConsolidatorReader:
    source_path = config.get("accounting_consolidator", {}).get("path", "/data/raw/accounting_consolidator")
    return AccountingConsolidatorReader(spark, source_path, batch_date)

// === ARCHIVO: src/transform/transaction_transformer.py ===
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, current_timestamp, md5, concat_ws
from typing import Tuple, Optional

from src.transform.quality_rules import (
    QualityRule,
    apply_quality_rules,
    quarantine_invalid_records,
    get_quarantine_dataframe
)
from src.utils.idempotency_manager import IdempotencyManager


class TransactionTransformer:
    """Transformer idempotente de transacciones con aplicación de reglas de calidad."""
    
    def __init__(self, spark: SparkSession, idempotency_manager: IdempotencyManager):
        self.spark = spark
        self.idempotency_manager = idempotency_manager
        self.quarantine_path = "data/quarantine"
    
    def transform(self, source_df: DataFrame, batch_id: str) -> Tuple[DataFrame, DataFrame]:
        """
        Aplica transformación idempotente a las transacciones.
        
        Args:
            source_df: DataFrame con transacciones crudas
            batch_id: Identificador único del lote de procesamiento
            
        Returns:
            Tupla de (transacciones_válidas, transacciones_en_cuarentena)
        """
        df_with_hash = self._add_transaction_hash(source_df)
        
        existing_hashes = self.idempotency_manager.get_processed_hashes(batch_id)
        new_records = df_with_hash.filter(~col("transaction_hash").isin(existing_hashes))
        
        valid_records, quarantine_records = apply_quality_rules(new_records, self.spark)
        
        valid_with_metadata = valid_records.withColumn(
            "processed_at", current_timestamp()
        ).withColumn(
            "batch_id", lit(batch_id)
        )
        
        quarantine_with_metadata = quarantine_records.withColumn(
            "quarantine_at", current_timestamp()
        ).withColumn(
            "batch_id", lit(batch_id)
        )
        
        if quarantine_with_metadata.count() > 0:
            self._save_quarantine(quarantine_with_metadata)
        
        return valid_with_metadata, quarantine_with_metadata
    
    def _add_transaction_hash(self, df: DataFrame) -> DataFrame:
        """Añade hash único para identificación idempotente."""
        return df.withColumn(
            "transaction_hash",
            md5(concat_ws(
                "|",
                col("transaction_id"),
                col("transaction_date"),
                col("amount"),
                col("currency")
            ))
        )
    
    def _save_quarantine(self, quarantine_df: DataFrame) -> None:
        """Guarda registros en cuarentena en formato Parquet."""
        quarantine_df.write.mode("append").partitionBy("transaction_date").parquet(
            self.quarantine_path
        )
    
    def reprocess_quarantine(self, quarantine_df: DataFrame) -> Tuple[DataFrame, DataFrame]:
        """Re-procesa registros que estaban en cuarentena."""
        return apply_quality_rules(quarantine_df, self.spark)


def create_transaction_transformer(spark: SparkSession) -> TransactionTransformer:
    """Factory para crear el transformer con sus dependencias."""
    idempotency_manager = IdempotencyManager(spark)
    return TransactionTransformer(spark, idempotency_manager)


def transform_transactions(
    spark: SparkSession,
    source_df: DataFrame,
    batch_id: str
) -> Tuple[DataFrame, DataFrame]:
    """
    Función de conveniencia para transformar transacciones.
    
    Args:
        spark: Sesión de Spark
        source_df: DataFrame de transacciones fuente
        batch_id: Identificador del lote
        
    Returns:
        (transacciones_válidas, transacciones_en_cuarentena)
    """
    transformer = create_transaction_transformer(spark)
    return transformer.transform(source_df, batch_id)


// === ARCHIVO: src/transform/quality_rules.py ===
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, when, lit, concat_ws, md5
from pyspark.sql.types import StructType, StructField, StringType, TimestampType
from typing import List, Tuple, Dict, Any
from enum import Enum


class QualityRuleType(Enum):
    """Tipos de reglas de calidad disponibles."""
    NON_NULL = "non_null"
    RANGE_CHECK = "range_check"
    PATTERN_MATCH = "pattern_match"
    DUPLICATE_CHECK = "duplicate_check"
    CROSS_FIELD_VALIDATION = "cross_field_validation"


class QualityRule:
    """Definición de una regla de calidad con umbrales configurables."""
    
    def __init__(
        self,
        name: str,
        rule_type: QualityRuleType,
        column: str,
        condition: str,
        error_message: str,
        severity: str = "ERROR",
        threshold: Any = None
    ):
        self.name = name
        self.rule_type = rule_type
        self.column = column
        self.condition = condition
        self.error_message = error_message
        self.severity = severity
        self.threshold = threshold
    
    def evaluate(self, df: DataFrame) -> DataFrame:
        """Evalúa la regla sobre el DataFrame dado."""
        return df.withColumn(
            f"{self.name}_passed",
            when(col(self.column).isNull() == False, True).otherwise(False)
        )


class QualityRulesConfig:
    """Configuración centralizada de reglas de calidad para transacciones."""
    
    @staticmethod
    def get_transaction_rules() -> List[QualityRule]:
        """Devuelve las reglas de calidad estándar para transacciones."""
        return [
            QualityRule(
                name="transaction_id_not_null",
                rule_type=QualityRuleType.NON_NULL,
                column="transaction_id",
                condition="transaction_id IS NOT NULL",
                error_message="El identificador de transacción no puede ser nulo",
                severity="ERROR"
            ),
            QualityRule(
                name="amount_positive",
                rule_type=QualityRuleType.RANGE_CHECK,
                column="amount",
                condition="amount > 0",
                error_message="El monto debe ser mayor que cero",
                severity="ERROR",
                threshold=0
            ),
            QualityRule(
                name="amount_not_null",
                rule_type=QualityRuleType.NON_NULL,
                column="amount",
                condition="amount IS NOT NULL",
                error_message="El monto no puede ser nulo",
                severity="ERROR"
            ),
            QualityRule(
                name="currency_valid",
                rule_type=QualityRuleType.PATTERN_MATCH,
                column="currency",
                condition="currency IN ('USD', 'EUR', 'MXN', 'COP', 'BRL')",
                error_message="La moneda debe ser una de las permitidas",
                severity="ERROR"
            ),
            QualityRule(
                name="transaction_date_not_null",
                rule_type=QualityRuleType.NON_NULL,
                column="transaction_date",
                condition="transaction_date IS NOT NULL",
                error_message="La fecha de transacción no puede ser nula",
                severity="ERROR"
            ),
            QualityRule(
                name="account_id_not_null",
                rule_type=QualityRuleType.NON_NULL,
                column="account_id",
                condition="account_id IS NOT NULL",
                error_message="El identificador de cuenta no puede ser nulo",
                severity="ERROR"
            ),
            QualityRule(
                name="amount_max_threshold",
                rule_type=QualityRuleType.RANGE_CHECK,
                column="amount",
                condition="amount <= 1000000",
                error_message="El monto excede el umbral máximo permitido",
                severity="WARNING",
                threshold=1000000
            ),
            QualityRule(
                name="description_length",
                rule_type=QualityRuleType.RANGE_CHECK,
                column="description",
                condition="LENGTH(description) <= 500",
                error_message="La descripción excede la longitud máxima",
                severity="WARNING",
                threshold=500
            )
        ]


def apply_quality_rules(df: DataFrame, spark: SparkSession) -> Tuple[DataFrame, DataFrame]:
    """
    Aplica todas las reglas de calidad definidas a un DataFrame de transacciones.
    
    Args:
        df: DataFrame de transacciones a validar
        spark: Sesión de Spark para crear DataFrames temporales
        
    Returns:
        Tupla de (registros_válidos, registros_invlálidos)
    """
    rules = QualityRulesConfig.get_transaction_rules()
    
    validation_df = df
    for rule in rules:
        validation_df = rule.evaluate(validation_df)
    
    passed_columns = [f"{rule.name}_passed" for rule in rules]
    all_passed = None
    for col_name in passed_columns:
        if all_passed is None:
            all_passed = col(col_name)
        else:
            all_passed = all_passed & col(col_name)
    
    validation_df = validation_df.withColumn("_all_rules_passed", all_passed)
    
    valid_records = validation_df.filter(col("_all_rules_passed") == True)
    invalid_records = validation_df.filter(col("_all_rules_passed") == False)
    
    return valid_records, invalid_records


def quarantine_invalid_records(
    invalid_df: DataFrame,
    spark: SparkSession,
    quarantine_path: str
) -> DataFrame:
    """
    Prepara DataFrame de cuarentena con metadata de errores.
    
    Args:
        invalid_df: DataFrame con registros que no pasaron las reglas
        spark: Sesión de Spark
        quarantine_path: Ruta donde se almacenará la cuarentena
        
    Returns:
        DataFrame enriquecido con metadata de cuarentena
    """
    return invalid_df.withColumn(
        "quarantine_reason",
        concat_ws("; ", *[
            when(col(f"{rule.name}_passed") == False, lit(rule.error_message))
            for rule in QualityRulesConfig.get_transaction_rules()
        ])
    ).withColumn(
        "quarantine_path",
        lit(quarantine_path)
    )


def get_quarantine_schema() -> StructType:
    """Devuelve el esquema del DataFrame de cuarentena."""
    base_fields = [
        StructField("transaction_id", StringType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("amount", StringType(), True),
        StructField("currency", StringType(), True),
        StructField("account_id", StringType(), True),
        StructField("description", StringType(), True),
        StructField("source_system", StringType(), True),
    ]
    quarantine_fields = [
        StructField("quarantine_reason", StringType(), True),
        StructField("quarantine_path", StringType(), True),
        StructField("quarantine_at", TimestampType(), True),
        StructField("batch_id", StringType(), True),
    ]
    return StructType(base_fields + quarantine_fields)


def get_quarantine_dataframe(spark: SparkSession, quarantine_path: str) -> DataFrame:
    """
    Lee los registros en cuarentena desde el almacenamiento.
    
    Args:
        spark: Sesión de Spark
        quarantine_path: Ruta donde se almacenan los registros en cuarentena
        
    Returns:
        DataFrame con registros en cuarentena
    """
    schema = get_quarantine_schema()
    return spark.read.schema(schema).parquet(quarantine_path)


// === ARCHIVO: src/load/analytical_model_writer.py ===
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DecimalType
from delta.tables import DeltaTable
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)


class AnalyticalModelWriter:
    """Escritor de transacciones válidas en el modelo analítico con soporte idempotente."""
    
    def __init__(
        self,
        spark: SparkSession,
        output_path: str,
        partition_columns: list[str] = ["transaction_date"]
    ):
        self.spark = spark
        self.output_path = output_path
        self.partition_columns = partition_columns
    
    def write(
        self,
        df: DataFrame,
        mode: str = "merge",
        batch_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Escribe transacciones en el modelo analítico usando Delta Lake.
        
        Args:
            df: DataFrame con transacciones válidas
            mode: Modo de escritura ('merge', 'append', 'overwrite')
            batch_id: Identificador del lote para trazabilidad
            
        Returns:
            Diccionario con métricas de la escritura
        """
        if df.count() == 0:
            logger.info("No hay transacciones válidas para escribir")
            return {"records_written": 0, "mode": mode}
        
        df_with_metadata = df.withColumn(
            "loaded_at", current_timestamp()
        )
        
        if self._delta_table_exists():
            if mode == "merge":
                return self._merge_existing(df_with_metadata, batch_id)
            else:
                return self._append_data(df_with_metadata)
        else:
            return self._create_new_table(df_with_metadata)
    
    def _delta_table_exists(self) -> bool:
        """Verifica si la tabla Delta ya existe."""
        try:
            DeltaTable.forPath(self.spark, self.output_path)
            return True
        except Exception:
            return False
    
    def _merge_existing(self, df: DataFrame, batch_id: Optional[str]) -> Dict[str, Any]:
        """Realiza merge idempotente con registros existentes."""
        delta_table = DeltaTable.forPath(self.spark, self.output_path)
        
        merge_condition = "t.transaction_id = s.transaction_id AND t.transaction_date = s.transaction_date"
        
        delta_table.alias("t").merge(
            df.alias("s"),
            merge_condition
        ).whenMatchedUpdateAll().whenNotMatchedInsertAll().execute()
        
        records_written = df.count()
        logger.info(f"Merge completado: {records_written} registros procesados")
        
        return {
            "records_written": records_written,
            "mode": "merge",
            "batch_id": batch_id
        }
    
    def _append_data(self, df: DataFrame) -> Dict[str, Any]:
        """Añade datos en modo append."""
        df.write.mode("append").partitionBy(*self.partition_columns).parquet(
            self.output_path
        )
        
        records_written = df.count()
        logger.info(f"Append completado: {records_written} registros escritos")
        
        return {
            "records_written": records_written,
            "mode": "append"
        }
    
    def _create_new_table(self, df: DataFrame) -> Dict[str, Any]:
        """Crea nueva tabla en formato Delta."""
        df.write.mode("overwrite").partitionBy(*self.partition_columns).format("delta").save(
            self.output_path
        )
        
        records_written = df.count()
        logger.info(f"Tabla creada: {records_written} registros escritos en {self.output_path}")
        
        return {
            "records_written": records_written,
            "mode": "overwrite",
            "table_created": True
        }
    
    def get_analytical_schema(self) -> StructType:
        """Devuelve el esquema del modelo analítico de transacciones."""
        return StructType([
            StructField("transaction_id", StringType(), False),
            StructField("transaction_date", StringType(), False),
            StructField("amount", DecimalType(18, 2), False),
            StructField("currency", StringType(), False),
            StructField("account_id", StringType(), False),
            StructField("description", StringType(), True),
            StructField("source_system", StringType(), True),
            StructField("transaction_hash", StringType(), True),
            StructField("processed_at", TimestampType(), True),
            StructField("loaded_at", TimestampType(), True),
            StructField("batch_id", StringType(), True)
        ])
    
    def validate_consistency(self) -> Dict[str, Any]:
        """
        Valida consistencia entre el registro de idempotencia y el modelo analítico.
        
        Returns:
            Diccionario con resultado de la validación
        """
        if not self._delta_table_exists():
            return {"consistent": True, "message": "Tabla no existe aún"}
        
        delta_table = DeltaTable.forPath(self.spark, self.output_path)
        df = delta_table.toDF()
        
        total_records = df.count()
        unique_transactions = df.select("transaction_id").distinct().count()
        
        is_consistent = total_records == unique_transactions
        
        return {
            "consistent": is_consistent,
            "total_records": total_records,
            "unique_transactions": unique_transactions,
            "duplicate_count": total_records - unique_transactions
        }


def create_analytical_writer(
    spark: SparkSession,
    output_path: str,
    partition_by: list[str] = None
) -> AnalyticalModelWriter:
    """
    Factory para crear el escritor del modelo analítico.
    
    Args:
        spark: Sesión de Spark
        output_path: Ruta del modelo analítico
        partition_by: Columnas para particionado
        
    Returns:
        Instancia de AnalyticalModelWriter
    """
    if partition_by is None:
        partition_by = ["transaction_date"]
    
    return AnalyticalModelWriter(spark, output_path, partition_by)


def write_to_analytical_model(
    spark: SparkSession,
    df: DataFrame,
    output_path: str,
    batch_id: Optional[str] = None
) -> Dict[str, Any]:
    """
    Función de conveniencia para escribir en el modelo analítico.
    
    Args:
        spark: Sesión de Spark
        df: DataFrame con transacciones válidas
        output_path: Ruta del modelo analítico
        batch_id: Identificador del lote
        
    Returns:
        Métricas de la escritura
    """
    writer = create_analytical_writer(spark, output_path)
    return writer.write(df, mode="merge", batch_id=batch_id)


import logging
from typing import Optional
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, count, lit, when, coalesce, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType


class IdempotencyManager:
    """Gestor de idempotencia que verifica identificadores únicos y maneja duplicados."""

    def __init__(self, spark: SparkSession, checkpoint_path: str):
        self.spark = spark
        self.checkpoint_path = checkpoint_path
        self.logger = logging.getLogger(__name__)
        self._ensure_checkpoint_table()

    def _ensure_checkpoint_table(self):
        """Crea la tabla de checkpoint si no existe."""
        try:
            self.spark.sql(f"CREATE TABLE IF NOT EXISTS idempotency_checkpoint (
                transaction_id STRING NOT NULL,
                processed_at TIMESTAMP NOT NULL,
                source_system STRING NOT NULL,
                PRIMARY KEY (transaction_id) NOT ENFORCED
            ) USING DELTA LOCATION '{self.checkpoint_path}'")
        except Exception as e:
            self.logger.warning(f"No se pudo crear tabla de checkpoint: {e}")

    def filter_new_transactions(
        self,
        df: DataFrame,
        id_column: str,
        source_column: str,
        timestamp_column: str
    ) -> DataFrame:
        """Filtra solo transacciones nuevas que no existen en el checkpoint."""
        checkpoint_df = self.spark.table("idempotency_checkpoint") if self._table_exists() else self.spark.createDataFrame([], StructType())

        if checkpoint_df.isEmpty():
            self.logger.info("No hay checkpoint existente, procesando todas las transacciones")
            return df.withColumn("processed_at", to_timestamp(lit(datetime.now().isoformat())))

        existing_ids = checkpoint_df.select("transaction_id").distinct()
        new_transactions = df.join(existing_ids, df[id_column] == existing_ids["transaction_id"], "leftanti")

        count_new = new_transactions.count()
        count_total = df.count()
        self.logger.info(f"Transacciones nuevas: {count_new}/{count_total}")

        return new_transactions.withColumn("processed_at", to_timestamp(lit(datetime.now().isoformat())))

    def _table_exists(self) -> bool:
        """Verifica si la tabla de checkpoint existe."""
        try:
            self.spark.sql("SELECT 1 FROM idempotency_checkpoint LIMIT 1")
            return True
        except:
            return False

    def register_processed(self, df: DataFrame, id_column: str, source_column: str):
        """Registra las transacciones procesadas en el checkpoint."""
        if df.isEmpty():
            self.logger.info("No hay transacciones para registrar")
            return

        checkpoint_data = df.select(
            col(id_column).alias("transaction_id"),
            col("processed_at"),
            col(source_column).alias("source_system")
        ).distinct()

        checkpoint_data.write.format("delta").mode("append").save(self.checkpoint_path)
        self.logger.info(f"Registradas {checkpoint_data.count()} transacciones en checkpoint")

    def get_duplicate_count(self, df: DataFrame, id_column: str) -> int:
        """Cuenta duplicados en el DataFrame de entrada."""
        duplicates = df.groupBy(id_column).agg(count(lit(1)).alias("cnt")).filter(col("cnt") > 1)
        return duplicates.count()

    def get_duplicate_records(self, df: DataFrame, id_column: str) -> DataFrame:
        """Obtiene los registros duplicados para análisis."""
        duplicate_ids = (
            df.groupBy(id_column)
            .agg(count(lit(1)).alias("occurrence_count"))
            .filter(col("occurrence_count") > 1)
        )
        return df.join(duplicate_ids, id_column).orderBy(id_column, "processed_at")

    def detect_duplicates_between_sources(
        self,
        source_a: DataFrame,
        source_b: DataFrame,
        id_column: str
    ) -> DataFrame:
        """Detecta duplicados entre dos fuentes diferentes."""
        ids_a = source_a.select(id_column).withColumn("source", lit("A"))
        ids_b = source_b.select(id_column).withColumn("source", lit("B"))

        duplicates = ids_a.join(ids_b, id_column).withColumn("duplicate", lit(True))
        return duplicates

    def get_processed_ids(self, source_system: Optional[str] = None) -> DataFrame:
        """Obtiene los IDs ya procesados, opcionalmente filtrados por sistema origen."""
        if not self._table_exists():
            return self.spark.createDataFrame([], StructType([StructField("transaction_id", StringType())]))

        query = "SELECT transaction_id FROM idempotency_checkpoint"
        if source_system:
            query += f" WHERE source_system = '{source_system}'"

        return self.spark.sql(query)

    def cleanup_old_checkpoints(self, days_to_keep: int = 30):
        """Limpia registros antiguos del checkpoint para mantener tamaño manejable."""
        if not self._table_exists():
            return

        cutoff_date = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        self.spark.sql(f"""
            DELETE FROM idempotency_checkpoint
            WHERE processed_at < timestamp '{cutoff_date.isoformat()}' - INTERVAL '{days_to_keep}' days
        """)
        self.logger.info(f"Limpiados checkpoints mayores a {days_to_keep} días")

    def validate_id_uniqueness(self, df: DataFrame, id_column: str) -> tuple[bool, int]:
        """Valida que no haya IDs duplicados en el DataFrame. Retorna (es_unico, count_duplicados)."""
        total_count = df.count()
        distinct_count = df.select(id_column).distinct().count()
        duplicate_count = total_count - distinct_count

        is_unique = duplicate_count == 0
        if not is_unique:
            self.logger.warning(f"Se encontraron {duplicate_count} IDs duplicados en el DataFrame")

        return is_unique, duplicate_count


def create_idempotency_manager(spark: SparkSession, config: dict) -> IdempotencyManager:
    """Factory para crear el gestor de idempotencia desde configuración."""
    checkpoint_path = config.get("idempotency", {}).get("checkpoint_path", "/tmp/idempotency")
    return IdempotencyManager(spark, checkpoint_path)
// === ARCHIVO: src/utils/logging_config.py ===
import logging
import sys
from typing import Optional
from datetime import datetime
from pathlib import Path


class PipelineLogger:
    """Configurador de logging especializado para pipelines de datos."""

    def __init__(self, app_name: str, log_level: str = "INFO", log_dir: Optional[str] = None):
        self.app_name = app_name
        self.log_level = getattr(logging, log_level.upper(), logging.INFO)
        self.log_dir = log_dir
        self.logger: Optional[logging.Logger] = None

    def setup(self) -> logging.Logger:
        """Configura el logging con handlers de consola y archivo."""
        self.logger = logging.getLogger(self.app_name)
        self.logger.setLevel(self.log_level)
        self.logger.handlers.clear()

        formatter = logging.Formatter(
            fmt="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S"
        )

        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(self.log_level)
        console_handler.setFormatter(formatter)
        self.logger.addHandler(console_handler)

        if self.log_dir:
            self._add_file_handler(formatter)

        return self.logger

    def _add_file_handler(self, formatter: logging.Formatter):
        """Agrega handler de archivo con rotación diaria."""
        log_path = Path(self.log_dir)
        log_path.mkdir(parents=True, exist_ok=True)

        filename = f"{self.app_name}_{datetime.now().strftime('%Y%m%d')}.log"
        filepath = log_path / filename

        file_handler = logging.FileHandler(filepath, encoding="utf-8")
        file_handler.setLevel(self.log_level)
        file_handler.setFormatter(formatter)
        self.logger.addHandler(file_handler)


class ExecutionTracker:
    """Trackea la ejecución de etapas del pipeline para auditoría."""

    def __init__(self, logger: logging.Logger):
        self.logger = logger
        self.stages: list[dict] = []
        self.current_stage: Optional[str] = None
        self.stage_start: Optional[datetime] = None

    def start_stage(self, stage_name: str, metadata: Optional[dict] = None):
        """Marca el inicio de una etapa del pipeline."""
        self.current_stage = stage_name
        self.stage_start = datetime.now()
        self.logger.info(f"=== INICIO ETAPA: {stage_name} ===")

        if metadata:
            for key, value in metadata.items():
                self.logger.info(f"  {key}: {value}")

    def end_stage(self, stage_name: str, record_count: int = 0, error: Optional[Exception] = None):
        """Marca el fin de una etapa del pipeline."""
        duration = (datetime.now() - self.stage_start).total_seconds() if self.stage_start else 0

        stage_info = {
            "stage": stage_name,
            "start_time": self.stage_start.isoformat() if self.stage_start else None,
            "end_time": datetime.now().isoformat(),
            "duration_seconds": duration,
            "record_count": record_count,
            "status": "FAILED" if error else "SUCCESS"
        }
        self.stages.append(stage_info)

        if error:
            self.logger.error(f"=== FIN ETAPA: {stage_name} - FALLÓ ===")
            self.logger.error(f"  Error: {str(error)}")
            self.logger.error(f"  Duración: {duration:.2f}s")
        else:
            self.logger.info(f"=== FIN ETAPA: {stage_name} - ÉXITO ===")
            self.logger.info(f"  Registros: {record_count}, Duración: {duration:.2f}s")

        self.current_stage = None
        self.stage_start = None

    def get_execution_summary(self) -> dict:
        """Genera resumen de la ejecución completa."""
        total_duration = sum(s["duration_seconds"] for s in self.stages)
        failed_stages = [s for s in self.stages if s["status"] == "FAILED"]

        return {
            "total_stages": len(self.stages),
            "total_duration_seconds": total_duration,
            "failed_stages": len(failed_stages),
            "stages": self.stages
        }

    def log_summary(self):
        """Imprime el resumen de ejecución."""
        summary = self.get_execution_summary()
        self.logger.info("=" * 60)
        self.logger.info("RESUMEN DE EJECUCIÓN DEL PIPELINE")
        self.logger.info("=" * 60)
        self.logger.info(f"Total de etapas: {summary['total_stages']}")
        self.logger.info(f"Etapas fallidas: {summary['failed_stages']}")
        self.logger.info(f"Duración total: {summary['total_duration_seconds']:.2f}s")

        for stage in summary["stages"]:
            status_icon = "✓" if stage["status"] == "SUCCESS" else "✗"
            self.logger.info(
                f"  {status_icon} {stage['stage']}: {stage['duration_seconds']:.2f}s "
                f"({stage['record_count']} registros)"
            )


def setup_pipeline_logging(
    app_name: str,
    log_level: str = "INFO",
    log_dir: Optional[str] = None
) -> tuple[logging.Logger, ExecutionTracker]:
    """Factory principal para configurar logging del pipeline."""
    pipeline_logger = PipelineLogger(app_name, log_level, log_dir)
    logger = pipeline_logger.setup()
    tracker = ExecutionTracker(logger)
    return logger, tracker


def log_dataframe_info(logger: logging.Logger, df, stage_name: str, show_sample: bool = False):
    """Registra información de un DataFrame de PySpark."""
    try:
        count = df.count()
        logger.info(f"[{stage_name}] DataFrame: {count} registros, {len(df.columns)} columnas")
        logger.info(f"[{stage_name}] Columnas: {', '.join(df.columns)}")

        if show_sample and count > 0:
            logger.info(f"[{stage_name}] Muestra de datos:")
            df.show(5, truncate=False)
    except Exception as e:
        logger.warning(f"[{stage_name}] No se pudo obtener info del DataFrame: {e}")


def log_quarantine_results(logger: logging.Logger, quarantine_df, stage_name: str):
    """Registra resultados de la cuarentena de calidad."""
    if quarantine_df is None or quarantine_df.isEmpty():
        logger.info(f"[{stage_name}] No hay registros en cuarentena")
        return

    total_count = quarantine_df.count()
    error_types = quarantine_df.groupBy("error_type").count().collect()

    logger.warning(f"[{stage_name}] {total_count} registros enviados a cuarentena")
    for row in error_types:
        logger.warning(f"  - {row['error_type']}: {row['count']} registros")
// === ARCHIVO: conf/config.yaml ===
estage: development

spark:
  app_name: transaction-consolidation-pipeline
  master: local[*]
  memory: 4g
  cores: 2
  max_result_size: 2g
  driver_memory: 2g
  executor_memory: 2g
  default_timeout: 300
  max_retries: 3

sources:
  credit_originator:
    type: delta
    path: s3://datalake/raw/credit_originator/
    format: delta
    options:
      mergeSchema: true
      overwriteSchema: false
    partition_by: date

  risk_bureau:
    type: delta
    path: s3://datalake/raw/risk_bureau/
    format: delta
    options:
      mergeSchema: true
      overwriteSchema: false
    partition_by: date

  accounting_consolidator:
    type: delta
    path: s3://datalake/raw/accounting_consolidator/
    format: delta
    options:
      mergeSchema: true
      overwriteSchema: false
    partition_by: date

destination:
  analytical_model:
    type: delta
    path: s3://datalake/analytics/transactions/
    format: delta
    partition_by:
      - transaction_date
      - source_system
    options:
      mergeSchema: true
      overwriteSchema: false
      dataSkippingNumIndexedCols: 3

quarantine:
  path: s3://datalake/quarantine/transactions/
  format: parquet
  partition_by: error_date
  retention_days: 90

idempotency:
  checkpoint_path: s3://datalake/checkpoints/idempotency/
  enabled: true
  cleanup_days: 30

quality_rules:
  enabled: true
  rules:
    - name: transaction_id_not_null
      description: El identificador de transacción no puede ser nulo
      severity: critical
      action: quarantine

    - name: amount_positive
      description: El monto de la transacción debe ser mayor a cero
      severity: critical
      action: quarantine

    - name: transaction_date_valid
      description: La fecha de transacción no puede ser futura ni anterior a 2020
      severity: warning
      action: quarantine

    - name: currency_code_valid
      description: El código de moneda debe ser válido (ISO 4217)
      severity: warning
      action: quarantine

    - name: account_id_format
      description: El ID de cuenta debe seguir el formato esperado
      severity: info
      action: quarantine

    - name: duplicate_detection
      description: No puede haber IDs de transacción duplicados en la misma fuente
      severity: critical
      action: quarantine

  valid_currencies:
    - USD
    - EUR
    - MXN
    - GBP
    - JPY
    - CAD
    - BRL
    - COP

  date_range:
    min_year: 2020
    max_future_days: 0

performance:
  shuffle_partitions: 200
  broadcast_threshold_mb: 10
  adaptive_enabled: true
  coalesce_partitions: 50
  checkpoint_interval: 100

  spark_conf:
    spark.sql.adaptive.enabled: true
    spark.sql.adaptive.coalescePartitions.enabled: true
    spark.sql.adaptive.coalescePartitions.minPartitionNum: 1
    spark.sql.adaptive.skewJoin.enabled: true
    spark.sql.adaptive.skewJoin.skewedPartitionFactor: 5
    spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes: 256MB
    spark.sql.files.maxPartitionBytes: 128MB
    spark.sql.shuffle.partitions: 200

airflow:
  dag_id: consolidate_transactions
  schedule_interval: 0 4 * * *
  start_date: "2024-01-01"
  retry_delay_minutes: 5
  max_retries: 3
  execution_timeout_minutes: 120
  sla_minutes: 90
  depends_on_past: false
  wait_for_downstream: true

  notifications:
    on_success:
      - type: email
        recipients:
          - data-eng@empresa.com
    on_failure:
      - type: email
        recipients:
          - data-eng@empresa.com
          - ops@empresa.com
      - type: slack
        webhook: https://hooks.slack.com/services/xxx

monitoring:
  metrics_enabled: true
  metrics_path: /tmp/metrics
  alerting:
    latency_threshold_seconds: 300
    error_rate_threshold_percent: 5
    record_count_variance_percent: 20

  thresholds:
    min_records_per_run: 1000
    max_records_per_run: 10000000
    expected_processing_rate_per_minute: 10000

security:
  iam_role: arn:aws:iam::123456789012:role/data-lake-writer
  kms_key_id: alias/data-lake-key
  encryption: AES256
  access_control:
    read_roles:
      - arn:aws:iam::123456789012:role/analyst
      - arn:aws:iam::123456789012:role/data-scientist
    write_roles:
      - arn:aws:iam::123456789012:role/data-engineer

environments:
  production:
    sources:
      credit_originator:
        path: s3://prod-datalake/raw/credit_originator/
      risk_bureau:
        path: s3://prod-datalake/raw/risk_bureau/
      accounting_consolidator:
        path: s3://prod-datalake/raw/accounting_consolidator/
    destination:
      analytical_model:
        path: s3://prod-datalake/analytics/transactions/
    spark:
      master: spark://prod-master:7077
      executor_memory: 4g
      cores: 4

  staging:
    sources:
      credit_originator:
        path: s3://staging-datalake/raw/credit_originator/
      risk_bureau:
        path: s3://staging-datalake/raw/risk_bureau/
      accounting_consolidator:
        path: s3://staging-datalake/raw/accounting_consolidator/
    destination:
      analytical_model:
        path: s3://staging-datalake/analytics/transactions/
    spark:
      master: local[*]
      executor_memory: 2g
      cores: 2


import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime


@pytest.fixture(scope="module")
def spark_session():
    """Crea una sesión de Spark para los tests."""
    spark = SparkSession.builder \
        .appName("test_transaction_transformer") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def sample_transactions_df(spark_session):
    """DataFrame de transacciones de prueba."""
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("originator", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), True),
        StructField("transaction_date", TimestampType(), False),
        StructField("transaction_type", StringType(), True),
        StructField("status", StringType(), True),
    ])
    
    data = [
        ("TX001", "CREDIT_ORIGINATOR", "ACC123", 1000.00, "USD", datetime(2024, 1, 15, 10, 30), "DISBURSEMENT", "COMPLETED"),
        ("TX002", "RISK_BUREAU", "ACC456", 2500.50, "USD", datetime(2024, 1, 15, 11, 0), "PAYMENT", "COMPLETED"),
        ("TX003", "ACCOUNTING_CONSOLIDATOR", "ACC789", 500.00, "EUR", datetime(2024, 1, 15, 12, 15), "TRANSFER", "PENDING"),
        ("TX004", "CREDIT_ORIGINATOR", "ACC123", 1500.00, "USD", datetime(2024, 1, 15, 13, 0), "DISBURSEMENT", "COMPLETED"),
        ("TX005", "RISK_BUREAU", "ACC999", 0.00, "USD", datetime(2024, 1, 15, 14, 30), "PAYMENT", "FAILED"),
    ]
    
    return spark_session.createDataFrame(data, schema)


@pytest.fixture
def expected_schema():
    """Esquema esperado del DataFrame transformado."""
    return StructType([
        StructField("transaction_id", StringType(), False),
        StructField("consolidated_date", StringType(), False),
        StructField("originator_code", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("amount_usd", DoubleType(), False),
        StructField("amount_eur", DoubleType(), True),
        StructField("transaction_category", StringType(), True),
        StructField("processing_status", StringType(), True),
        StructField("is_valid", StringType(), True),
    ])


def test_transformer_applies_business_rules(spark_session, sample_transactions_df):
    """Verifica que el transformer aplica las reglas de negocio correctamente."""
    from src.transform.transaction_transformer import TransactionTransformer
    
    transformer = TransactionTransformer(spark_session)
    result_df = transformer.transform(sample_transactions_df)
    
    assert result_df.count() == 5
    
    result_rows = result_df.collect()
    
    first_row = next(r for r in result_rows if r["transaction_id"] == "TX001")
    assert first_row["consolidated_date"] == "2024-01-15"
    assert first_row["originator_code"] == "CRED"
    assert first_row["amount_usd"] == 1000.00
    assert first_row["transaction_category"] == "DISBURSEMENT"


def test_transformer_handles_currency_conversion(spark_session, sample_transactions_df):
    """Verifica la conversión de monedas para transacciones en EUR."""
    from src.transform.transaction_transformer import TransactionTransformer
    
    transformer = TransactionTransformer(spark_session)
    result_df = transformer.transform(sample_transactions_df)
    
    eur_row = next(r for r in result_df.collect() if r["transaction_id"] == "TX003")
    assert eur_row["amount_eur"] is not None
    assert eur_row["amount_usd"] > 0


def test_transformer_maps_originator_codes(spark_session, sample_transactions_df):
    """Verifica el mapeo correcto de códigos de originador."""
    from src.transform.transaction_transformer import TransactionTransformer
    
    transformer = TransactionTransformer(spark_session)
    result_df = transformer.transform(sample_transactions_df)
    
    rows = result_df.collect()
    originators = {r["originator_code"] for r in rows}
    
    assert "CRED" in originators
    assert "RISK" in originators
    assert "ACCT" in originators


def test_transformer_filters_failed_transactions(spark_session, sample_transactions_df):
    """Verifica que las transacciones fallidas se marcan como inválidas."""
    from src.transform.transaction_transformer import TransactionTransformer
    
    transformer = TransactionTransformer(spark_session)
    result_df = transformer.transform(sample_transactions_df)
    
    failed_tx = next(r for r in result_df.collect() if r["transaction_id"] == "TX005")
    assert failed_tx["is_valid"] == "false"


def test_transformer_preserves_valid_transaction_ids(spark_session, sample_transactions_df):
    """Verifica que los IDs de transacción válidos se preservan."""
    from src.transform.transaction_transformer import TransactionTransformer
    
    transformer = TransactionTransformer(spark_session)
    result_df = transformer.transform(sample_transactions_df)
    
    result_ids = {r["transaction_id"] for r in result_df.collect()}
    expected_ids = {"TX001", "TX002", "TX003", "TX004", "TX005"}
    
    assert result_ids == expected_ids


def test_transformer_handles_empty_dataframe(spark_session):
    """Verifica que el transformer maneja DataFrames vacíos."""
    from src.transform.transaction_transformer import TransactionTransformer
    
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("originator", StringType(), False),
        StructField("account_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), True),
        StructField("transaction_date", TimestampType(), False),
        StructField("transaction_type", StringType(), True),
        StructField("status", StringType(), True),
    ])
    
    empty_df = spark_session.createDataFrame([], schema)
    transformer = TransactionTransformer(spark_session)
    result_df = transformer.transform(empty_df)
    
    assert result_df.count() == 0
// === ARCHIVO: tests/test_quality_rules.py ===
import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime


@pytest.fixture(scope="module")
def spark_session():
    """Crea una sesión de Spark para los tests."""
    spark = SparkSession.builder \
        .appName("test_quality_rules") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def valid_transaction():
    """Transacción válida para pruebas."""
    return {
        "transaction_id": "TX_VALID_001",
        "originator": "CREDIT_ORIGINATOR",
        "account_id": "ACC123",
        "amount": 1000.00,
        "currency": "USD",
        "transaction_date": datetime(2024, 1, 15, 10, 30),
        "transaction_type": "DISBURSEMENT",
        "status": "COMPLETED",
    }


@pytest.fixture
def invalid_amount_transaction():
    """Transacción con monto inválido (cero o negativo)."""
    return {
        "transaction_id": "TX_INVALID_AMOUNT",
        "originator": "RISK_BUREAU",
        "account_id": "ACC456",
        "amount": 0.00,
        "currency": "USD",
        "transaction_date": datetime(2024, 1, 15, 11, 0),
        "transaction_type": "PAYMENT",
        "status": "COMPLETED",
    }


@pytest.fixture
def missing_required_field_transaction():
    """Transacción con campo requerido faltante."""
    return {
        "transaction_id": "TX_MISSING_FIELD",
        "originator": None,
        "account_id": "ACC789",
        "amount": 500.00,
        "currency": "USD",
        "transaction_date": datetime(2024, 1, 15, 12, 15),
        "transaction_type": "TRANSFER",
        "status": "COMPLETED",
    }


@pytest.fixture
def failed_status_transaction():
    """Transacción con estado fallido."""
    return {
        "transaction_id": "TX_FAILED_STATUS",
        "originator": "ACCOUNTING_CONSOLIDATOR",
        "account_id": "ACC999",
        "amount": 2500.00,
        "currency": "EUR",
        "transaction_date": datetime(2024, 1, 15, 13, 0),
        "transaction_type": "PAYMENT",
        "status": "FAILED",
    }


def test_amount_must_be_positive(spark_session, valid_transaction, invalid_amount_transaction):
    """Verifica que el monto debe ser positivo."""
    from src.transform.quality_rules import QualityRules
    
    quality = QualityRules(spark_session)
    
    valid_df = spark_session.createDataFrame([valid_transaction])
    invalid_df = spark_session.createDataFrame([invalid_amount_transaction])
    
    valid_result = quality.validate_amount(valid_df)
    invalid_result = quality.validate_amount(invalid_df)
    
    assert valid_result.filter("is_valid = 'true'").count() == 1
    assert invalid_result.filter("is_valid = 'false'").count() == 1


def test_required_fields_must_be_present(spark_session, valid_transaction, missing_required_field_transaction):
    """Verifica que los campos requeridos no pueden ser nulos."""
    from src.transform.quality_rules import QualityRules
    
    quality = QualityRules(spark_session)
    
    valid_df = spark_session.createDataFrame([valid_transaction])
    invalid_df = spark_session.createDataFrame([missing_required_field_transaction])
    
    valid_result = quality.validate_required_fields(valid_df)
    invalid_result = quality.validate_required_fields(invalid_df)
    
    assert valid_result.filter("is_valid = 'true'").count() == 1
    assert invalid_result.filter("is_valid = 'false'").count() == 1


def test_status_must_be_completed(spark_session, valid_transaction, failed_status_transaction):
    """Verifica que solo transacciones con status COMPLETED son válidas."""
    from src.transform.quality_rules import QualityRules
    
    quality = QualityRules(spark_session)
    
    valid_df = spark_session.createDataFrame([valid_transaction])
    invalid_df = spark_session.createDataFrame([failed_status_transaction])
    
    valid_result = quality.validate_status(valid_df)
    invalid_result = quality.validate_status(invalid_df)
    
    assert valid_result.filter("is_valid = 'true'").count() == 1
    assert invalid_result.filter("is_valid = 'false'").count() == 1


def test_all_quality_rules_applied(spark_session):
    """Verifica que todas las reglas de calidad se aplican correctamente."""
    from src.transform.quality_rules import QualityRules
    
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("originator", StringType(), True),
        StructField("account_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), True),
        StructField("transaction_date", TimestampType(), False),
        StructField("transaction_type", StringType(), True),
        StructField("status", StringType(), True),
    ])
    
    data = [
        ("TX001", "CREDIT_ORIGINATOR", "ACC123", 1000.00, "USD", datetime(2024, 1, 15), "DISBURSEMENT", "COMPLETED"),
        ("TX002", None, "ACC456", 0.00, "USD", datetime(2024, 1, 15), "PAYMENT", "FAILED"),
    ]
    
    df = spark_session.createDataFrame(data, schema)
    quality = QualityRules(spark_session)
    result_df = quality.apply_all_rules(df)
    
    results = result_df.collect()
    valid_count = sum(1 for r in results if r["is_valid"] == "true")
    invalid_count = sum(1 for r in results if r["is_valid"] == "false")
    
    assert valid_count == 1
    assert invalid_count == 1


def test_quarantine_metadata_includes_rejection_reason(spark_session):
    """Verifica que la cuarentena incluye la razón del rechazo."""
    from src.transform.quality_rules import QualityRules
    
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("originator", StringType(), True),
        StructField("account_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("currency", StringType(), True),
        StructField("transaction_date", TimestampType(), False),
        StructField("transaction_type", StringType(), True),
        StructField("status", StringType(), True),
    ])
    
    data = [
        ("TX_REJECT", "RISK_BUREAU", "ACC999", -100.00, "USD", datetime(2024, 1, 15), "PAYMENT", "COMPLETED"),
    ]
    
    df = spark_session.createDataFrame(data, schema)
    quality = QualityRules(spark_session)
    result_df = quality.apply_all_rules(df)
    
    rejected_row = result_df.filter("is_valid = 'false'").first()
    assert rejected_row is not None
    assert rejected_row["rejection_reason"] is not None
    assert len(rejected_row["rejection_reason"]) > 0


def test_quality_rules_pandas_dataframe():
    """Verifica que las reglas de calidad funcionan con pandas DataFrame."""
    from src.transform.quality_rules import QualityRules
    
    data = {
        "transaction_id": ["TX001", "TX002", "TX003"],
        "amount": [1000.00, -50.00, 0.00],
        "status": ["COMPLETED", "COMPLETED", "FAILED"],
    }
    df = pd.DataFrame(data)
    
    quality = QualityRules()
    result = quality.validate_pandas(df)
    
    assert result["is_valid"].tolist() == [True, False, False]
// === ARCHIVO: tests/test_idempotency_manager.py ===
import pytest
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from datetime import datetime
from unittest.mock import Mock, patch, MagicMock
import os
import json


@pytest.fixture(scope="module")
def spark_session():
    """Crea una sesión de Spark para los tests."""
    spark = SparkSession.builder \
        .appName("test_idempotency_manager") \
        .master("local[2]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .getOrCreate()
    yield spark
    spark.stop()


@pytest.fixture
def processed_transactions_set():
    """Conjunto de transacciones ya procesadas."""
    return {
        "TX001_2024-01-15",
        "TX002_2024-01-15",
        "TX003_2024-01-14",
    }


@pytest.fixture
def new_transactions():
    """Nuevas transacciones a procesar."""
    return [
        {
            "transaction_id": "TX001",
            "originator": "CREDIT_ORIGINATOR",
            "account_id": "ACC123",
            "amount": 1000.00,
            "transaction_date": datetime(2024, 1, 15),
        },
        {
            "transaction_id": "TX004",
            "originator": "RISK_BUREAU",
            "account_id": "ACC456",
            "amount": 2500.50,
            "transaction_date": datetime(2024, 1, 15),
        },
    ]


@pytest.fixture
def duplicate_transactions():
    """Transacciones duplicadas para pruebas."""
    return [
        {
            "transaction_id": "TX001",
            "originator": "CREDIT_ORIGINATOR",
            "account_id": "ACC123",
            "amount": 1000.00,
            "transaction_date": datetime(2024, 1, 15),
        },
        {
            "transaction_id": "TX001",
            "originator": "CREDIT_ORIGINATOR",
            "account_id": "ACC123",
            "amount": 1000.00,
            "transaction_date": datetime(2024, 1, 15),
        },
    ]


def test_detect_duplicate_transactions(spark_session, processed_transactions_set, new_transactions):
    """Verifica que se detectan transacciones duplicadas."""
    from src.utils.idempotency_manager import IdempotencyManager
    
    manager = IdempotencyManager(spark_session)
    
    new_df = spark_session.createDataFrame(new_transactions)
    result = manager.filter_duplicates(new_df, processed_transactions_set)
    
    assert result.count() == 1
    assert result.first()["transaction_id"] == "TX004"


def test_allow_new_transactions(spark_session, processed_transactions_set, new_transactions):
    """Verifica que las transacciones nuevas se permiten."""
    from src.utils.idempotency_manager import IdempotencyManager
    
    manager = IdempotencyManager(spark_session)
    
    new_tx = [new_transactions[1]]
    new_df = spark_session.createDataFrame(new_tx)
    result = manager.filter_duplicates(new_df, processed_transactions_set)
    
    assert result.count() == 1
    assert result.first()["transaction_id"] == "TX004"


def test_mark_transactions_as_processed(spark_session, new_transactions):
    """Verifica que las transacciones se marcan como procesadas."""
    from src.utils.idempotency_manager import IdempotencyManager
    
    manager = IdempotencyManager(spark_session)
    
    new_df = spark_session.createDataFrame(new_transactions)
    processed_keys = manager.mark_as_processed(new_df)
    
    assert len(processed_keys) == 2
    assert "TX001_2024-01-15" in processed_keys
    assert "TX004_2024-01-15" in processed_keys


def test_generate_idempotency_key(spark_session):
    """Verifica la generación de claves de idempotencia."""
    from src.utils.idempotency_manager import IdempotencyManager
    
    manager = IdempotencyManager(spark_session)
    
    key = manager.generate_idempotency_key("TX123", datetime(2024, 1, 15))
    assert key == "TX123_2024-01-15"


def test_persisted_state_survives_restart(spark_session, tmp_path):
    """Verifica que el estado persiste entre ejecuciones."""
    from src.utils.idempotency_manager import IdempotencyManager
    
    state_file = tmp_path / "idempotency_state.json"
    initial_state = {"TX001_2024-01-15": True, "TX002_2024-01-15": True}
    
    with open(state_file, "w") as f:
        json.dump(initial_state, f)
    
    manager = IdempotencyManager(spark_session, state_path=str(state_file))
    loaded_state = manager.load_state()
    
    assert "TX001_2024-01-15" in loaded_state
    assert "TX002_2024-01-15" in loaded_state


def test_empty_transactions_list(spark_session, processed_transactions_set):
    """Verifica el manejo de lista vacía de transacciones."""
    from src.utils.idempotency_manager import IdempotencyManager
    
    schema = StructType([
        StructField("transaction_id", StringType(), False),
        StructField("originator", StringType(), True),
        StructField("account_id", StringType(), False),
        StructField("amount", DoubleType(), False),
        StructField("transaction_date", TimestampType(), False),
    ])
    
    empty_df = spark_session.createDataFrame([], schema)
    manager = IdempotencyManager(spark_session)
    result = manager.filter_duplicates(empty_df, processed_transactions_set)
    
    assert result.count() == 0


def test_duplicate_within_same_batch(spark_session, duplicate_transactions):
    """Verifica que los duplicados dentro del mismo batch se manejan."""
    from src.utils.idempotency_manager import IdempotencyManager
    
    manager = IdempotencyManager(spark_session)
    
    duplicate_df = spark_session.createDataFrame(duplicate_transactions)
    result = manager.remove_duplicates_within_batch(duplicate_df)
    
    assert result.count() == 1


def test_idempotency_key_format_with_different_dates(spark_session):
    """Verifica el formato de clave con diferentes fechas."""
    from src.utils.idempotency_manager import IdempotencyManager
    
    manager = IdempotencyManager(spark_session)
    
    key1 = manager.generate_idempotency_key("TX001", datetime(2024, 1, 15))
    key2 = manager.generate_idempotency_key("TX001", datetime(2024, 1, 16))
    key3 = manager.generate_idempotency_key("TX002", datetime(2024, 1, 15))
    
    assert key1 != key2
    assert key1 != key3
    assert key2 != key3


// === ARCHIVO: data/quarantine/quarantine_schema.json ===
{
  "$schema": "http://json-schema.org/draft-07/schema#",
  "title": "Quarantined Transaction Record Schema",
  "description": "Esquema para registros de transacciones en cuarentena por fallar reglas de calidad. Incluye datos originales y metadatos de error para análisis posterior.",
  "type": "object",
  "properties": {
    "transaction_id": {
      "type": "string",
      "description": "Identificador único de la transacción original del sistema origen"
    },
    "source_system": {
      "type": "string",
      "enum": ["credit_originator", "risk_bureau", "accounting_consolidator"],
      "description": "Sistema origen que generó la transacción"
    },
    "transaction_date": {
      "type": "string",
      "format": "date",
      "description": "Fecha de la transacción en formato ISO 8601"
    },
    "transaction_timestamp": {
      "type": "string",
      "format": "date-time",
      "description": "Timestamp completo de cuando se procesó la transacción"
    },
    "amount": {
      "type": "number",
      "description": "Monto de la transacción"
    },
    "currency": {
      "type": "string",
      "description": "Código de moneda ISO 4217"
    },
    "account_id": {
      "type": "string",
      "description": "Identificador de cuenta involucrada"
    },
    "account_type": {
      "type": ["string", "null"],
      "description": "Tipo de cuenta si aplica"
    },
    "customer_id": {
      "type": ["string", "null"],
      "description": "Identificador del cliente"
    },
    "transaction_type": {
      "type": ["string", "null"],
      "description": "Tipo de transacción"
    },
    "status": {
      "type": ["string", "null"],
      "description": "Estado de la transacción"
    },
    "quarantine_metadata": {
      "type": "object",
      "description": "Metadatos específicos de la cuarentena",
      "properties": {
        "quarantine_timestamp": {
          "type": "string",
          "format": "date-time",
          "description": "Timestamp de cuando la transacción fue puesta en cuarentena"
        },
        "processing_batch_id": {
          "type": "string",
          "description": "Identificador del batch de procesamiento que generó la cuarentena"
        },
        "rule_violations": {
          "type": "array",
          "description": "Lista de reglas de calidad que la transacción violó",
          "items": {
            "type": "object",
            "properties": {
              "rule_id": {
                "type": "string",
                "description": "Identificador único de la regla de calidad"
              },
              "rule_name": {
                "type": "string",
                "description": "Nombre descriptivo de la regla"
              },
              "rule_category": {
                "type": "string",
                "enum": ["completeness", "validity", "consistency", "uniqueness", "timeliness"],
                "description": "Categoría de la regla de calidad"
              },
              "severity": {
                "type": "string",
                "enum": ["error", "warning"],
                "description": "Severidad de la violación"
              },
              "error_message": {
                "type": "string",
                "description": "Mensaje descriptivo del error"
              },
              "expected_value": {
                "type": ["string", "number", "null"],
                "description": "Valor esperado según la regla"
              },
              "actual_value": {
                "type": ["string", "number", "null"],
                "description": "Valor actual que violó la regla"
              }
            },
            "required": ["rule_id", "rule_name", "rule_category", "severity", "error_message"]
          }
        },
        "rejection_reason": {
          "type": "string",
          "description": "Descripción consolidada de por qué la transacción fue rechazada"
        },
        "retry_count": {
          "type": "integer",
          "description": "Número de intentos de reprocesamiento",
          "minimum": 0
        },
        "last_retry_timestamp": {
          "type": ["string", "null"],
          "format": "date-time",
          "description": "Timestamp del último intento de reprocesamiento"
        },
        "manual_review_required": {
          "type": "boolean",
          "description": "Indica si la transacción requiere revisión manual"
        },
        "original_raw_data": {
          "type": "object",
          "description": "Copia del registro original tal cual llegó del origen",
          "additionalProperties": true
        }
      },
      "required": ["quarantine_timestamp", "processing_batch_id", "rule_violations"]
    },
    "lineage": {
      "type": "object",
      "description": "Información de linaje para trazabilidad",
      "properties": {
        "extraction_timestamp": {
          "type": ["string", "null"],
          "format": "date-time",
          "description": "Timestamp de extracción del sistema origen"
        },
        "transformation_pipeline_version": {
          "type": "string",
          "description": "Versión del pipeline de transformación que procesó la transacción"
        },
        "environment": {
          "type": "string",
          "description": "Ambiente de procesamiento (dev, staging, prod)"
        }
      }
    }
  },
  "required": ["transaction_id", "source_system", "quarantine_metadata"],
  "additionalProperties": false
}

// === ARCHIVO: README.md ===
# Transaction Consolidation Pipeline

Pipeline ETL para consolidación diaria de transacciones financieras en un modelo analítico. El sistema procesa datos de tres fuentes: originador de créditos, buró de riesgos y consolidador contable.

## Requisitos

- Python 3.13+
- Apache Airflow 2.9+
- PySpark 3.5+
- Delta Lake 0.17+
- PostgreSQL (como metastore de Hive)

## Instalación

```bash
pip install -r requirements.txt
```

## Configuración

El pipeline se configura mediante el archivo `conf/config.yaml`. Las variables de entorno de Airflow complementan la configuración:

| Variable | Descripción | Default |
|----------|-------------|---------|
| `ENVIRONMENT` | Ambiente de ejecución | `dev` |
| `S3_BUCKET` | Bucket S3 para datos | - |
| `DB_HOST` | Host de PostgreSQL | `localhost` |
| `DB_PORT` | Puerto de PostgreSQL | `5432` |
| `DB_NAME` | Nombre de base de datos | `analytics` |

## Arquitectura del Pipeline

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   EXTRACT       │    │   TRANSFORM     │    │     LOAD        │
│                 │    │                 │    │                 │
│ • Credit Origin │───▶│ • Validación    │───▶│ • Modelo anal.  │
│ • Risk Bureau   │    │ • Enriquecimiento│   │ • Partitioning  │
│ • Accounting    │    │ • Cuarentena    │    │ • Delta Lake    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

### Etapas del ETL

1. **Extract (src/extract/)**: Lectura de las tres fuentes de datos
   - `credit_originator_reader.py`: Transacciones del originador de créditos
   - `risk_bureau_reader.py`: Datos del buró de riesgos
   - `accounting_consolidator_reader.py`: Consolidador contable

2. **Transform (src/transform/)**: Transformaciones y reglas de calidad
   - `transaction_transformer.py`: Transformaciones de negocio
   - `quality_rules.py`: Reglas de calidad implementadas

3. **Load (src/load/)**: Escritura al modelo analítico
   - `analytical_model_writer.py`: Escritor con particionado y Delta Lake

### Cuarentena de Registros Inválidos

Los registros que fallan las reglas de calidad se envían a la ruta de cuarentena en `data/quarantine/`. Cada registro incluye:

- Datos originales de la transacción
- Metadatos de error (regla violada, severidad, mensaje)
- Información de linaje para trazabilidad
- Flag para revisión manual

El esquema de cuarentena está definido en `data/quarantine/quarantine_schema.json`.

## Reglas de Calidad

El pipeline implementa las siguientes reglas de calidad como funciones puras en PySpark:

| ID | Nombre | Categoría | Severidad | Descripción |
|----|--------|-----------|-----------|-------------|
| Q001 | transaction_id_not_null | Completitud | Error | El identificador de transacción no puede ser nulo |
| Q002 | amount_positive | Validez | Error | El monto debe ser mayor a cero |
| Q003 | currency_valid | Validez | Error | Código de moneda debe ser válido ISO 4217 |
| Q004 | date_not_future | Validez | Error | La fecha de transacción no puede ser futura |
| Q005 | account_id_format | Validez | Error | Formato de ID de cuenta inválido |
| Q006 | transaction_uniqueness | Unicidad | Advertencia | Transacción duplicada detectada |
| Q007 | customer_id_consistency | Consistencia | Advertencia | Customer ID inconsistente entre fuentes |
| Q008 | amount_reasonable | Validez | Advertencia | Monto fuera de rangos razonables |
| Q009 | transaction_type_valid | Validez | Error | Tipo de transacción no reconocido |
| Q010 | timestamp_valid | Temporalidad | Error | Timestamp inválido o malformado |

### Categorías de Reglas

- **Completitud**: Validan que campos obligatorios existan
- **Validez**: Validan formato y rango de valores
- **Consistencia**: Validan coherencia entre campos y fuentes
- **Unicidad**: Validan que no haya duplicados
- **Temporalidad**: Validan que los datos estén en tiempo apropiado

## Ejecución

### Ejecución como script standalone

```bash
# Procesar transacciones del día anterior
python -m src.main --date $(date -d "yesterday" +%Y-%m-%d)
```

### Ejecución via Airflow

El DAG `dags/consolidate_transactions_dag.py` orquesta la ejecución completa:

```bash
# Iniciar Airflow
airflow webserver -p 8080
airflow scheduler

# Trigger manual del DAG
airflow dags trigger consolidate_transactions
```

### Verificación de calidad

```bash
# Ejecutar solo validación de transacciones
python -m src.utils.validator --date 2024-01-15
```

## Tests

```bash
# Ejecutar todos los tests
pytest -q

# Ejecutar tests de transformación
pytest tests/test_transaction_transformer.py -v

# Ejecutar tests de reglas de calidad
pytest tests/test_quality_rules.py -v

# Ejecutar tests de idempotencia
pytest tests/test_idempotency_manager.py -v
```

## Modelo Analítico

El modelo analítico se almacena en formato Parquet con particionado por fecha:

```
s3://bucket/analytics/transactions/
├── transaction_date=2024-01-15/
│   ├── part-00000.parquet
│   └── ...
├── transaction_date=2024-01-16/
│   └── ...
```

### Esquema del modelo

| Campo | Tipo | Descripción |
|-------|------|-------------|
| transaction_id | string | Identificador único |
| source_system | string | Sistema origen |
| transaction_date | date | Fecha de transacción |
| amount | decimal | Monto de la transacción |
| currency | string | Código de moneda |
| account_id | string | ID de cuenta |
| customer_id | string | ID de cliente |
| transaction_type | string | Tipo de transacción |
| status | string | Estado procesando/procesada |
| processed_timestamp | timestamp | Timestamp de procesamiento |

## Idempotencia

El pipeline garantiza idempotencia mediante:
- Identificador único de transacción como clave primaria
- Control de idempotencia en `src/utils/idempotency_manager.py`
- Estrategia de reintento con marca de tiempo
- Upsert en Delta Lake para evitar duplicados

## Logging

El sistema usa configuración estructurada de logging en `src/utils/logging_config.py`. Los logs incluyen:
- Identificador de batch
- Timestamp de cada etapa
- Conteo de registros válidos/inválidos
- Detalles de reglas violadas

## Troubleshooting

### Transacciones en cuarentena

1. Consultar registros en `data/quarantine/`
2. Revisar campo `rule_violations` para identificar reglas fallidas
3. Corregir datos en sistema origen o ajustar reglas
4. Re-procesar con `retry_count` actualizado

### Problemas de rendimiento

- Verificar particionado por fecha
- Ajustar número de particiones Spark
- Revisar tamaño de batches
- Monitorear uso de memoria executor

### Errores de Delta Lake

- Verificar permisos en S3
- Confirmar versión de delta-rs compatible
- Revisar esquema del metastore


string — TODOS los archivos del proyecto separados por '// === ARCHIVO: ruta/del/archivo ===' o null si la propuesta arquitectónica no aplica
```
