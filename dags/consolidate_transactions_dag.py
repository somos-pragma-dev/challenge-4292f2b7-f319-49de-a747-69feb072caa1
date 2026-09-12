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