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