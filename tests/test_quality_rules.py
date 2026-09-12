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