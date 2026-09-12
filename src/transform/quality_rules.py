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