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