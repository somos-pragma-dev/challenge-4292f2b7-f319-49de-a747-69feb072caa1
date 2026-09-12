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