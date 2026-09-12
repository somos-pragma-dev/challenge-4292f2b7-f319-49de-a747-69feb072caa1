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