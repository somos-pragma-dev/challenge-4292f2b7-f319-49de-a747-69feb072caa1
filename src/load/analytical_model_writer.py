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