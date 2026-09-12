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