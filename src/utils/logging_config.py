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