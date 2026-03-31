import json
import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F


@dataclass
class BootstrapConfig:
    repo_root: str
    project_config: Dict[str, Any]
    table_configs: List[Dict[str, Any]]


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload = {
            "timestamp": datetime.utcnow().isoformat(timespec="seconds") + "Z",
            "level": record.levelname,
            "message": record.getMessage(),
        }
        for attr in [
            "notebook_name",
            "table_name",
            "rows_read",
            "rows_written",
            "duration_seconds",
            "status",
            "error_message",
            "batch_id",
            "run_id",
        ]:
            value = getattr(record, attr, None)
            if value is not None:
                payload[attr] = value
        return json.dumps(payload, ensure_ascii=False)


class NotebookLogger:
    def __init__(self, notebook_name: str) -> None:
        self.logger = logging.getLogger(notebook_name)
        self.logger.setLevel(logging.INFO)
        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(JsonFormatter())
            self.logger.addHandler(handler)
        self.notebook_name = notebook_name

    def info(self, message: str, **kwargs: Any) -> None:
        extra = {"notebook_name": self.notebook_name, **kwargs}
        self.logger.info(message, extra=extra)

    def error(self, message: str, **kwargs: Any) -> None:
        extra = {"notebook_name": self.notebook_name, **kwargs}
        self.logger.error(message, extra=extra)


def ensure_repo_root_on_path(marker_file: str = "conf/project_config.json") -> str:
    cwd = Path.cwd().resolve()
    for candidate in [cwd, *cwd.parents]:
        marker = candidate / marker_file
        if marker.exists():
            candidate_str = str(candidate)
            if candidate_str not in sys.path:
                sys.path.insert(0, candidate_str)
            return candidate_str
    raise FileNotFoundError(
        f"Unable to resolve repository root. Could not find '{marker_file}' from {cwd}."
    )


def load_json_file(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        return json.load(handle)


def get_notebook_context() -> Dict[str, str]:
    try:
        context = dbutils.notebook.entry_point.getDbutils().notebook().getContext()
        return {
            "notebook_path": context.notebookPath().getOrElse("unknown"),
            "run_id": context.currentRunId().toString(),
            "job_id": context.jobId().toString() if context.jobId().isDefined() else "interactive",
        }
    except Exception:
        return {
            "notebook_path": "local-or-non-databricks",
            "run_id": "interactive",
            "job_id": "interactive",
        }


def build_abfss_path(container: str, storage_account: str, relative_path: str) -> str:
    return f"abfss://{container}@{storage_account}.dfs.core.windows.net/{relative_path}"


def ensure_metadata_tables(spark: SparkSession, catalog_name: str, metadata_schema: str) -> None:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{metadata_schema}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.{metadata_schema}.pipeline_logs (
            event_timestamp TIMESTAMP,
            notebook_name STRING,
            table_name STRING,
            rows_read BIGINT,
            rows_written BIGINT,
            duration_seconds DOUBLE,
            status STRING,
            error_message STRING,
            batch_id STRING,
            run_id STRING
        )
        USING DELTA
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.{metadata_schema}.data_quality_log (
            check_timestamp TIMESTAMP,
            table_name STRING,
            check_name STRING,
            status STRING,
            details STRING
        )
        USING DELTA
        """
    )

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {catalog_name}.{metadata_schema}.pipeline_watermarks (
            table_name STRING NOT NULL,
            last_load_timestamp TIMESTAMP,
            last_row_count BIGINT,
            load_status STRING,
            last_updated_at TIMESTAMP
        )
        USING DELTA
        """
    )


def persist_pipeline_log(
    spark: SparkSession,
    catalog_name: str,
    metadata_schema: str,
    notebook_name: str,
    table_name: str,
    rows_read: int,
    rows_written: int,
    duration_seconds: float,
    status: str,
    error_message: Optional[str],
    batch_id: str,
    run_id: str,
) -> None:
    payload = [
        (
            datetime.utcnow(),
            notebook_name,
            table_name,
            int(rows_read),
            int(rows_written),
            float(duration_seconds),
            status,
            error_message,
            str(batch_id),
            str(run_id),
        )
    ]
    df = spark.createDataFrame(
        payload,
        schema="""
            event_timestamp TIMESTAMP,
            notebook_name STRING,
            table_name STRING,
            rows_read BIGINT,
            rows_written BIGINT,
            duration_seconds DOUBLE,
            status STRING,
            error_message STRING,
            batch_id STRING,
            run_id STRING
        """,
    )
    df.write.format("delta").mode("append").saveAsTable(
        f"{catalog_name}.{metadata_schema}.pipeline_logs"
    )


def persist_quality_log(
    spark: SparkSession,
    catalog_name: str,
    metadata_schema: str,
    table_name: str,
    check_name: str,
    status: str,
    details: str,
) -> None:
    payload = [(datetime.utcnow(), table_name, check_name, status, details)]
    df = spark.createDataFrame(
        payload,
        schema="""
            check_timestamp TIMESTAMP,
            table_name STRING,
            check_name STRING,
            status STRING,
            details STRING
        """,
    )
    df.write.format("delta").mode("append").saveAsTable(
        f"{catalog_name}.{metadata_schema}.data_quality_log"
    )


def normalise_columns(df: DataFrame, table_name: str) -> DataFrame:
    if "_rescued_data" in df.columns:
        df = df.drop("_rescued_data")

    if table_name == "DimUser" and "user_name" in df.columns:
        df = df.withColumn("user_name", F.upper(F.col("user_name")))

    if table_name == "DimTrack" and "track_name" in df.columns:
        df = df.withColumn("track_name", F.regexp_replace(F.col("track_name"), "-", " "))

    if table_name == "FactStream" and "stream_timestamp" in df.columns:
        df = df.withColumn("event_date", F.to_date(F.col("stream_timestamp")))

    return df


def deduplicate_latest(df: DataFrame, primary_keys: List[str], order_column: str) -> DataFrame:
    if not primary_keys:
        return df
    window = Window.partitionBy(*[F.col(pk) for pk in primary_keys]).orderBy(F.col(order_column).desc())
    return df.withColumn("_row_num", F.row_number().over(window)).filter(F.col("_row_num") == 1).drop("_row_num")


def count_rows_written_from_operation_metrics(operation_metrics: Optional[Dict[str, Any]]) -> int:
    if not operation_metrics:
        return 0
    metric_keys = [
        "numTargetRowsInserted",
        "numTargetRowsUpdated",
        "numTargetRowsDeleted",
        "numOutputRows",
        "numInsertedRows",
        "numUpdatedRows",
        "numDeletedRows",
    ]
    total = 0
    for key in metric_keys:
        value = operation_metrics.get(key)
        if value is None:
            continue
        try:
            total += int(value)
        except (TypeError, ValueError):
            continue
    return total


def get_delta_operation_metrics(delta_table: DeltaTable) -> Dict[str, Any]:
    history_row = delta_table.history(1).select("operationMetrics").collect()
    if not history_row:
        return {}
    metrics = history_row[0]["operationMetrics"]
    if metrics is None:
        return {}
    if isinstance(metrics, dict):
        return metrics
    try:
        return json.loads(metrics)
    except Exception:
        return {}


def merge_batch_to_delta(
    spark: SparkSession,
    batch_df: DataFrame,
    target_table: str,
    primary_keys: List[str],
    partition_by: Optional[List[str]] = None,
) -> int:
    partition_by = partition_by or []
    batch_count = batch_df.count()

    if spark.catalog.tableExists(target_table):
        target = DeltaTable.forName(spark, target_table)
        merge_condition = " AND ".join([f"t.{key} = s.{key}" for key in primary_keys])
        (
            target.alias("t")
            .merge(batch_df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        metrics = get_delta_operation_metrics(target)
        rows_written = count_rows_written_from_operation_metrics(metrics)
        return rows_written if rows_written > 0 else batch_count

    writer = batch_df.write.format("delta").mode("overwrite")
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.saveAsTable(target_table)
    return batch_count


def create_or_replace_temp_view(df: DataFrame, view_name: str) -> None:
    df.createOrReplaceTempView(view_name)
