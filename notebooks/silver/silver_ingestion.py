# Databricks notebook source
# COMMAND ----------
import os
import sys
import time
from typing import Dict, Any, List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

repo_root = None
for candidate in [os.getcwd(), os.path.abspath(os.path.join(os.getcwd(), "..", "..")), "/Workspace/Repos"]:
    if candidate and os.path.isdir(candidate):
        if candidate not in sys.path:
            sys.path.insert(0, candidate)

from utils.transformations import (
    NotebookLogger,
    build_abfss_path,
    deduplicate_latest,
    ensure_metadata_tables,
    ensure_repo_root_on_path,
    get_notebook_context,
    load_json_file,
    merge_batch_to_delta,
    normalise_columns,
    persist_pipeline_log,
)

repo_root = ensure_repo_root_on_path()
project_config = load_json_file(os.path.join(repo_root, "conf", "project_config.json"))
table_configs = load_json_file(os.path.join(repo_root, "conf", "table_configs.json"))["tables"]

catalog_name = project_config["catalog_name"]
silver_schema = project_config["silver_schema"]
metadata_schema = project_config["metadata_schema"]
storage_account = project_config["storage_account"]
bronze_container = project_config["bronze_container"]
silver_container = project_config["silver_container"]

context = get_notebook_context()
logger = NotebookLogger("silver_ingestion")

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{silver_schema}")
ensure_metadata_tables(spark, catalog_name, metadata_schema)

# COMMAND ----------
def process_microbatch(batch_df: DataFrame, batch_id: int, table_cfg: Dict[str, Any]) -> None:
    table_name = table_cfg["table_name"]
    primary_keys: List[str] = table_cfg["primary_keys"]
    order_column: str = table_cfg["deduplicate_order_column"]
    target_table = f"{catalog_name}.{silver_schema}.{table_cfg['silver_table']}"
    start = time.time()
    rows_read = batch_df.count()

    try:
        conformed_df = normalise_columns(batch_df, table_name)
        conformed_df = deduplicate_latest(conformed_df, primary_keys, order_column)
        rows_written = merge_batch_to_delta(
            spark=spark,
            batch_df=conformed_df,
            target_table=target_table,
            primary_keys=primary_keys,
            partition_by=table_cfg.get("partition_by", []),
        )

        persist_pipeline_log(
            spark=spark,
            catalog_name=catalog_name,
            metadata_schema=metadata_schema,
            notebook_name="silver_ingestion",
            table_name=table_name,
            rows_read=rows_read,
            rows_written=rows_written,
            duration_seconds=time.time() - start,
            status="SUCCESS",
            error_message=None,
            batch_id=str(batch_id),
            run_id=context["run_id"],
        )

        logger.info(
            f"Silver merge completed for {table_name}",
            table_name=table_name,
            rows_read=rows_read,
            rows_written=rows_written,
            duration_seconds=round(time.time() - start, 3),
            status="SUCCESS",
            batch_id=str(batch_id),
            run_id=context["run_id"],
        )
    except Exception as exc:
        persist_pipeline_log(
            spark=spark,
            catalog_name=catalog_name,
            metadata_schema=metadata_schema,
            notebook_name="silver_ingestion",
            table_name=table_name,
            rows_read=rows_read,
            rows_written=0,
            duration_seconds=time.time() - start,
            status="FAILED",
            error_message=str(exc),
            batch_id=str(batch_id),
            run_id=context["run_id"],
        )
        logger.error(
            f"Silver merge failed for {table_name}",
            table_name=table_name,
            rows_read=rows_read,
            rows_written=0,
            duration_seconds=round(time.time() - start, 3),
            status="FAILED",
            error_message=str(exc),
            batch_id=str(batch_id),
            run_id=context["run_id"],
        )
        raise

# COMMAND ----------
for table_cfg in table_configs:
    table_name = table_cfg["table_name"]
    source_path = build_abfss_path(bronze_container, storage_account, table_cfg["bronze_path"])
    schema_path = build_abfss_path(silver_container, storage_account, f"{table_cfg['target_path']}/schema")
    checkpoint_path = build_abfss_path(silver_container, storage_account, f"{table_cfg['target_path']}/checkpoint")

    logger.info(
        f"Starting Auto Loader stream for {table_name}",
        table_name=table_name,
        rows_read=0,
        rows_written=0,
        duration_seconds=0.0,
        status="STARTED",
        batch_id="N/A",
        run_id=context["run_id"],
    )

    streaming_df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_path)
        .option("cloudFiles.schemaEvolutionMode", "failOnNewColumns")
        .option("rescuedDataColumn", "_rescued_data")
        .load(source_path)
    )

    try:
        query = (
            streaming_df.writeStream
            .queryName(f"silver_{table_name.lower()}_available_now")
            .foreachBatch(lambda batch_df, batch_id, cfg=table_cfg: process_microbatch(batch_df, batch_id, cfg))
            .option("checkpointLocation", checkpoint_path)
            .trigger(availableNow=True)
            .start()
        )
        query.awaitTermination()
    except Exception as exc:
        logger.error(
            f"Streaming write failed for {table_name}",
            table_name=table_name,
            rows_read=0,
            rows_written=0,
            duration_seconds=0.0,
            status="FAILED",
            error_message=str(exc),
            batch_id="N/A",
            run_id=context["run_id"],
        )
        raise

# COMMAND ----------
display(spark.sql(f"SHOW TABLES IN {catalog_name}.{silver_schema}"))



