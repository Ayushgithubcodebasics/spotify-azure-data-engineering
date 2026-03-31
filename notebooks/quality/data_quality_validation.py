# Databricks notebook source
# COMMAND ----------
import os
import sys

from pyspark.sql import functions as F

for candidate in [os.getcwd(), os.path.abspath(os.path.join(os.getcwd(), "..", "..")), "/Workspace/Repos"]:
    if candidate and os.path.isdir(candidate):
        if candidate not in sys.path:
            sys.path.insert(0, candidate)

from utils.transformations import (
    NotebookLogger,
    ensure_metadata_tables,
    ensure_repo_root_on_path,
    load_json_file,
    persist_quality_log,
)

repo_root = ensure_repo_root_on_path()
project_config = load_json_file(os.path.join(repo_root, "conf", "project_config.json"))
table_configs = load_json_file(os.path.join(repo_root, "conf", "table_configs.json"))["tables"]

catalog_name = project_config["catalog_name"]
silver_schema = project_config["silver_schema"]
metadata_schema = project_config["metadata_schema"]

ensure_metadata_tables(spark, catalog_name, metadata_schema)
logger = NotebookLogger("data_quality_validation")

# COMMAND ----------
def run_pk_null_check(table_name: str, primary_keys: list[str]) -> None:
    df = spark.table(f"{catalog_name}.{silver_schema}.{table_name}")
    total_rows = df.count()

    for pk in primary_keys:
        null_rows = df.filter(F.col(pk).isNull()).count()
        null_rate = 0.0 if total_rows == 0 else round((null_rows / total_rows) * 100, 4)
        status = "PASS" if null_rows == 0 else "FAIL"
        details = f"column={pk}, null_rows={null_rows}, total_rows={total_rows}, null_rate={null_rate}%"
        persist_quality_log(spark, catalog_name, metadata_schema, table_name, f"pk_null_rate_{pk}", status, details)
        if status == "FAIL":
            raise ValueError(f"Primary key null check failed for {table_name}.{pk}: {details}")

# COMMAND ----------
def run_referential_integrity_checks() -> None:
    fact = spark.table(f"{catalog_name}.{silver_schema}.FactStream")
    users = spark.table(f"{catalog_name}.{silver_schema}.DimUser").select("user_id").distinct()
    tracks = spark.table(f"{catalog_name}.{silver_schema}.DimTrack").select("track_id").distinct()

    orphan_users = fact.select("user_id").distinct().join(users, "user_id", "left_anti")
    orphan_tracks = fact.select("track_id").distinct().join(tracks, "track_id", "left_anti")

    orphan_user_count = orphan_users.count()
    orphan_track_count = orphan_tracks.count()

    persist_quality_log(
        spark,
        catalog_name,
        metadata_schema,
        "FactStream",
        "referential_integrity_user_id",
        "PASS" if orphan_user_count == 0 else "FAIL",
        f"orphan_user_ids={orphan_user_count}",
    )
    persist_quality_log(
        spark,
        catalog_name,
        metadata_schema,
        "FactStream",
        "referential_integrity_track_id",
        "PASS" if orphan_track_count == 0 else "FAIL",
        f"orphan_track_ids={orphan_track_count}",
    )

    if orphan_user_count > 0 or orphan_track_count > 0:
        orphan_users.show(20, truncate=False)
        orphan_tracks.show(20, truncate=False)
        raise ValueError(
            f"Referential integrity failed. orphan_user_ids={orphan_user_count}, orphan_track_ids={orphan_track_count}"
        )

# COMMAND ----------
def run_row_count_delta_check(table_name: str) -> None:
    current_count = spark.table(f"{catalog_name}.{silver_schema}.{table_name}").count()
    watermark_df = spark.table(f"{catalog_name}.{metadata_schema}.pipeline_watermarks").filter(
        F.col("table_name") == table_name
    )
    previous = watermark_df.select("last_row_count").collect()
    previous_count = previous[0]["last_row_count"] if previous and previous[0]["last_row_count"] is not None else None

    if previous_count is None or previous_count == 0:
        status = "PASS"
        details = f"current_count={current_count}, previous_count={previous_count}"
    else:
        delta_pct = abs(current_count - previous_count) / previous_count * 100
        status = "WARNING" if delta_pct > 50 else "PASS"
        details = (
            f"current_count={current_count}, previous_count={previous_count}, "
            f"delta_pct={round(delta_pct, 2)}"
        )

    persist_quality_log(
        spark,
        catalog_name,
        metadata_schema,
        table_name,
        "row_count_delta_check",
        status,
        details,
    )

    if status == "WARNING":
        logger.info(
            f"Row count delta warning for {table_name}",
            table_name=table_name,
            rows_read=current_count,
            rows_written=current_count,
            duration_seconds=0.0,
            status=status,
            batch_id="N/A",
            run_id="interactive",
        )

# COMMAND ----------
for cfg in table_configs:
    run_pk_null_check(cfg["table_name"], cfg["primary_keys"])
    run_row_count_delta_check(cfg["table_name"])

run_referential_integrity_checks()

# COMMAND ----------
display(spark.table(f"{catalog_name}.{metadata_schema}.data_quality_log").orderBy(F.col("check_timestamp").desc()))



