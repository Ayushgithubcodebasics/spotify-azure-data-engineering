# Databricks notebook source
# COMMAND ----------
import os
import sys
from datetime import datetime

from delta.tables import DeltaTable
from pyspark.sql import DataFrame
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
    persist_pipeline_log,
)

repo_root = ensure_repo_root_on_path()
project_config = load_json_file(os.path.join(repo_root, "conf", "project_config.json"))

catalog_name = project_config["catalog_name"]
silver_schema = project_config["silver_schema"]
gold_schema = project_config["gold_schema"]
metadata_schema = project_config["metadata_schema"]

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{gold_schema}")
ensure_metadata_tables(spark, catalog_name, metadata_schema)
logger = NotebookLogger("gold_scd2_merge")

# COMMAND ----------
def ensure_dim_table_exists(create_sql: str) -> None:
    spark.sql(create_sql)


ensure_dim_table_exists(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{gold_schema}.dim_user_scd2 (
        surrogate_user_key STRING,
        user_id INT,
        user_name STRING,
        country STRING,
        subscription_type STRING,
        source_start_date DATE,
        source_end_date DATE,
        source_updated_at TIMESTAMP,
        start_date DATE,
        end_date DATE,
        is_current BOOLEAN,
        attribute_hash STRING,
        record_start_ts TIMESTAMP,
        record_end_ts TIMESTAMP
    )
    USING DELTA
    """
)

ensure_dim_table_exists(
    f"""
    CREATE TABLE IF NOT EXISTS {catalog_name}.{gold_schema}.dim_artist_scd2 (
        surrogate_artist_key STRING,
        artist_id INT,
        artist_name STRING,
        genre STRING,
        country STRING,
        source_updated_at TIMESTAMP,
        start_date DATE,
        end_date DATE,
        is_current BOOLEAN,
        attribute_hash STRING,
        record_start_ts TIMESTAMP,
        record_end_ts TIMESTAMP
    )
    USING DELTA
    """
)

# COMMAND ----------
def build_user_source() -> DataFrame:
    return (
        spark.table(f"{catalog_name}.{silver_schema}.DimUser")
        .withColumn("attribute_hash", F.sha2(F.concat_ws("||", "user_name", "country", "subscription_type"), 256))
    )


def build_artist_source() -> DataFrame:
    return (
        spark.table(f"{catalog_name}.{silver_schema}.DimArtist")
        .withColumn("attribute_hash", F.sha2(F.concat_ws("||", "artist_name", "genre", "country"), 256))
    )


def get_run_temporal_markers() -> tuple:
    markers = spark.sql("SELECT current_timestamp() AS run_ts, current_date() AS run_date").first()
    return markers["run_ts"], markers["run_date"]


def build_user_changes(run_ts, run_date) -> DataFrame:
    current_target = spark.table(f"{catalog_name}.{gold_schema}.dim_user_scd2").filter(F.col("is_current") == True)
    changed_or_new = (
        build_user_source().alias("s")
        .join(current_target.alias("t"), F.col("s.user_id") == F.col("t.user_id"), "left")
        .where(F.col("t.user_id").isNull() | (F.col("s.attribute_hash") != F.col("t.attribute_hash")))
        .select(
            F.col("s.user_id"),
            F.col("s.user_name"),
            F.col("s.country"),
            F.col("s.subscription_type"),
            F.col("s.start_date").alias("source_start_date"),
            F.col("s.end_date").alias("source_end_date"),
            F.col("s.updated_at").alias("source_updated_at"),
            F.col("s.attribute_hash"),
        )
        .withColumn("start_date", F.lit(run_date).cast("date"))
        .withColumn("end_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn("record_start_ts", F.lit(run_ts).cast("timestamp"))
        .withColumn("record_end_ts", F.lit(None).cast("timestamp"))
        .withColumn(
            "surrogate_user_key",
            F.sha2(F.concat_ws("||", F.col("user_id").cast("string"), F.col("record_start_ts").cast("string")), 256),
        )
        .cache()
    )
    changed_or_new.count()
    return changed_or_new


def build_artist_changes(run_ts, run_date) -> DataFrame:
    current_target = spark.table(f"{catalog_name}.{gold_schema}.dim_artist_scd2").filter(F.col("is_current") == True)
    changed_or_new = (
        build_artist_source().alias("s")
        .join(current_target.alias("t"), F.col("s.artist_id") == F.col("t.artist_id"), "left")
        .where(F.col("t.artist_id").isNull() | (F.col("s.attribute_hash") != F.col("t.attribute_hash")))
        .select(
            F.col("s.artist_id"),
            F.col("s.artist_name"),
            F.col("s.genre"),
            F.col("s.country"),
            F.col("s.updated_at").alias("source_updated_at"),
            F.col("s.attribute_hash"),
        )
        .withColumn("start_date", F.lit(run_date).cast("date"))
        .withColumn("end_date", F.lit(None).cast("date"))
        .withColumn("is_current", F.lit(True))
        .withColumn("record_start_ts", F.lit(run_ts).cast("timestamp"))
        .withColumn("record_end_ts", F.lit(None).cast("timestamp"))
        .withColumn(
            "surrogate_artist_key",
            F.sha2(F.concat_ws("||", F.col("artist_id").cast("string"), F.col("record_start_ts").cast("string")), 256),
        )
        .cache()
    )
    changed_or_new.count()
    return changed_or_new


# COMMAND ----------
def apply_scd2_user_merge() -> int:
    target_table = f"{catalog_name}.{gold_schema}.dim_user_scd2"
    run_ts, run_date = get_run_temporal_markers()
    changed_or_new = build_user_changes(run_ts, run_date)
    change_count = changed_or_new.count()

    if change_count == 0:
        changed_or_new.unpersist()
        return 0

    delta_target = DeltaTable.forName(spark, target_table)
    (
        delta_target.alias("t")
        .merge(
            changed_or_new.select("user_id").distinct().alias("s"),
            "t.user_id = s.user_id AND t.is_current = true",
        )
        .whenMatchedUpdate(
            set={
                "end_date": F.lit(run_date).cast("date"),
                "is_current": F.lit(False),
                "record_end_ts": F.lit(run_ts).cast("timestamp"),
            }
        )
        .execute()
    )

    changed_or_new.select(
        "surrogate_user_key",
        "user_id",
        "user_name",
        "country",
        "subscription_type",
        "source_start_date",
        "source_end_date",
        "source_updated_at",
        "start_date",
        "end_date",
        "is_current",
        "attribute_hash",
        "record_start_ts",
        "record_end_ts",
    ).write.format("delta").mode("append").saveAsTable(target_table)

    changed_or_new.unpersist()
    return change_count


def apply_scd2_artist_merge() -> int:
    target_table = f"{catalog_name}.{gold_schema}.dim_artist_scd2"
    run_ts, run_date = get_run_temporal_markers()
    changed_or_new = build_artist_changes(run_ts, run_date)
    change_count = changed_or_new.count()

    if change_count == 0:
        changed_or_new.unpersist()
        return 0

    delta_target = DeltaTable.forName(spark, target_table)
    (
        delta_target.alias("t")
        .merge(
            changed_or_new.select("artist_id").distinct().alias("s"),
            "t.artist_id = s.artist_id AND t.is_current = true",
        )
        .whenMatchedUpdate(
            set={
                "end_date": F.lit(run_date).cast("date"),
                "is_current": F.lit(False),
                "record_end_ts": F.lit(run_ts).cast("timestamp"),
            }
        )
        .execute()
    )

    changed_or_new.select(
        "surrogate_artist_key",
        "artist_id",
        "artist_name",
        "genre",
        "country",
        "source_updated_at",
        "start_date",
        "end_date",
        "is_current",
        "attribute_hash",
        "record_start_ts",
        "record_end_ts",
    ).write.format("delta").mode("append").saveAsTable(target_table)

    changed_or_new.unpersist()
    return change_count

# COMMAND ----------
start_ts = datetime.utcnow()
try:
    user_rows_written = apply_scd2_user_merge()
    artist_rows_written = apply_scd2_artist_merge()
    total_rows_written = user_rows_written + artist_rows_written
    persist_pipeline_log(
        spark=spark,
        catalog_name=catalog_name,
        metadata_schema=metadata_schema,
        notebook_name="gold_scd2_merge",
        table_name="dim_user_scd2,dim_artist_scd2",
        rows_read=total_rows_written,
        rows_written=total_rows_written,
        duration_seconds=(datetime.utcnow() - start_ts).total_seconds(),
        status="SUCCESS",
        error_message=None,
        batch_id="N/A",
        run_id="interactive",
    )
    logger.info(
        "Gold SCD2 merge completed",
        table_name="dim_user_scd2,dim_artist_scd2",
        rows_read=total_rows_written,
        rows_written=total_rows_written,
        duration_seconds=(datetime.utcnow() - start_ts).total_seconds(),
        status="SUCCESS",
        batch_id="N/A",
        run_id="interactive",
    )
except Exception as exc:
    persist_pipeline_log(
        spark=spark,
        catalog_name=catalog_name,
        metadata_schema=metadata_schema,
        notebook_name="gold_scd2_merge",
        table_name="dim_user_scd2,dim_artist_scd2",
        rows_read=0,
        rows_written=0,
        duration_seconds=(datetime.utcnow() - start_ts).total_seconds(),
        status="FAILED",
        error_message=str(exc),
        batch_id="N/A",
        run_id="interactive",
    )
    logger.error(
        "Gold SCD2 merge failed",
        table_name="dim_user_scd2,dim_artist_scd2",
        rows_read=0,
        rows_written=0,
        duration_seconds=(datetime.utcnow() - start_ts).total_seconds(),
        status="FAILED",
        error_message=str(exc),
        batch_id="N/A",
        run_id="interactive",
    )
    raise

# COMMAND ----------
display(spark.table(f"{catalog_name}.{gold_schema}.dim_user_scd2").orderBy(F.col("record_start_ts").desc()))



