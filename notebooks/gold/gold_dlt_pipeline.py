# Databricks notebook source
# COMMAND ----------
import os
import sys

import dlt
from pyspark.sql import functions as F

for candidate in [os.getcwd(), os.path.abspath(os.path.join(os.getcwd(), "..", "..")), "/Workspace/Repos"]:
    if candidate and os.path.isdir(candidate):
        if candidate not in sys.path:
            sys.path.insert(0, candidate)

from utils.transformations import ensure_repo_root_on_path, load_json_file

repo_root = ensure_repo_root_on_path()
project_config = load_json_file(os.path.join(repo_root, "conf", "project_config.json"))

CATALOG = spark.conf.get("catalog_name", project_config["catalog_name"])
SILVER_SCHEMA = spark.conf.get("silver_schema", project_config["silver_schema"])
GOLD_SCHEMA = spark.conf.get("gold_schema", project_config["gold_schema"])

silver_prefix = f"{CATALOG}.{SILVER_SCHEMA}"
gold_prefix = f"{CATALOG}.{GOLD_SCHEMA}"

# COMMAND ----------
@dlt.table(name="gold_dim_user")
def gold_dim_user():
    return spark.read.table(f"{gold_prefix}.dim_user_scd2")


@dlt.table(name="gold_dim_artist")
def gold_dim_artist():
    return spark.read.table(f"{gold_prefix}.dim_artist_scd2")


@dlt.table(name="gold_dim_track")
def gold_dim_track():
    return (
        spark.read.table(f"{silver_prefix}.DimTrack")
        .withColumn(
            "durationFlag",
            F.when(F.col("duration_sec") < 150, F.lit("low"))
             .when(F.col("duration_sec") < 300, F.lit("medium"))
             .otherwise(F.lit("high"))
        )
    )


@dlt.table(name="gold_dim_date")
def gold_dim_date():
    date_df = spark.read.table(f"{silver_prefix}.DimDate")
    return (
        date_df.withColumn("quarter", F.quarter(F.col("date")))
               .withColumn("fiscal_year", F.year(F.col("date")))
               .withColumn("week_number", F.weekofyear(F.col("date")))
               .withColumn("is_weekend", F.dayofweek(F.col("date")).isin(1, 7))
               .withColumn("day_of_year", F.dayofyear(F.col("date")))
    )

# COMMAND ----------
@dlt.table(name="gold_fact_stream")
@dlt.expect_or_fail("user_id_not_null", "user_id IS NOT NULL")
@dlt.expect_or_fail("listen_duration_positive", "listen_duration > 0")
def gold_fact_stream():
    fact = (
        spark.read.table(f"{silver_prefix}.FactStream")
        .alias("f")
        .join(
            spark.read.table(f"{silver_prefix}.DimTrack").select("track_id", "artist_id").alias("t"),
            on="track_id",
            how="left",
        )
    )

    dim_user = (
        spark.read.table(f"{gold_prefix}.dim_user_scd2")
        .filter(F.col("is_current") == True)
        .select("user_id", F.col("surrogate_user_key").alias("user_key"))
    )

    dim_artist = (
        spark.read.table(f"{gold_prefix}.dim_artist_scd2")
        .filter(F.col("is_current") == True)
        .select("artist_id", F.col("surrogate_artist_key").alias("artist_key"))
    )

    dim_track = (
        spark.read.table(f"{silver_prefix}.DimTrack")
        .select(
            "track_id",
            F.sha2(F.concat_ws("||", F.col("track_id").cast("string"), F.col("updated_at").cast("string")), 256).alias("track_key"),
        )
    )

    dim_date = (
        spark.read.table(f"{silver_prefix}.DimDate")
        .select(
            "date_key",
            F.sha2(F.concat_ws("||", F.col("date_key").cast("string")), 256).alias("date_key_hash"),
        )
    )

    return (
        fact.join(dim_user, on="user_id", how="left")
            .join(dim_artist, on="artist_id", how="left")
            .join(dim_track, on="track_id", how="left")
            .join(dim_date, on="date_key", how="left")
            .select(
                "stream_id",
                "user_id",
                "track_id",
                "artist_id",
                "date_key",
                "listen_duration",
                "device_type",
                "stream_timestamp",
                "event_date",
                "user_key",
                "artist_key",
                "track_key",
                F.col("date_key_hash").alias("date_dim_key"),
            )
    )



