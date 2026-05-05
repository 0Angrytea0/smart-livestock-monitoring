from pathlib import Path
from datetime import datetime, timedelta, date
import os
import math
import uuid

import pandas as pd
import clickhouse_connect
from pyspark.sql.functions import date_format

from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import (
    col,
    lit,
    avg,
    max as spark_max,
    sum as spark_sum,
    when,
    lag,
    sqrt,
    pow,
    row_number,
    greatest,
    to_date,
    current_timestamp,
)

CHECKPOINT_BASE_PATH = "/opt/spark/checkpoints/gold"
Path(CHECKPOINT_BASE_PATH).mkdir(parents=True, exist_ok=True)
_GOLD_READY_MARKED = False

GOLD_TRIGGER_INTERVAL = os.getenv("GOLD_TRIGGER_INTERVAL", "30 seconds")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "clickhouse")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))
CLICKHOUSE_USERNAME = os.getenv("CLICKHOUSE_USERNAME", "default")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD", "")
CLICKHOUSE_DATABASE = os.getenv("CLICKHOUSE_DATABASE", "livestock")

RESET_GOLD_TABLES_ON_START = os.getenv("RESET_GOLD_TABLES_ON_START", "false").lower() == "true"
_GOLD_TABLES_RESET_DONE = False

TEMP_HIGH = float(os.getenv("TEMP_HIGH", "39.5"))
TEMP_CRITICAL = float(os.getenv("TEMP_CRITICAL", "40.5"))

LONG_LYING_MINUTES_3H = float(os.getenv("LONG_LYING_MINUTES_3H", "150"))
LONG_LYING_CRITICAL_MINUTES_3H = float(os.getenv("LONG_LYING_CRITICAL_MINUTES_3H", "180"))

LOW_ACTIVITY_DISTANCE_M_6H = float(os.getenv("LOW_ACTIVITY_DISTANCE_M_6H", "50.0"))
LOW_ACTIVITY_CRITICAL_DISTANCE_M_6H = float(os.getenv("LOW_ACTIVITY_CRITICAL_DISTANCE_M_6H", "20.0"))

THI_WARNING = float(os.getenv("THI_WARNING", "68.0"))
THI_DANGER = float(os.getenv("THI_DANGER", "72.0"))

USE_HEAD_DOWN_EXPERIMENTAL = os.getenv("USE_HEAD_DOWN_EXPERIMENTAL", "false").lower() == "true"
HEAD_DOWN_Z_RATIO_THRESHOLD = float(os.getenv("HEAD_DOWN_Z_RATIO_THRESHOLD", "-0.70"))
HEAD_DOWN_RATIO_ALERT = float(os.getenv("HEAD_DOWN_RATIO_ALERT", "0.70"))
HEAD_DOWN_RATIO_CRITICAL = float(os.getenv("HEAD_DOWN_RATIO_CRITICAL", "0.85"))

TABLE_COLUMNS = {
    "gold_cow_current": [
        "cow_id",
        "last_event_time",
        "last_temp_c",
        "max_temp_1h",
        "lying_now",
        "lying_minutes_1h",
        "lying_minutes_3h",
        "milk_kg_today",
        "open_alerts_count",
        "risk_score",
        "last_update_ts",
    ],
    "gold_cow_day": [
        "event_date",
        "cow_id",
        "avg_temp_c",
        "max_temp_c",
        "lying_minutes",
        "lying_ratio",
        "milk_kg",
        "last_update_ts",
    ],
    "gold_tag_current": [
        "tag_id",
        "last_event_time",
        "coord_x_cm",
        "coord_y_cm",
        "coord_z_cm",
        "distance_m_1h",
        "distance_m_6h",
        "pressure_pa",
        "elevation_m",
        "head_down_ratio_1h",
        "open_alerts_count",
        "risk_score",
        "last_update_ts",
    ],
    "gold_tag_day": [
        "event_date",
        "tag_id",
        "distance_m",
        "avg_pressure_pa",
        "max_pressure_pa",
        "avg_elevation_m",
        "head_down_ratio_day",
        "last_update_ts",
    ],
    "gold_environment_current": [
        "sensor_id",
        "event_time",
        "temperature_c",
        "humidity_per",
        "thi",
        "thi_risk",
        "last_update_ts",
    ],
    "gold_environment_global_current": [
        "source_sensor_id",
        "source_mode",
        "event_time",
        "temperature_c",
        "humidity_per",
        "thi",
        "thi_risk",
        "last_update_ts",
    ],
    "gold_alerts_open": [
        "entity_type",
        "entity_id",
        "alert_code",
        "alert_name",
        "severity",
        "metric_value",
        "threshold_value",
        "description",
        "first_seen",
        "last_seen",
        "is_open",
        "last_update_ts",
    ],
    "gold_alerts_history": [
        "event_id",
        "alert_event_time",
        "entity_type",
        "entity_id",
        "alert_code",
        "alert_name",
        "severity",
        "metric_value",
        "threshold_value",
        "description",
        "event_status",
        "first_seen",
        "last_seen",
    ],
}

spark = (
    SparkSession.builder
        .appName("GoldIngestion")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.files.maxPartitionBytes", "33554432")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def read_iceberg_table(table_name: str):
    return spark.read.format("iceberg").load(table_name)


def latest_by(df, key_columns):
    order_columns = [col("event_time").desc_nulls_last()]

    if "bronze_ingest_ts" in df.columns:
        order_columns.append(col("bronze_ingest_ts").desc_nulls_last())
    if "silver_ingest_ts" in df.columns:
        order_columns.append(col("silver_ingest_ts").desc_nulls_last())

    w = Window.partitionBy(*key_columns).orderBy(*order_columns)

    return (
        df.withColumn("rn", row_number().over(w))
          .filter(col("rn") == 1)
          .drop("rn")
    )


def classify_thi_value(value):
    if value is None or pd.isna(value):
        return "unknown"
    if float(value) >= THI_DANGER:
        return "danger"
    if float(value) >= THI_WARNING:
        return "warning"
    return "normal"


def get_table_max_event_time(table_name: str):
    row = read_iceberg_table(table_name).agg(spark_max("event_time").alias("max_event_time")).collect()[0]
    return row["max_event_time"]


def get_reference_ts():
    timestamps = [
        get_table_max_event_time("iceberg.silver_cbt"),
        get_table_max_event_time("iceberg.silver_ankle"),
        get_table_max_event_time("iceberg.silver_milk"),
        get_table_max_event_time("iceberg.silver_uwb"),
        get_table_max_event_time("iceberg.silver_pressure"),
        get_table_max_event_time("iceberg.silver_immu"),
        get_table_max_event_time("iceberg.silver_thi"),
    ]
    timestamps = [ts for ts in timestamps if ts is not None]
    if not timestamps:
        return datetime.utcnow().replace(microsecond=0)
    return max(timestamps)


def compute_distance_over_window(uwb_df, cutoff_ts, alias_name: str):
    recent_df = uwb_df.filter(col("event_time") >= lit(cutoff_ts))
    w = Window.partitionBy("tag_id").orderBy("event_time")

    return (
        recent_df
        .withColumn("prev_x", lag("coord_x_cm").over(w))
        .withColumn("prev_y", lag("coord_y_cm").over(w))
        .withColumn(
            "distance_m",
            when(
                col("prev_x").isNull() | col("prev_y").isNull(),
                lit(0.0)
            ).otherwise(
                sqrt(
                    pow((col("coord_x_cm") - col("prev_x")) / lit(100.0), 2) +
                    pow((col("coord_y_cm") - col("prev_y")) / lit(100.0), 2)
                )
            )
        )
        .groupBy("tag_id")
        .agg(spark_sum("distance_m").alias(alias_name))
    )


def compute_daily_distance(uwb_df):
    base_df = uwb_df.withColumn("event_date", to_date(col("event_time")))
    w = Window.partitionBy("tag_id", "event_date").orderBy("event_time")

    return (
        base_df
        .withColumn("prev_x", lag("coord_x_cm").over(w))
        .withColumn("prev_y", lag("coord_y_cm").over(w))
        .withColumn(
            "distance_m",
            when(
                col("prev_x").isNull() | col("prev_y").isNull(),
                lit(0.0)
            ).otherwise(
                sqrt(
                    pow((col("coord_x_cm") - col("prev_x")) / lit(100.0), 2) +
                    pow((col("coord_y_cm") - col("prev_y")) / lit(100.0), 2)
                )
            )
        )
        .groupBy("tag_id", "event_date")
        .agg(spark_sum("distance_m").alias("distance_m"))
    )


def compute_cow_current(reference_ts: datetime):
    cbt_df = read_iceberg_table("iceberg.silver_cbt")
    ankle_df = read_iceberg_table("iceberg.silver_ankle")
    milk_df = read_iceberg_table("iceberg.silver_milk")

    cutoff_1h = reference_ts - timedelta(hours=1)
    cutoff_3h = reference_ts - timedelta(hours=3)
    ref_date = reference_ts.date()

    latest_temp = latest_by(
        cbt_df.select("cow_id", "event_time", "temperature_c", "bronze_ingest_ts", "silver_ingest_ts"),
        ["cow_id"]
    ).select(
        "cow_id",
        col("event_time").alias("temp_event_time"),
        col("temperature_c").alias("last_temp_c")
    )

    max_temp_1h = (
        cbt_df
        .filter(col("event_time") >= lit(cutoff_1h))
        .groupBy("cow_id")
        .agg(spark_max("temperature_c").alias("max_temp_1h"))
    )

    latest_ankle = latest_by(
        ankle_df.select("cow_id", "event_time", "lying", "bronze_ingest_ts", "silver_ingest_ts"),
        ["cow_id"]
    ).select(
        "cow_id",
        col("event_time").alias("ankle_event_time"),
        col("lying").cast("int").alias("lying_now")
    )

    lying_1h = (
        ankle_df
        .filter(col("event_time") >= lit(cutoff_1h))
        .groupBy("cow_id")
        .agg(spark_sum(col("lying").cast("double")).alias("lying_minutes_1h"))
    )

    lying_3h = (
        ankle_df
        .filter(col("event_time") >= lit(cutoff_3h))
        .groupBy("cow_id")
        .agg(spark_sum(col("lying").cast("double")).alias("lying_minutes_3h"))
    )

    milk_today = (
        milk_df
        .filter(to_date(col("event_time")) == lit(ref_date))
        .groupBy("cow_id")
        .agg(
            spark_sum("milk_weight_kg").alias("milk_kg_today"),
            spark_max("event_time").alias("milk_event_time")
        )
    )

    cow_keys = (
        cbt_df.select("cow_id")
        .unionByName(ankle_df.select("cow_id"))
        .unionByName(milk_df.select("cow_id"))
        .dropDuplicates(["cow_id"])
    )

    return (
        cow_keys
        .join(latest_temp, on="cow_id", how="left")
        .join(max_temp_1h, on="cow_id", how="left")
        .join(latest_ankle, on="cow_id", how="left")
        .join(lying_1h, on="cow_id", how="left")
        .join(lying_3h, on="cow_id", how="left")
        .join(milk_today, on="cow_id", how="left")
        .withColumn(
            "last_event_time",
            greatest("temp_event_time", "ankle_event_time", "milk_event_time")
        )
        .withColumn("last_update_ts", current_timestamp())
        .select(
            "cow_id",
            "last_event_time",
            "last_temp_c",
            "max_temp_1h",
            "lying_now",
            "lying_minutes_1h",
            "lying_minutes_3h",
            "milk_kg_today",
            "last_update_ts"
        )
    )


def compute_cow_day():
    cbt_df = read_iceberg_table("iceberg.silver_cbt")
    ankle_df = read_iceberg_table("iceberg.silver_ankle")
    milk_df = read_iceberg_table("iceberg.silver_milk")

    cbt_day = (
        cbt_df
        .withColumn("event_date", to_date(col("event_time")))
        .groupBy("event_date", "cow_id")
        .agg(
            avg("temperature_c").alias("avg_temp_c"),
            spark_max("temperature_c").alias("max_temp_c")
        )
    )

    ankle_day = (
        ankle_df
        .withColumn("event_date", to_date(col("event_time")))
        .groupBy("event_date", "cow_id")
        .agg(
            spark_sum(col("lying").cast("double")).alias("lying_minutes"),
            avg(col("lying").cast("double")).alias("lying_ratio")
        )
    )

    milk_day = (
        milk_df
        .withColumn("event_date", to_date(col("event_time")))
        .groupBy("event_date", "cow_id")
        .agg(spark_sum("milk_weight_kg").alias("milk_kg"))
    )

    day_keys = (
        cbt_day.select("event_date", "cow_id")
        .unionByName(ankle_day.select("event_date", "cow_id"))
        .unionByName(milk_day.select("event_date", "cow_id"))
        .dropDuplicates(["event_date", "cow_id"])
    )

    return (
        day_keys
        .join(cbt_day, on=["event_date", "cow_id"], how="left")
        .join(ankle_day, on=["event_date", "cow_id"], how="left")
        .join(milk_day, on=["event_date", "cow_id"], how="left")
        .withColumn("lying_minutes", when(col("lying_minutes").isNull(), lit(0.0)).otherwise(col("lying_minutes")))
        .withColumn("lying_ratio", when(col("lying_ratio").isNull(), lit(0.0)).otherwise(col("lying_ratio")))
        .withColumn("milk_kg", when(col("milk_kg").isNull(), lit(0.0)).otherwise(col("milk_kg")))
        .withColumn("last_update_ts", current_timestamp())
        .select(*TABLE_COLUMNS["gold_cow_day"])
    )


def prepare_for_pandas(df):
    result = df
    for field in df.schema.fields:
        field_name = field.name
        field_type = field.dataType.simpleString()

        if field_type == "timestamp":
            result = result.withColumn(
                field_name,
                date_format(col(field_name), "yyyy-MM-dd HH:mm:ss")
            )
        elif field_type == "date":
            result = result.withColumn(
                field_name,
                date_format(col(field_name), "yyyy-MM-dd")
            )

    return result


def compute_tag_current(reference_ts: datetime):
    uwb_df = read_iceberg_table("iceberg.silver_uwb")
    pressure_df = read_iceberg_table("iceberg.silver_pressure")
    immu_df = read_iceberg_table("iceberg.silver_immu")

    cutoff_1h = reference_ts - timedelta(hours=1)
    cutoff_6h = reference_ts - timedelta(hours=6)

    latest_uwb = latest_by(
        uwb_df.select(
            "tag_id",
            "event_time",
            "coord_x_cm",
            "coord_y_cm",
            "coord_z_cm",
            "bronze_ingest_ts",
            "silver_ingest_ts"
        ),
        ["tag_id"]
    ).select(
        "tag_id",
        col("event_time").alias("uwb_event_time"),
        "coord_x_cm",
        "coord_y_cm",
        "coord_z_cm"
    )

    distance_1h = compute_distance_over_window(uwb_df, cutoff_1h, "distance_m_1h")
    distance_6h = compute_distance_over_window(uwb_df, cutoff_6h, "distance_m_6h")

    latest_pressure = latest_by(
        pressure_df.select(
            "tag_id",
            "event_time",
            "pressure_pa",
            "elevation_m",
            "bronze_ingest_ts",
            "silver_ingest_ts"
        ),
        ["tag_id"]
    ).select(
        "tag_id",
        col("event_time").alias("pressure_event_time"),
        "pressure_pa",
        "elevation_m"
    )

    latest_immu_time = latest_by(
        immu_df.select("tag_id", "event_time", "bronze_ingest_ts", "silver_ingest_ts"),
        ["tag_id"]
    ).select(
        "tag_id",
        col("event_time").alias("immu_event_time")
    )

    if USE_HEAD_DOWN_EXPERIMENTAL:
        immu_head_down_1h = (
            immu_df
            .filter(col("event_time") >= lit(cutoff_1h))
            .withColumn(
                "accel_norm",
                sqrt(
                    pow(col("accel_x_mps2"), 2) +
                    pow(col("accel_y_mps2"), 2) +
                    pow(col("accel_z_mps2"), 2)
                )
            )
            .withColumn(
                "head_down_flag",
                when(
                    col("accel_norm").isNull() | (col("accel_norm") == 0),
                    lit(None).cast("double")
                ).when(
                    (col("accel_z_mps2") / col("accel_norm")) <= lit(HEAD_DOWN_Z_RATIO_THRESHOLD),
                    lit(1.0)
                ).otherwise(lit(0.0))
            )
            .groupBy("tag_id")
            .agg(avg("head_down_flag").alias("head_down_ratio_1h"))
        )
    else:
        immu_head_down_1h = (
            immu_df
            .select("tag_id")
            .dropDuplicates(["tag_id"])
            .withColumn("head_down_ratio_1h", lit(None).cast("double"))
        )

    tag_keys = (
        uwb_df.select("tag_id")
        .unionByName(pressure_df.select("tag_id"))
        .unionByName(immu_df.select("tag_id"))
        .dropDuplicates(["tag_id"])
    )

    return (
        tag_keys
        .join(latest_uwb, on="tag_id", how="left")
        .join(distance_1h, on="tag_id", how="left")
        .join(distance_6h, on="tag_id", how="left")
        .join(latest_pressure, on="tag_id", how="left")
        .join(latest_immu_time, on="tag_id", how="left")
        .join(immu_head_down_1h, on="tag_id", how="left")
        .withColumn(
            "last_event_time",
            greatest("uwb_event_time", "pressure_event_time", "immu_event_time")
        )
        .withColumn("last_update_ts", current_timestamp())
        .select(
            "tag_id",
            "last_event_time",
            "coord_x_cm",
            "coord_y_cm",
            "coord_z_cm",
            "distance_m_1h",
            "distance_m_6h",
            "pressure_pa",
            "elevation_m",
            "head_down_ratio_1h",
            "last_update_ts"
        )
    )


def compute_tag_day():
    uwb_df = read_iceberg_table("iceberg.silver_uwb")
    pressure_df = read_iceberg_table("iceberg.silver_pressure")
    immu_df = read_iceberg_table("iceberg.silver_immu")

    uwb_day = compute_daily_distance(uwb_df)

    pressure_day = (
        pressure_df
        .withColumn("event_date", to_date(col("event_time")))
        .groupBy("event_date", "tag_id")
        .agg(
            avg("pressure_pa").alias("avg_pressure_pa"),
            spark_max("pressure_pa").alias("max_pressure_pa"),
            avg("elevation_m").alias("avg_elevation_m")
        )
    )

    immu_day = (
        immu_df
        .withColumn("event_date", to_date(col("event_time")))
        .groupBy("event_date", "tag_id")
        .agg(spark_max("event_time").alias("last_immu_event_time"))
    )

    if USE_HEAD_DOWN_EXPERIMENTAL:
        immu_head_down_day = (
            immu_df
            .withColumn("event_date", to_date(col("event_time")))
            .withColumn(
                "accel_norm",
                sqrt(
                    pow(col("accel_x_mps2"), 2) +
                    pow(col("accel_y_mps2"), 2) +
                    pow(col("accel_z_mps2"), 2)
                )
            )
            .withColumn(
                "head_down_flag",
                when(
                    col("accel_norm").isNull() | (col("accel_norm") == 0),
                    lit(None).cast("double")
                ).when(
                    (col("accel_z_mps2") / col("accel_norm")) <= lit(HEAD_DOWN_Z_RATIO_THRESHOLD),
                    lit(1.0)
                ).otherwise(lit(0.0))
            )
            .groupBy("event_date", "tag_id")
            .agg(avg("head_down_flag").alias("head_down_ratio_day"))
        )
    else:
        immu_head_down_day = (
            immu_df
            .withColumn("event_date", to_date(col("event_time")))
            .select("event_date", "tag_id")
            .dropDuplicates(["event_date", "tag_id"])
            .withColumn("head_down_ratio_day", lit(None).cast("double"))
        )

    day_keys = (
        uwb_day.select("event_date", "tag_id")
        .unionByName(pressure_day.select("event_date", "tag_id"))
        .unionByName(immu_day.select("event_date", "tag_id"))
        .dropDuplicates(["event_date", "tag_id"])
    )

    return (
        day_keys
        .join(uwb_day, on=["event_date", "tag_id"], how="left")
        .join(pressure_day, on=["event_date", "tag_id"], how="left")
        .join(immu_day, on=["event_date", "tag_id"], how="left")
        .join(immu_head_down_day, on=["event_date", "tag_id"], how="left")
        .withColumn("distance_m", when(col("distance_m").isNull(), lit(0.0)).otherwise(col("distance_m")))
        .withColumn("last_update_ts", current_timestamp())
        .select(*TABLE_COLUMNS["gold_tag_day"])
    )


def compute_environment_current():
    thi_df = read_iceberg_table("iceberg.silver_thi")

    latest_thi = latest_by(
        thi_df.select(
            "sensor_id",
            "event_time",
            "temperature_c",
            "humidity_per",
            "thi",
            "bronze_ingest_ts",
            "silver_ingest_ts"
        ),
        ["sensor_id"]
    )

    return (
        latest_thi
        .withColumn("last_update_ts", current_timestamp())
        .select(
            "sensor_id",
            "event_time",
            "temperature_c",
            "humidity_per",
            "thi",
            "last_update_ts"
        )
    )


def get_clickhouse_client():
    return clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USERNAME,
        password=CLICKHOUSE_PASSWORD,
        database=CLICKHOUSE_DATABASE
    )


def ensure_clickhouse_tables(client):
    global _GOLD_TABLES_RESET_DONE

    client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")

    if RESET_GOLD_TABLES_ON_START and not _GOLD_TABLES_RESET_DONE:
        tables_to_reset = [
            "gold_cow_current",
            "gold_cow_day",
            "gold_tag_current",
            "gold_tag_day",
            "gold_environment_current",
            "gold_environment_global_current",
            "gold_alerts_open",
            "gold_alerts_history",
        ]
        for table_name in tables_to_reset:
            client.command(f"DROP TABLE IF EXISTS {CLICKHOUSE_DATABASE}.{table_name}")
        _GOLD_TABLES_RESET_DONE = True

    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.gold_cow_current
        (
            cow_id String,
            last_event_time Nullable(DateTime),
            last_temp_c Nullable(Float64),
            max_temp_1h Nullable(Float64),
            lying_now Nullable(UInt8),
            lying_minutes_1h Nullable(Float64),
            lying_minutes_3h Nullable(Float64),
            milk_kg_today Nullable(Float64),
            open_alerts_count UInt32,
            risk_score Float64,
            last_update_ts DateTime
        )
        ENGINE = MergeTree
        ORDER BY (cow_id)
        """
    )

    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.gold_cow_day
        (
            event_date Date,
            cow_id String,
            avg_temp_c Nullable(Float64),
            max_temp_c Nullable(Float64),
            lying_minutes Float64,
            lying_ratio Float64,
            milk_kg Float64,
            last_update_ts DateTime
        )
        ENGINE = MergeTree
        ORDER BY (event_date, cow_id)
        """
    )

    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.gold_tag_current
        (
            tag_id String,
            last_event_time Nullable(DateTime),
            coord_x_cm Nullable(Float64),
            coord_y_cm Nullable(Float64),
            coord_z_cm Nullable(Float64),
            distance_m_1h Nullable(Float64),
            distance_m_6h Nullable(Float64),
            pressure_pa Nullable(Float64),
            elevation_m Nullable(Float64),
            head_down_ratio_1h Nullable(Float64),
            open_alerts_count UInt32,
            risk_score Float64,
            last_update_ts DateTime
        )
        ENGINE = MergeTree
        ORDER BY (tag_id)
        """
    )

    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.gold_tag_day
        (
            event_date Date,
            tag_id String,
            distance_m Float64,
            avg_pressure_pa Nullable(Float64),
            max_pressure_pa Nullable(Float64),
            avg_elevation_m Nullable(Float64),
            head_down_ratio_day Nullable(Float64),
            last_update_ts DateTime
        )
        ENGINE = MergeTree
        ORDER BY (event_date, tag_id)
        """
    )

    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.gold_environment_current
        (
            sensor_id String,
            event_time Nullable(DateTime),
            temperature_c Nullable(Float64),
            humidity_per Nullable(Float64),
            thi Nullable(Float64),
            thi_risk String,
            last_update_ts DateTime
        )
        ENGINE = MergeTree
        ORDER BY (sensor_id)
        """
    )

    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.gold_environment_global_current
        (
            source_sensor_id String,
            source_mode String,
            event_time Nullable(DateTime),
            temperature_c Nullable(Float64),
            humidity_per Nullable(Float64),
            thi Nullable(Float64),
            thi_risk String,
            last_update_ts DateTime
        )
        ENGINE = MergeTree
        ORDER BY (source_sensor_id)
        """
    )

    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.gold_alerts_open
        (
            entity_type String,
            entity_id String,
            alert_code String,
            alert_name String,
            severity String,
            metric_value Nullable(Float64),
            threshold_value Nullable(Float64),
            description String,
            first_seen DateTime,
            last_seen DateTime,
            is_open UInt8,
            last_update_ts DateTime
        )
        ENGINE = MergeTree
        ORDER BY (entity_type, entity_id, alert_code)
        """
    )

    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.gold_alerts_history
        (
            event_id String,
            alert_event_time DateTime,
            entity_type String,
            entity_id String,
            alert_code String,
            alert_name String,
            severity String,
            metric_value Nullable(Float64),
            threshold_value Nullable(Float64),
            description String,
            event_status String,
            first_seen DateTime,
            last_seen DateTime
        )
        ENGINE = MergeTree
        ORDER BY (alert_event_time, entity_type, entity_id, alert_code)
        """
    )


def align_pdf_to_table(table_name: str, pdf: pd.DataFrame) -> pd.DataFrame:
    expected_columns = TABLE_COLUMNS.get(table_name)
    if expected_columns is None:
        return pdf

    out = pdf.copy()

    for column_name in expected_columns:
        if column_name not in out.columns:
            out[column_name] = None

    return out[expected_columns]


def to_python_scalar(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if isinstance(value, pd.Timestamp):
        dt = value.to_pydatetime()
        if dt.tzinfo is not None:
            dt = dt.replace(tzinfo=None)
        return dt

    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.replace(tzinfo=None)
        return value

    if isinstance(value, date):
        return value

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass

    return value


def dataframe_to_rows(table_name: str, pdf: pd.DataFrame):
    pdf = align_pdf_to_table(table_name, pdf)

    if pdf is None or pdf.empty:
        return pdf, []

    out = pdf.copy()
    rows = []

    for row in out.itertuples(index=False, name=None):
        rows.append(tuple(to_python_scalar(v) for v in row))

    return out, rows


def clickhouse_literal(value):
    if value is None:
        return "NULL"

    if isinstance(value, pd.Timestamp):
        value = value.to_pydatetime()

    if isinstance(value, datetime):
        if value.tzinfo is not None:
            value = value.replace(tzinfo=None)
        value = value.replace(microsecond=0)
        return f"toDateTime('{value.strftime('%Y-%m-%d %H:%M:%S')}')"

    if isinstance(value, date) and not isinstance(value, datetime):
        return f"toDate('{value.isoformat()}')"

    if isinstance(value, bool):
        return "1" if value else "0"

    if isinstance(value, float):
        if math.isnan(value) or math.isinf(value):
            return "NULL"
        return repr(float(value))

    if isinstance(value, int):
        return str(int(value))

    if hasattr(value, "item"):
        try:
            return clickhouse_literal(value.item())
        except Exception:
            pass

    escaped = str(value).replace("\\", "\\\\").replace("'", "\\'")
    return f"'{escaped}'"


def insert_rows_sql(client, table_name: str, pdf: pd.DataFrame, rows, chunk_size: int = 1000):
    if not rows:
        return

    columns_sql = ", ".join(pdf.columns)

    for start_idx in range(0, len(rows), chunk_size):
        chunk = rows[start_idx:start_idx + chunk_size]
        values_sql = []

        for row in chunk:
            values_sql.append("(" + ", ".join(clickhouse_literal(v) for v in row) + ")")

        sql = f"INSERT INTO {CLICKHOUSE_DATABASE}.{table_name} ({columns_sql}) VALUES " + ", ".join(values_sql)
        client.command(sql)


def overwrite_table(client, table_name: str, pdf: pd.DataFrame):
    pdf, rows = dataframe_to_rows(table_name, pdf)
    client.command(f"TRUNCATE TABLE {CLICKHOUSE_DATABASE}.{table_name}")

    if not rows:
        return

    insert_rows_sql(client, table_name, pdf, rows)


def append_table(client, table_name: str, pdf: pd.DataFrame):
    pdf, rows = dataframe_to_rows(table_name, pdf)

    if not rows:
        return

    insert_rows_sql(client, table_name, pdf, rows)


def load_previous_open_alerts(client) -> pd.DataFrame:
    try:
        return client.query_df(
            f"""
            SELECT
                entity_type,
                entity_id,
                alert_code,
                alert_name,
                severity,
                metric_value,
                threshold_value,
                description,
                first_seen,
                last_seen,
                is_open,
                last_update_ts
            FROM {CLICKHOUSE_DATABASE}.gold_alerts_open
            """
        )
    except Exception:
        return pd.DataFrame(columns=TABLE_COLUMNS["gold_alerts_open"])


def build_environment_current_pdf(environment_current_pdf: pd.DataFrame):
    cols = TABLE_COLUMNS["gold_environment_current"]

    if environment_current_pdf is None or environment_current_pdf.empty:
        return pd.DataFrame(columns=cols)

    out = environment_current_pdf.copy()
    out["thi_risk"] = out["thi"].map(classify_thi_value)
    return out[cols]


def build_environment_global_pdf(environment_current_pdf: pd.DataFrame, batch_ts: datetime):
    cols = TABLE_COLUMNS["gold_environment_global_current"]

    if environment_current_pdf is None or environment_current_pdf.empty:
        return pd.DataFrame([{
            "source_sensor_id": "GLOBAL",
            "source_mode": "empty",
            "event_time": None,
            "temperature_c": None,
            "humidity_per": None,
            "thi": None,
            "thi_risk": "unknown",
            "last_update_ts": batch_ts,
        }], columns=cols)

    env = environment_current_pdf.copy()
    env["sensor_id_lower"] = env["sensor_id"].astype(str).str.lower()

    avg_rows = env[env["sensor_id_lower"] == "average"].copy()
    if not avg_rows.empty:
        row = avg_rows.iloc[0]
        return pd.DataFrame([{
            "source_sensor_id": row["sensor_id"],
            "source_mode": "average_sensor",
            "event_time": row.get("event_time"),
            "temperature_c": row.get("temperature_c"),
            "humidity_per": row.get("humidity_per"),
            "thi": row.get("thi"),
            "thi_risk": classify_thi_value(row.get("thi")),
            "last_update_ts": batch_ts,
        }], columns=cols)

    temperature_c = env["temperature_c"].dropna().mean() if env["temperature_c"].notna().any() else None
    humidity_per = env["humidity_per"].dropna().mean() if env["humidity_per"].notna().any() else None
    thi = env["thi"].dropna().mean() if env["thi"].notna().any() else None
    event_time = env["event_time"].max() if env["event_time"].notna().any() else None

    return pd.DataFrame([{
        "source_sensor_id": "GLOBAL",
        "source_mode": "mean_of_current_sensors",
        "event_time": event_time,
        "temperature_c": temperature_c,
        "humidity_per": humidity_per,
        "thi": thi,
        "thi_risk": classify_thi_value(thi),
        "last_update_ts": batch_ts,
    }], columns=cols)


def severity_for_temperature(value):
    if value is None or pd.isna(value):
        return None
    if float(value) >= TEMP_CRITICAL:
        return "critical"
    if float(value) >= TEMP_HIGH:
        return "warning"
    return None


def severity_for_long_lying(value):
    if value is None or pd.isna(value):
        return None
    if float(value) >= LONG_LYING_CRITICAL_MINUTES_3H:
        return "critical"
    if float(value) >= LONG_LYING_MINUTES_3H:
        return "warning"
    return None


def severity_for_low_activity(value):
    if value is None or pd.isna(value):
        return None
    if float(value) <= LOW_ACTIVITY_CRITICAL_DISTANCE_M_6H:
        return "critical"
    if float(value) <= LOW_ACTIVITY_DISTANCE_M_6H:
        return "warning"
    return None


def severity_for_thi(value):
    if value is None or pd.isna(value):
        return None
    if float(value) >= THI_DANGER:
        return "critical"
    if float(value) >= THI_WARNING:
        return "warning"
    return None


def severity_for_head_down(value):
    if value is None or pd.isna(value):
        return None
    if float(value) >= HEAD_DOWN_RATIO_CRITICAL:
        return "critical"
    if float(value) >= HEAD_DOWN_RATIO_ALERT:
        return "warning"
    return None


def make_alert_row(
    batch_ts: datetime,
    entity_type: str,
    entity_id: str,
    alert_code: str,
    alert_name: str,
    severity: str,
    metric_value,
    threshold_value,
    description: str,
):
    return {
        "entity_type": entity_type,
        "entity_id": entity_id,
        "alert_code": alert_code,
        "alert_name": alert_name,
        "severity": severity,
        "metric_value": None if pd.isna(metric_value) else float(metric_value),
        "threshold_value": None if threshold_value is None else float(threshold_value),
        "description": description,
        "first_seen": batch_ts,
        "last_seen": batch_ts,
        "is_open": 1,
        "last_update_ts": batch_ts,
    }


def build_open_alerts_pdf(cow_current_pdf: pd.DataFrame, tag_current_pdf: pd.DataFrame, environment_global_pdf: pd.DataFrame, batch_ts: datetime):
    rows = []

    if cow_current_pdf is not None and not cow_current_pdf.empty:
        for _, row in cow_current_pdf.iterrows():
            cow_id = str(row["cow_id"])

            temp_value = row.get("max_temp_1h")
            if pd.isna(temp_value):
                temp_value = row.get("last_temp_c")
            temp_severity = severity_for_temperature(temp_value)
            if temp_severity is not None:
                rows.append(make_alert_row(
                    batch_ts=batch_ts,
                    entity_type="cow",
                    entity_id=cow_id,
                    alert_code="HIGH_TEMPERATURE",
                    alert_name="Высокая температура",
                    severity=temp_severity,
                    metric_value=temp_value,
                    threshold_value=TEMP_HIGH if temp_severity == "warning" else TEMP_CRITICAL,
                    description=f"У коровы {cow_id} обнаружена повышенная температура.",
                ))

            long_lying_value = row.get("lying_minutes_3h")
            long_lying_severity = severity_for_long_lying(long_lying_value)
            if long_lying_severity is not None:
                rows.append(make_alert_row(
                    batch_ts=batch_ts,
                    entity_type="cow",
                    entity_id=cow_id,
                    alert_code="LONG_LYING",
                    alert_name="Длительное лежание",
                    severity=long_lying_severity,
                    metric_value=long_lying_value,
                    threshold_value=LONG_LYING_MINUTES_3H if long_lying_severity == "warning" else LONG_LYING_CRITICAL_MINUTES_3H,
                    description=f"Корова {cow_id} слишком долго лежит за последние 3 часа.",
                ))

    if tag_current_pdf is not None and not tag_current_pdf.empty:
        for _, row in tag_current_pdf.iterrows():
            tag_id = str(row["tag_id"])

            activity_value = row.get("distance_m_6h")
            activity_severity = severity_for_low_activity(activity_value)
            if activity_severity is not None:
                rows.append(make_alert_row(
                    batch_ts=batch_ts,
                    entity_type="tag",
                    entity_id=tag_id,
                    alert_code="LOW_ACTIVITY",
                    alert_name="Низкая активность",
                    severity=activity_severity,
                    metric_value=activity_value,
                    threshold_value=LOW_ACTIVITY_DISTANCE_M_6H if activity_severity == "warning" else LOW_ACTIVITY_CRITICAL_DISTANCE_M_6H,
                    description=f"Низкая двигательная активность тега {tag_id} за последние 6 часов.",
                ))

            if USE_HEAD_DOWN_EXPERIMENTAL:
                head_down_value = row.get("head_down_ratio_1h")
                head_down_severity = severity_for_head_down(head_down_value)
                if head_down_severity is not None:
                    rows.append(make_alert_row(
                        batch_ts=batch_ts,
                        entity_type="tag",
                        entity_id=tag_id,
                        alert_code="HEAD_DOWN_FREQUENT_EXPERIMENTAL",
                        alert_name="Частое положение головы вниз",
                        severity=head_down_severity,
                        metric_value=head_down_value,
                        threshold_value=HEAD_DOWN_RATIO_ALERT if head_down_severity == "warning" else HEAD_DOWN_RATIO_CRITICAL,
                        description=f"Экспериментальный индикатор по IMMU: tag {tag_id} часто держит голову вниз.",
                    ))

    if environment_global_pdf is not None and not environment_global_pdf.empty:
        row = environment_global_pdf.iloc[0]
        thi_value = row.get("thi")
        thi_severity = severity_for_thi(thi_value)
        if thi_severity is not None:
            rows.append(make_alert_row(
                batch_ts=batch_ts,
                entity_type="environment",
                entity_id="GLOBAL",
                alert_code="HIGH_THI",
                alert_name="Тепловой стресс",
                severity=thi_severity,
                metric_value=thi_value,
                threshold_value=THI_WARNING if thi_severity == "warning" else THI_DANGER,
                description="Обнаружен риск теплового стресса по интегральному индексу THI.",
            ))

    if not rows:
        return pd.DataFrame(columns=TABLE_COLUMNS["gold_alerts_open"])

    return pd.DataFrame(rows)


def preserve_first_seen(open_alerts_pdf: pd.DataFrame, previous_open_pdf: pd.DataFrame, batch_ts: datetime):
    cols = TABLE_COLUMNS["gold_alerts_open"]

    if open_alerts_pdf is None or open_alerts_pdf.empty:
        return pd.DataFrame(columns=cols)

    out = open_alerts_pdf.copy()
    if previous_open_pdf is None or previous_open_pdf.empty:
        out["first_seen"] = batch_ts
        out["last_seen"] = batch_ts
        out["is_open"] = 1
        out["last_update_ts"] = batch_ts
        return out[cols]

    prev = previous_open_pdf[["entity_type", "entity_id", "alert_code", "first_seen"]].copy()
    out = out.merge(prev, on=["entity_type", "entity_id", "alert_code"], how="left", suffixes=("", "_prev"))
    out["first_seen"] = out["first_seen_prev"].where(out["first_seen_prev"].notna(), out["first_seen"])
    out = out.drop(columns=["first_seen_prev"])
    out["last_seen"] = batch_ts
    out["is_open"] = 1
    out["last_update_ts"] = batch_ts
    return out[cols]


def build_alert_history_pdf(previous_open_pdf: pd.DataFrame, open_alerts_pdf: pd.DataFrame, batch_ts: datetime):
    cols = TABLE_COLUMNS["gold_alerts_history"]

    previous_open_pdf = previous_open_pdf.copy() if previous_open_pdf is not None else pd.DataFrame()
    open_alerts_pdf = open_alerts_pdf.copy() if open_alerts_pdf is not None else pd.DataFrame()

    prev_keys = set()
    curr_keys = set()

    if not previous_open_pdf.empty:
        prev_keys = set(zip(previous_open_pdf["entity_type"], previous_open_pdf["entity_id"], previous_open_pdf["alert_code"]))
    if not open_alerts_pdf.empty:
        curr_keys = set(zip(open_alerts_pdf["entity_type"], open_alerts_pdf["entity_id"], open_alerts_pdf["alert_code"]))

    opened_keys = curr_keys - prev_keys
    closed_keys = prev_keys - curr_keys

    rows = []

    if not open_alerts_pdf.empty:
        for _, row in open_alerts_pdf.iterrows():
            key = (row["entity_type"], row["entity_id"], row["alert_code"])
            if key not in opened_keys:
                continue
            rows.append({
                "event_id": str(uuid.uuid4()),
                "alert_event_time": batch_ts,
                "entity_type": row["entity_type"],
                "entity_id": row["entity_id"],
                "alert_code": row["alert_code"],
                "alert_name": row["alert_name"],
                "severity": row["severity"],
                "metric_value": row.get("metric_value"),
                "threshold_value": row.get("threshold_value"),
                "description": row["description"],
                "event_status": "opened",
                "first_seen": row["first_seen"],
                "last_seen": row["last_seen"],
            })

    if not previous_open_pdf.empty:
        prev_lookup = previous_open_pdf.set_index(["entity_type", "entity_id", "alert_code"])
        for key in closed_keys:
            row = prev_lookup.loc[key]
            rows.append({
                "event_id": str(uuid.uuid4()),
                "alert_event_time": batch_ts,
                "entity_type": key[0],
                "entity_id": key[1],
                "alert_code": key[2],
                "alert_name": row["alert_name"],
                "severity": row["severity"],
                "metric_value": row.get("metric_value"),
                "threshold_value": row.get("threshold_value"),
                "description": row["description"],
                "event_status": "closed",
                "first_seen": row["first_seen"],
                "last_seen": batch_ts,
            })

    if not rows:
        return pd.DataFrame(columns=cols)

    return pd.DataFrame(rows, columns=cols)


def build_entity_alert_stats_pdf(alerts_open_pdf: pd.DataFrame, entity_type: str):
    if alerts_open_pdf is None or alerts_open_pdf.empty:
        return pd.DataFrame(columns=["entity_id", "open_alerts_count", "risk_score"])

    df = alerts_open_pdf[alerts_open_pdf["entity_type"] == entity_type].copy()
    if df.empty:
        return pd.DataFrame(columns=["entity_id", "open_alerts_count", "risk_score"])

    base_weights = {
        "HIGH_TEMPERATURE": 3.0,
        "LONG_LYING": 2.0,
        "LOW_ACTIVITY": 2.0,
        "HIGH_THI": 3.0,
        "HEAD_DOWN_FREQUENT_EXPERIMENTAL": 1.0,
    }

    severity_multiplier = {
        "warning": 1.0,
        "critical": 1.5,
    }

    df["base_weight"] = df["alert_code"].map(base_weights).fillna(1.0)
    df["severity_mult"] = df["severity"].map(severity_multiplier).fillna(1.0)
    df["risk_part"] = df["base_weight"] * df["severity_mult"]

    return (
        df.groupby("entity_id", as_index=False)
          .agg(
              open_alerts_count=("alert_code", "count"),
              risk_score=("risk_part", "sum")
          )
    )


def verify_clickhouse_write(client):
    client.command(
        f"""
        CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.__gold_probe
        (
            x UInt8
        )
        ENGINE = Memory
        """
    )
    client.command(f"TRUNCATE TABLE {CLICKHOUSE_DATABASE}.__gold_probe")
    client.command(f"INSERT INTO {CLICKHOUSE_DATABASE}.__gold_probe VALUES (1)")


def process_gold_batch(batch_df, batch_id: int):
    print(f"[gold] processing batch_id={batch_id}")

    client = get_clickhouse_client()
    ensure_clickhouse_tables(client)
    verify_clickhouse_write(client)

    reference_ts = get_reference_ts()
    batch_ts = datetime.utcnow().replace(microsecond=0)

    cow_current_df = compute_cow_current(reference_ts)
    cow_day_df = compute_cow_day()
    tag_current_df = compute_tag_current(reference_ts)
    tag_day_df = compute_tag_day()
    environment_current_df = compute_environment_current()

    cow_current_pdf = prepare_for_pandas(cow_current_df).toPandas()
    cow_day_pdf = prepare_for_pandas(cow_day_df).toPandas()
    tag_current_pdf = prepare_for_pandas(tag_current_df).toPandas()
    tag_day_pdf = prepare_for_pandas(tag_day_df).toPandas()
    environment_current_raw_pdf = prepare_for_pandas(environment_current_df).toPandas()
    environment_current_pdf = build_environment_current_pdf(environment_current_raw_pdf)
    environment_global_pdf = build_environment_global_pdf(environment_current_pdf, batch_ts)

    previous_open_pdf = load_previous_open_alerts(client)
    open_alerts_pdf = build_open_alerts_pdf(
        cow_current_pdf=cow_current_pdf,
        tag_current_pdf=tag_current_pdf,
        environment_global_pdf=environment_global_pdf,
        batch_ts=batch_ts,
    )
    open_alerts_pdf = preserve_first_seen(open_alerts_pdf, previous_open_pdf, batch_ts)
    alert_history_pdf = build_alert_history_pdf(previous_open_pdf, open_alerts_pdf, batch_ts)

    cow_alert_stats_pdf = build_entity_alert_stats_pdf(open_alerts_pdf, "cow")
    tag_alert_stats_pdf = build_entity_alert_stats_pdf(open_alerts_pdf, "tag")

    if cow_current_pdf.empty:
        cow_current_pdf = pd.DataFrame(columns=TABLE_COLUMNS["gold_cow_current"])
    else:
        cow_current_pdf = cow_current_pdf.merge(
            cow_alert_stats_pdf.rename(columns={"entity_id": "cow_id"}),
            on="cow_id",
            how="left",
        )
        cow_current_pdf["open_alerts_count"] = cow_current_pdf["open_alerts_count"].fillna(0).astype(int)
        cow_current_pdf["risk_score"] = cow_current_pdf["risk_score"].fillna(0.0).astype(float)
        if "lying_now" in cow_current_pdf.columns:
            cow_current_pdf["lying_now"] = pd.Series(
                [None if pd.isna(x) else int(x) for x in cow_current_pdf["lying_now"]],
                index=cow_current_pdf.index,
                dtype=object,
            )
        cow_current_pdf = align_pdf_to_table("gold_cow_current", cow_current_pdf)

    if tag_current_pdf.empty:
        tag_current_pdf = pd.DataFrame(columns=TABLE_COLUMNS["gold_tag_current"])
    else:
        tag_current_pdf = tag_current_pdf.merge(
            tag_alert_stats_pdf.rename(columns={"entity_id": "tag_id"}),
            on="tag_id",
            how="left",
        )
        tag_current_pdf["open_alerts_count"] = tag_current_pdf["open_alerts_count"].fillna(0).astype(int)
        tag_current_pdf["risk_score"] = tag_current_pdf["risk_score"].fillna(0.0).astype(float)
        tag_current_pdf = align_pdf_to_table("gold_tag_current", tag_current_pdf)

    cow_day_pdf = align_pdf_to_table("gold_cow_day", cow_day_pdf)
    tag_day_pdf = align_pdf_to_table("gold_tag_day", tag_day_pdf)
    environment_current_pdf = align_pdf_to_table("gold_environment_current", environment_current_pdf)
    environment_global_pdf = align_pdf_to_table("gold_environment_global_current", environment_global_pdf)
    open_alerts_pdf = align_pdf_to_table("gold_alerts_open", open_alerts_pdf)
    alert_history_pdf = align_pdf_to_table("gold_alerts_history", alert_history_pdf)

    overwrite_table(client, "gold_cow_current", cow_current_pdf)
    overwrite_table(client, "gold_cow_day", cow_day_pdf)
    overwrite_table(client, "gold_tag_current", tag_current_pdf)
    overwrite_table(client, "gold_tag_day", tag_day_pdf)
    overwrite_table(client, "gold_environment_current", environment_current_pdf)
    overwrite_table(client, "gold_environment_global_current", environment_global_pdf)
    overwrite_table(client, "gold_alerts_open", open_alerts_pdf)
    append_table(client, "gold_alerts_history", alert_history_pdf)

    global _GOLD_READY_MARKED

    if not _GOLD_READY_MARKED:
        Path("/tmp/gold_ready").write_text("ready")
        print("Gold consumer is ready")
        _GOLD_READY_MARKED = True

    print(f"[gold] batch_id={batch_id} completed")


heartbeat_df = (
    spark.readStream
        .format("rate")
        .option("rowsPerSecond", 1)
        .load()
)

gold_query = (
    heartbeat_df.writeStream
        .queryName("gold_refresh")
        .trigger(processingTime=GOLD_TRIGGER_INTERVAL)
        .option("checkpointLocation", f"{CHECKPOINT_BASE_PATH}/gold_refresh")
        .foreachBatch(process_gold_batch)
        .start()
)

spark.streams.awaitAnyTermination()