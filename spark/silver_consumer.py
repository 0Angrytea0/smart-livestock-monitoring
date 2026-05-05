from pathlib import Path
import os

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    trim,
    lower,
    current_timestamp,
    to_timestamp,
    to_date,
    coalesce,
    when,
    isnan,
    to_json,
    concat_ws,
    size,
    split,
)
from pyspark.storagelevel import StorageLevel
from functools import reduce

CHECKPOINT_BASE_PATH = "/opt/spark/checkpoints/silver"
SILVER_TRIGGER_INTERVAL = os.getenv("SILVER_TRIGGER_INTERVAL", "30 seconds")
SILVER_QUARANTINE_TABLE = os.getenv("SILVER_QUARANTINE_TABLE", "iceberg.silver_quarantine")
SILVER_MODE = os.getenv("SILVER_MODE", "valid").strip().lower()

if SILVER_MODE not in {"valid", "quarantine"}:
    raise ValueError("SILVER_MODE must be either 'valid' or 'quarantine'")

Path(CHECKPOINT_BASE_PATH).mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder
        .appName(f"SilverIngestion-{SILVER_MODE}")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", "s3a://warehouse/")
        .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.adaptive.enabled", "false")
        .config("spark.network.timeout", "120s")
        .config("spark.executor.heartbeatInterval", "30s")
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

SENSOR_CONFIGS = {
    "cbt": {
        "bronze_table": "iceberg.mmcows_cbt",
        "silver_table": "iceberg.silver_cbt",
        "entity_column": "cow_id",
        "sensor_type": "cbt",
        "expected_record_type": "cow",
        "metric_columns": [
            ("temperature_c", "metrics.temperature", "double"),
        ],
    },
    "ankle": {
        "bronze_table": "iceberg.mmcows_ankle",
        "silver_table": "iceberg.silver_ankle",
        "entity_column": "cow_id",
        "sensor_type": "ankle",
        "expected_record_type": "cow",
        "metric_columns": [
            ("lying", "metrics.lying", "int"),
        ],
    },
    "immu": {
        "bronze_table": "iceberg.mmcows_immu",
        "silver_table": "iceberg.silver_immu",
        "entity_column": "tag_id",
        "sensor_type": "immu",
        "expected_record_type": "tag",
        "metric_columns": [
            ("accel_x_mps2", "metrics.accel_x_mps2", "double"),
            ("accel_y_mps2", "metrics.accel_y_mps2", "double"),
            ("accel_z_mps2", "metrics.accel_z_mps2", "double"),
            ("mag_x_uT", "metrics.mag_x_uT", "double"),
            ("mag_y_uT", "metrics.mag_y_uT", "double"),
            ("mag_z_uT", "metrics.mag_z_uT", "double"),
        ],
    },
    "pressure": {
        "bronze_table": "iceberg.mmcows_pressure",
        "silver_table": "iceberg.silver_pressure",
        "entity_column": "tag_id",
        "sensor_type": "pressure",
        "expected_record_type": "tag",
        "metric_columns": [
            ("pressure_pa", "metrics.pressure_Pa", "double"),
            ("elevation_m", "metrics.elevation_m", "double"),
        ],
    },
    "uwb": {
        "bronze_table": "iceberg.mmcows_uwb",
        "silver_table": "iceberg.silver_uwb",
        "entity_column": "tag_id",
        "sensor_type": "uwb",
        "expected_record_type": "tag",
        "metric_columns": [
            ("coord_x_cm", "metrics.coord_x_cm", "double"),
            ("coord_y_cm", "metrics.coord_y_cm", "double"),
            ("coord_z_cm", "metrics.coord_z_cm", "double"),
        ],
    },
    "milk": {
        "bronze_table": "iceberg.mmcows_milk",
        "silver_table": "iceberg.silver_milk",
        "entity_column": "cow_id",
        "sensor_type": "milk",
        "expected_record_type": "cow",
        "metric_columns": [
            ("milk_weight_kg", "metrics.milk_weight_kg", "double"),
            ("dim", "metrics.DIM", "double"),
        ],
    },
    "thi": {
        "bronze_table": "iceberg.mmcows_thi",
        "silver_table": "iceberg.silver_thi",
        "entity_column": "sensor_id",
        "sensor_type": "thi",
        "expected_record_type": "environment",
        "metric_columns": [
            ("temperature_c", "metrics.temperature_C", "double"),
            ("humidity_per", "metrics.humidity_per", "double"),
            ("thi", "metrics.THI", "double"),
        ],
    },
}


def is_blank(column_name: str):
    return col(column_name).isNull() | (trim(col(column_name)) == "")


def is_null_or_nan(column_name: str):
    return col(column_name).isNull() | isnan(col(column_name))


def build_missing_fields(*rules):
    return concat_ws(",", *[when(condition, lit(field_name)) for field_name, condition in rules])


def missing_count_expr(column_name: str):
    return when(trim(col(column_name)) == "", lit(0)).otherwise(size(split(col(column_name), ",")))


def create_silver_table_if_not_exists(sensor_name: str, config: dict):
    table_name = config["silver_table"]
    entity_column = config["entity_column"]

    metric_sql = []
    for metric_name, _, metric_type in config["metric_columns"]:
        metric_sql.append(f"{metric_name} {metric_type}")

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            event_time timestamp,
            event_date date,
            {entity_column} string,
            sensor_type string,
            {", ".join(metric_sql)},
            quality_status string,
            missing_fields string,
            missing_fields_count int,
            source_table string,
            bronze_ingest_ts timestamp,
            silver_ingest_ts timestamp
        )
        USING iceberg
        PARTITIONED BY (event_date)
        """
    )

def create_quarantine_table_if_not_exists(table_name: str):
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            source_table string,
            target_table string,
            raw_json string,
            parse_status string,
            raw_time string,
            raw_type string,
            raw_id string,
            raw_sensor string,
            raw_metrics string,
            event_time timestamp,
            kafka_date date,
            kafka_key string,
            kafka_topic string,
            kafka_partition int,
            kafka_offset bigint,
            kafka_timestamp timestamp,
            bronze_ingest_ts timestamp,
            quarantine_ts timestamp,
            error_reason string
        )
        USING iceberg
        PARTITIONED BY (kafka_date)
        """
    )

def build_normalized_df(config: dict):
    bronze_table = config["bronze_table"]
    entity_column = config["entity_column"]

    df_bronze = (
        spark.readStream
            .format("iceberg")
            .option("streaming-skip-delete-snapshots", "true")
            .option("streaming-skip-overwrite-snapshots", "true")
            .load(bronze_table)
    )

    select_exprs = [
        coalesce(col("event_ts"), to_timestamp(col("time"))).alias("event_time"),
        trim(col("time")).alias("raw_time"),
        trim(col("type")).alias("raw_type"),
        lower(trim(col("type"))).alias("record_type"),
        trim(col("id")).alias("raw_id"),
        trim(col("sensor")).alias("raw_sensor"),
        lower(trim(col("sensor"))).alias("sensor_type"),
        to_json(col("metrics")).alias("raw_metrics"),
        col("raw_json"),
        col("parse_status"),
        trim(col("id")).alias(entity_column),
        lit(bronze_table).alias("source_table"),
        col("kafka_key"),
        col("kafka_topic"),
        col("kafka_partition"),
        col("kafka_offset"),
        col("kafka_timestamp"),
        to_date(col("kafka_timestamp")).alias("kafka_date"),
        col("ingest_ts").alias("bronze_ingest_ts"),
        current_timestamp().alias("silver_ingest_ts"),
    ]

    for metric_name, metric_path, metric_type in config["metric_columns"]:
        select_exprs.append(col(metric_path).cast(metric_type).alias(metric_name))

    return df_bronze.select(*select_exprs)

def apply_quality_logic(df, sensor_name: str, config: dict):
    entity_column = config["entity_column"]
    sensor_type = config["sensor_type"]
    expected_record_type = config["expected_record_type"]

    base_quarantine_reason = (
        when(col("parse_status") != lit("parsed"), concat_ws("", lit("bronze_parse_status_"), col("parse_status")))
        .when(col("event_time").isNull(), lit("event_time_is_null"))
        .when(is_blank(entity_column), lit(f"{entity_column}_is_null_or_blank"))
        .when(is_blank("sensor_type"), lit("sensor_type_is_null_or_blank"))
        .when(col("sensor_type") != lit(sensor_type), lit(f"unexpected_sensor_type_{sensor_type}"))
        .when(is_blank("record_type"), lit("record_type_is_null_or_blank"))
        .when(col("record_type") != lit(expected_record_type), lit(f"unexpected_record_type_{expected_record_type}"))
    )

    if sensor_name == "cbt":
        missing_fields_col = build_missing_fields(("temperature_c", is_null_or_nan("temperature_c")))
        quarantine_reason = (
            base_quarantine_reason
            .when(is_null_or_nan("temperature_c"), lit("temperature_c_is_null_or_nan"))
            .when(col("temperature_c") < lit(30.0), lit("temperature_c_below_min"))
            .when(col("temperature_c") > lit(45.0), lit("temperature_c_above_max"))
        )
        return (
            df.withColumn("error_reason", quarantine_reason.otherwise(lit(None)))
              .withColumn("missing_fields", missing_fields_col)
              .withColumn("missing_fields_count", missing_count_expr("missing_fields"))
              .withColumn("quality_status", when(col("error_reason").isNotNull(), lit("quarantine")).otherwise(lit("valid")))
        )

    if sensor_name == "ankle":
        missing_fields_col = build_missing_fields(("lying", col("lying").isNull()))
        quarantine_reason = (
            base_quarantine_reason
            .when(col("lying").isNull(), lit("lying_is_null"))
            .when(~col("lying").isin(0, 1), lit("lying_not_in_0_1"))
        )
        return (
            df.withColumn("error_reason", quarantine_reason.otherwise(lit(None)))
              .withColumn("missing_fields", missing_fields_col)
              .withColumn("missing_fields_count", missing_count_expr("missing_fields"))
              .withColumn("quality_status", when(col("error_reason").isNotNull(), lit("quarantine")).otherwise(lit("valid")))
        )

    if sensor_name == "pressure":
        missing_fields_col = build_missing_fields(("elevation_m", is_null_or_nan("elevation_m")))
        quarantine_reason = (
            base_quarantine_reason
            .when(is_null_or_nan("pressure_pa"), lit("pressure_pa_is_null_or_nan"))
            .when(col("pressure_pa") <= lit(0.0), lit("pressure_pa_not_positive"))
        )
        return (
            df.withColumn("error_reason", quarantine_reason.otherwise(lit(None)))
              .withColumn("missing_fields", missing_fields_col)
              .withColumn("missing_fields_count", missing_count_expr("missing_fields"))
              .withColumn(
                  "quality_status",
                  when(col("error_reason").isNotNull(), lit("quarantine"))
                  .when(col("missing_fields_count") > lit(0), lit("partial"))
                  .otherwise(lit("valid"))
              )
        )

    if sensor_name == "uwb":
        missing_fields_col = build_missing_fields(("coord_z_cm", is_null_or_nan("coord_z_cm")))
        quarantine_reason = (
            base_quarantine_reason
            .when(is_null_or_nan("coord_x_cm"), lit("coord_x_cm_is_null_or_nan"))
            .when(is_null_or_nan("coord_y_cm"), lit("coord_y_cm_is_null_or_nan"))
        )
        return (
            df.withColumn("error_reason", quarantine_reason.otherwise(lit(None)))
              .withColumn("missing_fields", missing_fields_col)
              .withColumn("missing_fields_count", missing_count_expr("missing_fields"))
              .withColumn(
                  "quality_status",
                  when(col("error_reason").isNotNull(), lit("quarantine"))
                  .when(col("missing_fields_count") > lit(0), lit("partial"))
                  .otherwise(lit("valid"))
              )
        )

    if sensor_name == "milk":
        missing_fields_col = build_missing_fields(("dim", is_null_or_nan("dim")))
        quarantine_reason = (
            base_quarantine_reason
            .when(is_null_or_nan("milk_weight_kg"), lit("milk_weight_kg_is_null_or_nan"))
            .when(col("milk_weight_kg") < lit(0.0), lit("milk_weight_kg_negative"))
            .when(col("dim").isNotNull() & ~isnan(col("dim")) & (col("dim") < lit(0.0)), lit("dim_negative"))
        )
        return (
            df.withColumn("error_reason", quarantine_reason.otherwise(lit(None)))
              .withColumn("missing_fields", missing_fields_col)
              .withColumn("missing_fields_count", missing_count_expr("missing_fields"))
              .withColumn(
                  "quality_status",
                  when(col("error_reason").isNotNull(), lit("quarantine"))
                  .when(col("missing_fields_count") > lit(0), lit("partial"))
                  .otherwise(lit("valid"))
              )
        )

    if sensor_name == "thi":
        all_metrics_missing = is_null_or_nan("temperature_c") & is_null_or_nan("humidity_per") & is_null_or_nan("thi")
        missing_fields_col = build_missing_fields(
            ("temperature_c", is_null_or_nan("temperature_c")),
            ("humidity_per", is_null_or_nan("humidity_per")),
            ("thi", is_null_or_nan("thi")),
        )
        quarantine_reason = (
            base_quarantine_reason
            .when(all_metrics_missing, lit("all_thi_metrics_are_null_or_nan"))
            .when(col("humidity_per").isNotNull() & ~isnan(col("humidity_per")) & (col("humidity_per") < lit(0.0)), lit("humidity_per_below_min"))
            .when(col("humidity_per").isNotNull() & ~isnan(col("humidity_per")) & (col("humidity_per") > lit(100.0)), lit("humidity_per_above_max"))
        )
        return (
            df.withColumn("error_reason", quarantine_reason.otherwise(lit(None)))
              .withColumn("missing_fields", missing_fields_col)
              .withColumn("missing_fields_count", missing_count_expr("missing_fields"))
              .withColumn(
                  "quality_status",
                  when(col("error_reason").isNotNull(), lit("quarantine"))
                  .when(col("missing_fields_count") > lit(0), lit("partial"))
                  .otherwise(lit("valid"))
              )
        )

    if sensor_name == "immu":
        accel_x_missing = is_null_or_nan("accel_x_mps2")
        accel_y_missing = is_null_or_nan("accel_y_mps2")
        accel_z_missing = is_null_or_nan("accel_z_mps2")
        mag_x_missing = is_null_or_nan("mag_x_uT")
        mag_y_missing = is_null_or_nan("mag_y_uT")
        mag_z_missing = is_null_or_nan("mag_z_uT")

        accel_any = (~accel_x_missing) | (~accel_y_missing) | (~accel_z_missing)
        mag_any = (~mag_x_missing) | (~mag_y_missing) | (~mag_z_missing)
        accel_complete = (~accel_x_missing) & (~accel_y_missing) & (~accel_z_missing)
        mag_complete = (~mag_x_missing) & (~mag_y_missing) & (~mag_z_missing)

        missing_fields_col = build_missing_fields(
            ("accel_x_mps2", accel_x_missing),
            ("accel_y_mps2", accel_y_missing),
            ("accel_z_mps2", accel_z_missing),
            ("mag_x_uT", mag_x_missing),
            ("mag_y_uT", mag_y_missing),
            ("mag_z_uT", mag_z_missing),
        )
        quarantine_reason = base_quarantine_reason.when((~accel_any) & (~mag_any), lit("all_immu_metrics_are_null_or_nan"))

        return (
            df.withColumn("error_reason", quarantine_reason.otherwise(lit(None)))
              .withColumn("missing_fields", missing_fields_col)
              .withColumn("missing_fields_count", missing_count_expr("missing_fields"))
              .withColumn(
                  "quality_status",
                  when(col("error_reason").isNotNull(), lit("quarantine"))
                  .when(accel_complete & mag_complete, lit("valid"))
                  .otherwise(lit("partial"))
              )
        )

    return (
        df.withColumn("error_reason", lit("unsupported_sensor"))
          .withColumn("missing_fields", lit(""))
          .withColumn("missing_fields_count", lit(0))
          .withColumn("quality_status", lit("quarantine"))
    )


def build_validated_stream(sensor_name: str, config: dict):
    return apply_quality_logic(build_normalized_df(config), sensor_name, config)


def build_silver_output_df(df: DataFrame, config: dict) -> DataFrame:
    return (
        df.select(
            col("event_time"),
            to_date(col("event_time")).alias("event_date"),
            col(config["entity_column"]),
            col("sensor_type"),
            *[col(metric_name) for metric_name, _, _ in config["metric_columns"]],
            col("quality_status"),
            col("missing_fields"),
            col("missing_fields_count"),
            col("source_table"),
            col("bronze_ingest_ts"),
            col("silver_ingest_ts"),
        )
    )


def build_quarantine_output_df(df: DataFrame, config: dict) -> DataFrame:
    return (
        df.select(
            col("source_table"),
            lit(config["silver_table"]).alias("target_table"),
            col("raw_json"),
            col("parse_status"),
            col("raw_time"),
            col("raw_type"),
            col("raw_id"),
            col("raw_sensor"),
            col("raw_metrics"),
            col("event_time"),
            col("kafka_date"),
            col("kafka_key"),
            col("kafka_topic"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
            col("bronze_ingest_ts"),
            current_timestamp().alias("quarantine_ts"),
            col("error_reason"),
        )
    )


def process_valid_batch(sensor_name: str, config: dict):
    def _process(batch_df, batch_id: int):
        print(f"[silver-valid][{sensor_name}] batch_id={batch_id} started")

        batch_df = batch_df.persist(StorageLevel.MEMORY_AND_DISK)

        try:
            total_count = batch_df.count()
            print(f"[silver-valid][{sensor_name}] incoming_rows={total_count}")

            if total_count == 0:
                print(f"[silver-valid][{sensor_name}] empty batch")
                return

            dedup_df = batch_df.dropDuplicates(["source_table", "kafka_partition", "kafka_offset"]).persist(StorageLevel.MEMORY_AND_DISK)

            try:
                dedup_count = dedup_df.count()
                valid_df = dedup_df.filter(col("error_reason").isNull()).persist(StorageLevel.MEMORY_AND_DISK)
                valid_count = valid_df.count()
                quarantine_count = dedup_count - valid_count

                print(f"[silver-valid][{sensor_name}] dedup_rows={dedup_count}")
                print(f"[silver-valid][{sensor_name}] valid_rows={valid_count}")
                print(f"[silver-valid][{sensor_name}] quarantine_rows_seen={quarantine_count}")

                try:
                    if valid_count > 0:
                        build_silver_output_df(valid_df, config).writeTo(config["silver_table"]).append()

                    print(f"[silver-valid][{sensor_name}] batch_id={batch_id} completed")
                finally:
                    valid_df.unpersist()
            finally:
                dedup_df.unpersist()
        finally:
            batch_df.unpersist()

    return _process


def process_quarantine_batch(selected_sensor_names):
    def _process(batch_df, batch_id: int):
        print(f"[silver-quarantine] batch_id={batch_id} started")

        batch_df = batch_df.persist(StorageLevel.MEMORY_AND_DISK)

        try:
            total_count = batch_df.count()
            print(f"[silver-quarantine] incoming_rows={total_count}")

            if total_count == 0:
                print(f"[silver-quarantine] empty batch")
                return

            dedup_df = batch_df.dropDuplicates(["source_table", "kafka_partition", "kafka_offset"]).persist(StorageLevel.MEMORY_AND_DISK)

            try:
                dedup_count = dedup_df.count()
                quarantine_df = dedup_df.filter(col("error_reason").isNotNull()).persist(StorageLevel.MEMORY_AND_DISK)

                try:
                    quarantine_count = quarantine_df.count()
                    valid_seen_count = dedup_count - quarantine_count

                    print(f"[silver-quarantine] dedup_rows={dedup_count}")
                    print(f"[silver-quarantine] valid_rows_seen={valid_seen_count}")
                    print(f"[silver-quarantine] quarantine_rows={quarantine_count}")

                    if quarantine_count > 0:
                        quarantine_df.writeTo(SILVER_QUARANTINE_TABLE).append()

                    print(
                        f"[silver-quarantine] batch_id={batch_id} completed for sensors={','.join(selected_sensor_names)}"
                    )
                finally:
                    quarantine_df.unpersist()
            finally:
                dedup_df.unpersist()
        finally:
            batch_df.unpersist()

    return _process


def start_valid_stream(sensor_name: str, config: dict):
    checkpoint_suffix = f"valid_{sensor_name}".replace(".", "_")
    checkpoint_path = f"{CHECKPOINT_BASE_PATH}/{checkpoint_suffix}"
    Path(checkpoint_path).mkdir(parents=True, exist_ok=True)

    validated_stream = build_validated_stream(sensor_name, config)

    return (
        validated_stream.writeStream
            .queryName(f"silver_valid_{sensor_name}")
            .trigger(processingTime=SILVER_TRIGGER_INTERVAL)
            .option("checkpointLocation", checkpoint_path)
            .foreachBatch(process_valid_batch(sensor_name, config))
            .start()
    )


def build_unified_quarantine_stream(selected_sensor_names) -> DataFrame:
    quarantine_streams = []

    for sensor_name in selected_sensor_names:
        config = SENSOR_CONFIGS[sensor_name]
        validated_stream = build_validated_stream(sensor_name, config)
        quarantine_streams.append(build_quarantine_output_df(validated_stream, config))

    if not quarantine_streams:
        raise ValueError("No sensor streams selected for quarantine mode")

    return reduce(lambda left, right: left.unionByName(right), quarantine_streams)


def start_quarantine_stream(selected_sensor_names):
    checkpoint_path = f"{CHECKPOINT_BASE_PATH}/quarantine_union"
    Path(checkpoint_path).mkdir(parents=True, exist_ok=True)

    quarantine_stream = build_unified_quarantine_stream(selected_sensor_names)

    return (
        quarantine_stream.writeStream
            .queryName("silver_quarantine_union")
            .trigger(processingTime=SILVER_TRIGGER_INTERVAL)
            .option("checkpointLocation", checkpoint_path)
            .foreachBatch(process_quarantine_batch(selected_sensor_names))
            .start()
    )


enabled_sensors_raw = os.getenv("SILVER_SENSORS", "").strip()
if enabled_sensors_raw:
    selected_sensors = [s.strip() for s in enabled_sensors_raw.split(",") if s.strip()]
    unknown = [s for s in selected_sensors if s not in SENSOR_CONFIGS]
    if unknown:
        raise ValueError(f"Unknown sensors in SILVER_SENSORS: {unknown}")
else:
    selected_sensors = list(SENSOR_CONFIGS.keys())

queries = []

if SILVER_MODE == "valid":
    for sensor_name in selected_sensors:
        create_silver_table_if_not_exists(sensor_name, SENSOR_CONFIGS[sensor_name])

    for sensor_name in selected_sensors:
        print(f"Starting silver valid stream for {sensor_name}")
        queries.append(start_valid_stream(sensor_name, SENSOR_CONFIGS[sensor_name]))

    Path("/tmp/silver_ready").write_text("ready")
    print("Silver valid consumer is ready")
else:
    create_quarantine_table_if_not_exists(SILVER_QUARANTINE_TABLE)
    print(f"Starting unified quarantine stream for sensors: {', '.join(selected_sensors)}")
    queries.append(start_quarantine_stream(selected_sensors))

    Path("/tmp/silver_quarantine_ready").write_text("ready")
    print("Silver quarantine consumer is ready")

spark.streams.awaitAnyTermination()
