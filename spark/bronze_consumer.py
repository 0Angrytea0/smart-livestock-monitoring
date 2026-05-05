from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp, to_timestamp, when, lit, to_date
from pyspark.sql.types import *

KAFKA_BOOTSTRAP_SERVERS = "kafka:9092"
CHECKPOINT_BASE_PATH = "/opt/spark/checkpoints/bronze"

Path(CHECKPOINT_BASE_PATH).mkdir(parents=True, exist_ok=True)

spark = (
    SparkSession.builder
        .appName("BronzeIngestion")
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
        .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

TOPIC_SCHEMAS = {
    "mmcows.cbt": StructType([
        StructField("time", StringType()),
        StructField("type", StringType()),
        StructField("id", StringType()),
        StructField("sensor", StringType()),
        StructField("metrics", StructType([
            StructField("temperature", DoubleType())
        ]))
    ]),

    "mmcows.ankle": StructType([
        StructField("time", StringType()),
        StructField("type", StringType()),
        StructField("id", StringType()),
        StructField("sensor", StringType()),
        StructField("metrics", StructType([
            StructField("lying", IntegerType())
        ]))
    ]),

    "mmcows.immu": StructType([
        StructField("time", StringType()),
        StructField("type", StringType()),
        StructField("id", StringType()),
        StructField("sensor", StringType()),
        StructField("metrics", StructType([
            StructField("accel_x_mps2", DoubleType()),
            StructField("accel_y_mps2", DoubleType()),
            StructField("accel_z_mps2", DoubleType()),
            StructField("mag_x_uT", DoubleType()),
            StructField("mag_y_uT", DoubleType()),
            StructField("mag_z_uT", DoubleType())
        ]))
    ]),

    "mmcows.pressure": StructType([
        StructField("time", StringType()),
        StructField("type", StringType()),
        StructField("id", StringType()),
        StructField("sensor", StringType()),
        StructField("metrics", StructType([
            StructField("pressure_Pa", DoubleType()),
            StructField("elevation_m", DoubleType())
        ]))
    ]),

    "mmcows.uwb": StructType([
        StructField("time", StringType()),
        StructField("type", StringType()),
        StructField("id", StringType()),
        StructField("sensor", StringType()),
        StructField("metrics", StructType([
            StructField("coord_x_cm", DoubleType()),
            StructField("coord_y_cm", DoubleType()),
            StructField("coord_z_cm", DoubleType())
        ]))
    ]),

    "mmcows.milk": StructType([
        StructField("time", StringType()),
        StructField("type", StringType()),
        StructField("id", StringType()),
        StructField("sensor", StringType()),
        StructField("metrics", StructType([
            StructField("milk_weight_kg", DoubleType()),
            StructField("DIM", DoubleType())
        ]))
    ]),

    "mmcows.thi": StructType([
        StructField("time", StringType()),
        StructField("type", StringType()),
        StructField("id", StringType()),
        StructField("sensor", StringType()),
        StructField("metrics", StructType([
            StructField("temperature_C", DoubleType()),
            StructField("humidity_per", DoubleType()),
            StructField("THI", DoubleType())
        ]))
    ])
}

def create_table_if_not_exists(topic_name: str, schema: StructType):
    table_name = f"iceberg.{topic_name.replace('.', '_')}"

    fields_sql = []
    for field in schema.fields:
        if field.name == "metrics":
            metrics_sql_parts = []
            for m in field.dataType.fields:
                spark_type = m.dataType.simpleString()
                metrics_sql_parts.append(f"`{m.name}`: {spark_type}")
            fields_sql.append(f"metrics STRUCT<{', '.join(metrics_sql_parts)}>")
        else:
            spark_type = field.dataType.simpleString()
            fields_sql.append(f"`{field.name}` {spark_type}")

    fields_sql.extend([
        "event_ts timestamp",
        "kafka_date date",
        "raw_json string",
        "parse_status string",
        "kafka_key string",
        "kafka_topic string",
        "kafka_partition int",
        "kafka_offset bigint",
        "kafka_timestamp timestamp",
        "ingest_ts timestamp"
    ])

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            {", ".join(fields_sql)}
        )
        USING iceberg
        PARTITIONED BY (kafka_date)
        """
    )

def start_stream(topic_name: str, schema: StructType):
    table_name = f"iceberg.{topic_name.replace('.', '_')}"
    checkpoint_path = f"{CHECKPOINT_BASE_PATH}/{topic_name.replace('.', '_')}"

    Path(checkpoint_path).mkdir(parents=True, exist_ok=True)

    create_table_if_not_exists(topic_name, schema)

    df_raw = (
        spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
            .option("subscribe", topic_name)
            .option("startingOffsets", "earliest")
            .option("failOnDataLoss", "false")
            .load()
    )

    df_prepared = (
        df_raw
            .select(
                col("topic").alias("kafka_topic"),
                col("partition").alias("kafka_partition"),
                col("offset").alias("kafka_offset"),
                col("timestamp").alias("kafka_timestamp"),
                col("key").cast("string").alias("kafka_key"),
                col("value").cast("string").alias("raw_json")
            )
            .withColumn("data", from_json(col("raw_json"), schema))
            .withColumn(
                "parse_status",
                when(col("raw_json").isNull(), lit("empty_payload"))
                .when(col("data").isNull(), lit("invalid_json_or_schema_mismatch"))
                .otherwise(lit("parsed"))
            )
    )

    df_bronze = (
        df_prepared
            .select(
                col("data.time").alias("time"),
                col("data.type").alias("type"),
                col("data.id").alias("id"),
                col("data.sensor").alias("sensor"),
                col("data.metrics").alias("metrics"),
                to_timestamp(col("data.time")).alias("event_ts"),
                to_date(col("kafka_timestamp")).alias("kafka_date"),
                col("raw_json"),
                col("parse_status"),
                col("kafka_key"),
                col("kafka_topic"),
                col("kafka_partition"),
                col("kafka_offset"),
                col("kafka_timestamp"),
                current_timestamp().alias("ingest_ts")
            )
    )

    return (
        df_bronze.writeStream
            .queryName(f"bronze_{topic_name.replace('.', '_')}")
            .format("iceberg")
            .outputMode("append")
            .option("checkpointLocation", checkpoint_path)
            .toTable(table_name)
    )
queries = []

for topic_name, schema in TOPIC_SCHEMAS.items():
    print(f"Starting bronze stream for {topic_name}")
    q = start_stream(topic_name, schema)
    queries.append(q)

Path("/tmp/bronze_ready").write_text("ready")
print("Bronze consumer is ready")

spark.streams.awaitAnyTermination()