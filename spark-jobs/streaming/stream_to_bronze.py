# spark-jobs/streaming/stream_to_bronze.py

from pyspark.sql import SparkSession, functions as F, types as T

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "sales_events"

BRONZE_PATH = "delta-lake/bronze/sales_events"
CHECKPOINT_PATH = "delta-lake/checkpoints/sales_events_bronze"


def get_spark() -> SparkSession:
    """
    Kreira SparkSession sa:
    - Kafka source/sink paketom
    - Delta Lake integracijom (ACID)
    """
    builder = (
        SparkSession.builder.appName("SalesEventsToBronzeDelta")
        # Delta Lake + Kafka JAR-ovi
        .config(
            "spark.jars.packages",
            ",".join(
                [
                    "io.delta:delta-spark_2.12:3.2.0",
                    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
                ]
            ),
        )
        .config(
            "spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension",
        )
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_event_schema() -> T.StructType:
    """
    Schema za JSON event koji šalje producer.
    """
    return T.StructType(
        [
            T.StructField("event_id", T.StringType(), True),
            T.StructField("order_id", T.StringType(), True),
            T.StructField("user_id", T.IntegerType(), True),
            T.StructField("region", T.StringType(), True),
            T.StructField("channel", T.StringType(), True),
            T.StructField("amount", T.DoubleType(), True),
            T.StructField("currency", T.StringType(), True),
            T.StructField("status", T.StringType(), True),
            T.StructField("created_at", T.StringType(), True),
        ]
    )


def main():
    spark = get_spark()
    schema = get_event_schema()

    # 1) Čitanje iz Kafka topica kao streaming DataFrame-a
    raw_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # 2) Parsiranje JSON value -> kolone
    json_df = raw_df.selectExpr("CAST(value AS STRING) as json_value")

    parsed_df = (
        json_df.withColumn("data", F.from_json(F.col("json_value"), schema))
        .select("data.*")
        .withColumn(
            "event_time",
            F.to_timestamp("created_at"),
        )
        .withColumn("ingest_time", F.current_timestamp())
    )

    # 3) Upis u Delta Bronze
    query = (
        parsed_df.writeStream.format("delta")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .outputMode("append")
        .start(BRONZE_PATH)
    )

    print(f"Writing streaming data to Delta Bronze at: {BRONZE_PATH}")
    query.awaitTermination()


if __name__ == "__main__":
    main()
