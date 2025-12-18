# spark-jobs/batch/bronze_to_silver.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip


BRONZE_PATH = "delta-lake/bronze/sales_events"
SILVER_PATH = "delta-lake/silver/sales_events_clean"


def get_spark():
    builder = (
        SparkSession.builder.appName("BronzeToSilver")
        .config(
            "spark.jars.packages",
            "io.delta:delta-spark_2.12:3.2.0",
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

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def transform_bronze_to_silver(spark: SparkSession):
    print(f"📥 Reading Bronze Delta from: {BRONZE_PATH}")
    bronze_df = spark.read.format("delta").load(BRONZE_PATH)

    print("🔧 Cleaning & standardizing data (Bronze -> Silver)...")

    # Primjer biznis-shemе – prilagodi po potrebi onome što stvarno imaš u Bronze
    df = bronze_df

    # Minimalni quality:
    # - ukloni duplikate po event_id (ako postoji)
    # - ukloni negative ili nula vrijednosti za quantity / price
    if "event_id" in df.columns:
        df = df.dropDuplicates(["event_id"])

    if "price" in df.columns:
        df = df.filter(F.col("price").isNotNull() & (F.col("price") > 0))

    if "quantity" in df.columns:
        df = df.filter(F.col("quantity").isNotNull() & (F.col("quantity") > 0))

    # Normalizacija zemlje ↘
    if "country" in df.columns:
        df = df.withColumn("country", F.upper(F.col("country")))

    # Dodaj pomoćnu kolonu event_date za kasnije agregacije u Gold sloju
    if "event_time" in df.columns:
        df = df.withColumn("event_date", F.to_date("event_time"))

    print(f"✅ Writing cleaned Silver Delta to: {SILVER_PATH}")

    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(SILVER_PATH)
    )

    print("✅ Bronze -> Silver completed.")
    print(f"   Records in Silver: {df.count()}")


def main():
    spark = get_spark()
    try:
        transform_bronze_to_silver(spark)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
