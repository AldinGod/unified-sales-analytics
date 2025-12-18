# spark-jobs/batch/silver_to_gold.py

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from delta import configure_spark_with_delta_pip

SILVER_PATH = "delta-lake/silver/sales_events_clean"
GOLD_PATH = "delta-lake/gold/sales_daily_metrics"


def get_spark():
    builder = (
        SparkSession.builder.appName("SilverToGold")
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


def build_gold_metrics(df):
    cols = df.columns
    group_cols = []

    if "event_date" in cols:
        group_cols.append("event_date")
    if "country" in cols:
        group_cols.append("country")

    if not group_cols:
        group_cols = []

    agg_exprs = []

    if "quantity" in cols:
        agg_exprs.append(F.sum("quantity").alias("total_quantity"))

    revenue_expr = None
    if "total_amount" in cols:
        revenue_expr = F.sum("total_amount")
    elif "amount" in cols:
        revenue_expr = F.sum("amount")
    elif "price" in cols and "quantity" in cols:
        revenue_expr = F.sum(F.col("price") * F.col("quantity"))

    if revenue_expr is not None:
        agg_exprs.append(revenue_expr.alias("gross_revenue"))

    if "order_id" in cols:
        agg_exprs.append(F.countDistinct("order_id").alias("order_count"))
    elif "event_id" in cols:
        agg_exprs.append(F.countDistinct("event_id").alias("event_count"))
    else:
        agg_exprs.append(F.count(F.lit(1)).alias("row_count"))

    if not agg_exprs:
        agg_exprs.append(F.count(F.lit(1)).alias("row_count"))

    if group_cols:
        gold_df = df.groupBy(*group_cols).agg(*agg_exprs)
    else:
        gold_df = df.agg(*agg_exprs)

    if "gross_revenue" in gold_df.columns and ("order_count" in gold_df.columns or "event_count" in gold_df.columns):
        count_col = "order_count" if "order_count" in gold_df.columns else "event_count"
        gold_df = gold_df.withColumn(
            "avg_order_value",
            F.col("gross_revenue") / F.col(count_col)
        )

    return gold_df


def main():
    spark = get_spark()

    print(f"📥 Reading Silver Delta from: {SILVER_PATH}")
    df = spark.read.format("delta").load(SILVER_PATH)

    print("📊 Building Gold daily metrics (Silver -> Gold)...")
    gold_df = build_gold_metrics(df)

    print(f"✅ Writing Gold Delta to: {GOLD_PATH}")
    (
        gold_df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .save(GOLD_PATH)
    )

    print("✅ Silver -> Gold completed.")
    print(f"   Records in Gold: {gold_df.count()}")

    print("🔎 Sample rows:")
    for row in gold_df.limit(10).collect():
        print(row)

    spark.stop()


if __name__ == "__main__":
    main()

