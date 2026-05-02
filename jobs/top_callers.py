import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, round as _round


def main():
    if len(sys.argv) != 3:
        print("Usage: top_callers.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = (
        SparkSession.builder
        .appName("top_callers_by_spend")
        .getOrCreate()
    )

    # Read input CSV
    df = (
        spark.read
        .option("header", "true")
        .csv(input_path)
    )

    # Cast charge_amount to double
    df = df.withColumn("charge_amount", col("charge_amount").cast("double"))

    # Aggregate total spend per caller
    result_df = (
        df.groupBy("caller_id")
        .agg(_round(_sum("charge_amount"), 2).alias("total_spend"))
        .orderBy(col("total_spend").desc())
        .limit(100)
    )

    # Write output
    (
        result_df.coalesce(1)
        .write
        .mode("overwrite")
        .option("header", "false")
        .csv(output_path)
    )

    spark.stop()


if __name__ == "__main__":
    main()