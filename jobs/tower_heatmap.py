import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, to_timestamp, count


def main():
    if len(sys.argv) != 3:
        print("Usage: tower_heatmap.py <input_path> <output_path>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = (
        SparkSession.builder
        .appName("tower_utilization_heatmap")
        .getOrCreate()
    )

    # Read CSV
    df = (
        spark.read
        .option("header", "true")
        .csv(input_path)
    )

    # Convert timestamp string to timestamp type
    df = df.withColumn(
        "parsed_timestamp",
        to_timestamp(col("timestamp"), "yyyy-MM-dd'T'HH:mm:ss'Z'")
    )

    # Extract hour and aggregate call counts
    result_df = (
        df.withColumn("hour_of_day", hour(col("parsed_timestamp")))
        .groupBy("tower_id", "hour_of_day")
        .agg(count("*").alias("call_count"))
        .select("tower_id", "hour_of_day", "call_count")
        .orderBy(col("tower_id").asc(), col("hour_of_day").asc())
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