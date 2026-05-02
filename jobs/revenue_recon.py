from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum
import sys

if len(sys.argv) != 3:
    print("Usage: revenue_recon.py <input_csv> <output_path>")
    sys.exit(1)

input_path = sys.argv[1]
output_path = sys.argv[2]

spark = SparkSession.builder \
    .appName("revenue_reconciliation") \
    .getOrCreate()

df = spark.read.csv(input_path, header=True, inferSchema=True)

result_df = df.select(col("charge_amount").cast("double").alias("charge_amount")) \
              .agg(spark_sum("charge_amount").alias("total_revenue"))

result_df.coalesce(1).write.mode("overwrite").csv(output_path, header=False)

spark.stop()