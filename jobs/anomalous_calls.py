from pyspark.sql import SparkSession
import sys
import math
import shutil
import os


def parse_line(line: str):
    parts = line.strip().split(",")
    if len(parts) != 7:
        return None

    # Skip header
    if parts[0] == "caller_id":
        return None

    try:
        caller_id = parts[0]
        timestamp = parts[4]
        duration_sec = int(parts[2])
        return (caller_id, (timestamp, duration_sec))
    except Exception:
        return None


def main():
    if len(sys.argv) != 3:
        print("Usage: anomalous_calls.py <input_csv> <output_dir>")
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    spark = SparkSession.builder \
        .appName("anomalous_call_detection") \
        .getOrCreate()

    sc = spark.sparkContext

    # Output path already unte remove cheyyali
    if output_path.startswith("/"):
        try:
            shutil.rmtree(output_path, ignore_errors=True)
        except Exception:
            pass

    # Read CSV as text
    lines = sc.textFile(input_path)

    # (caller_id, (timestamp, duration_sec))
    records = lines.map(parse_line).filter(lambda x: x is not None)

    # ---- Custom partitioner style logic ----
    # Same caller_id records same partition lo undela partitionBy use chestunnam
    num_partitions = 8
    partitioned_records = records.partitionBy(num_partitions, lambda key: hash(key))

    # Stats per caller:
    # value = (count, sum, sumsq)
    stats = partitioned_records.mapValues(
        lambda x: (1, float(x[1]), float(x[1]) * float(x[1]))
    ).reduceByKey(
        lambda a, b: (
            a[0] + b[0],
            a[1] + b[1],
            a[2] + b[2]
        )
    )

    def compute_stats(v):
        count, total, sumsq = v
        mean = total / count
        variance = (sumsq / count) - (mean * mean)
        variance = max(variance, 0.0)
        stddev = math.sqrt(variance)
        return (mean, stddev)

    # (caller_id, (mean, stddev))
    caller_stats = stats.mapValues(compute_stats).partitionBy(
        num_partitions, lambda key: hash(key)
    )

    # Join original partitioned records with partitioned stats
    joined = partitioned_records.join(caller_stats)
    # joined:
    # (caller_id, ((timestamp, duration_sec), (mean, stddev)))

    def is_anomalous(row):
        caller_id, ((timestamp, duration_sec), (mean, stddev)) = row
        return abs(duration_sec - mean) > (3 * stddev)

    anomalies = joined.filter(is_anomalous)

    # Output format:
    # caller_id,call_timestamp,duration_sec,user_mean_duration,user_stddev
    output = anomalies.map(
        lambda row: "{},{},{},{:.2f},{:.2f}".format(
            row[0],
            row[1][0][0],
            row[1][0][1],
            row[1][1][0],
            row[1][1][1]
        )
    )

    output.saveAsTextFile(output_path)

    spark.stop()


if __name__ == "__main__":
    main()