#!/bin/bash
set -e

mkdir -p /data

python3 << 'PYTHON_SCRIPT'
import csv
import random
from datetime import datetime, timedelta

output_file = "/data/cdr_data.csv"
total_records = 2_000_000

# Requirement 2
skew_caller = "WHOLE_CALLER_999"
skew_records = 200_000

# Requirement 5 support
special_callers_count = 100
normal_calls_per_special = 20
anomaly_calls_per_special = 2
special_records = special_callers_count * (normal_calls_per_special + anomaly_calls_per_special)

normal_records = total_records - skew_records - special_records

call_types = ["VOICE", "SMS", "DATA"]
start_date = datetime(2025, 1, 1)

random.seed(42)

def random_timestamp():
    delta_days = random.randint(0, 364)
    delta_seconds = random.randint(0, 86399)
    ts = start_date + timedelta(days=delta_days, seconds=delta_seconds)
    return ts.strftime("%Y-%m-%dT%H:%M:%SZ")

def random_receiver():
    return f"RECV_{random.randint(100000, 999999)}"

def random_caller():
    return f"CALLER_{random.randint(100000, 999999)}"

def random_tower():
    return f"TOWER_{random.randint(1, 500)}"

def random_duration():
    return random.randint(1, 3600)

def random_call_type():
    return random.choice(call_types)

def calc_charge(call_type, duration_sec):
    if call_type == "VOICE":
        return round(duration_sec * 0.02, 2)
    elif call_type == "SMS":
        return 0.50
    else:
        return round(duration_sec * 0.01, 2)

with open(output_file, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow([
        "caller_id",
        "receiver_id",
        "duration_sec",
        "tower_id",
        "timestamp",
        "call_type",
        "charge_amount"
    ])

    written = 0

    # 1) Skewed records for requirement 2
    for _ in range(skew_records):
        duration = random_duration()
        ctype = random_call_type()
        writer.writerow([
            skew_caller,
            random_receiver(),
            duration,
            random_tower(),
            random_timestamp(),
            ctype,
            f"{calc_charge(ctype, duration):.2f}"
        ])
        written += 1

    # 2) Special callers with guaranteed anomaly-style pattern
    for i in range(1, special_callers_count + 1):
        caller_id = f"ANOM_CALLER_{i:03d}"

        # Normal cluster of calls: tightly grouped durations
        base_duration = random.randint(180, 300)

        for _ in range(normal_calls_per_special):
            duration = max(1, base_duration + random.randint(-10, 10))
            ctype = "VOICE"
            writer.writerow([
                caller_id,
                random_receiver(),
                duration,
                random_tower(),
                random_timestamp(),
                ctype,
                f"{calc_charge(ctype, duration):.2f}"
            ])
            written += 1

        # Extreme durations to trigger anomaly detection
        extreme_durations = [5000, 5200]
        for duration in extreme_durations[:anomaly_calls_per_special]:
            ctype = "VOICE"
            writer.writerow([
                caller_id,
                random_receiver(),
                duration,
                random_tower(),
                random_timestamp(),
                ctype,
                f"{calc_charge(ctype, duration):.2f}"
            ])
            written += 1

    # 3) Remaining normal records
    for _ in range(normal_records):
        duration = random_duration()
        ctype = random_call_type()
        writer.writerow([
            random_caller(),
            random_receiver(),
            duration,
            random_tower(),
            random_timestamp(),
            ctype,
            f"{calc_charge(ctype, duration):.2f}"
        ])
        written += 1

print(f"CDR data generated successfully at {output_file}")
print(f"Total records written: {written}")
print(f"Skew caller: {skew_caller}")
print(f"Skew records: {skew_records}")
print(f"Special anomaly-support callers: {special_callers_count}")
print(f"Special records written: {special_records}")
PYTHON_SCRIPT