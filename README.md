# Hadoop + Spark + Airflow Data Pipeline

## 📌 Overview

This project implements an end-to-end **data pipeline** using **Hadoop (HDFS)**, **Apache Spark**, and **Apache Airflow**, fully orchestrated with **Docker Compose**.

The system processes large-scale Call Detail Record (CDR) data and supports multiple analytical queries triggered through a unified script.

---

## 🏗️ Architecture

* **Hadoop (HDFS)** → Distributed storage (NameNode + DataNode)
* **Apache Spark** → Data processing engine
* **Apache Airflow** → Workflow orchestration (DAGs)
* **Docker Compose** → Container orchestration
* **Data Generator** → Generates synthetic CDR dataset

---

## ⚙️ Services

The system includes the following services:

* `namenode` → HDFS NameNode
* `datanode` → HDFS DataNode
* `spark-master` → Spark Master
* `spark-worker` → Spark Worker
* `airflow` → Airflow Webserver
* `airflow-scheduler` → Airflow Scheduler
* `airflow-db` → PostgreSQL (Airflow metadata DB)
* `data-generator` → Generates input dataset

---

## 📂 Project Structure

```
Hadoop-based-pipeline/
│
├── docker-compose.yml
├── Dockerfile.airflow
├── .env.example
├── README.md
├── run_pipeline.sh
│
├── dags/
│   ├── top_callers_dag.py
│   ├── tower_heatmap_dag.py
│   ├── anomalous_calls_dag.py
│   ├── revenue_recon_dag.py
│
├── jobs/
│   ├── top_callers.py
│   ├── tower_heatmap.py
│   ├── anomalous_calls.py
│   ├── revenue_recon.py
│
├── data/
│   └── generate_records.sh
│
└── output/
```

---

## 🚀 Setup Instructions

Start all services using:

```bash
docker-compose up --build -d
```

Verify all containers are running:

```bash
docker-compose ps
```

---

## ▶️ Running the Pipeline

Trigger jobs using:

```bash
bash run_pipeline.sh top_callers
bash run_pipeline.sh tower_heatmap
bash run_pipeline.sh anomalous_calls
bash run_pipeline.sh revenue_recon
```

---

## 📊 Supported Queries

| Query Name      | Description                  |
| --------------- | ---------------------------- |
| top_callers     | Top users by spending        |
| tower_heatmap   | Tower utilization analysis   |
| anomalous_calls | Detect unusual call patterns |
| revenue_recon   | Revenue reconciliation       |

---

## 📁 Output

All outputs are stored in:

```
/output/<job_name>/<run_id>/
```

Each run generates:

* Processed result files
* `_MANIFEST.json` file

---

## 📄 Manifest File

Each job produces a `_MANIFEST.json` file summarizing execution:

```json
{
  "job_name": "string",
  "run_id": "string",
  "execution_timestamp_utc": "ISO 8601",
  "input_path": "string",
  "output_path": "string",
  "input_record_count": "integer",
  "output_record_count": "integer",
  "status": "SUCCESS"
}
```

---

## ✅ Verification Steps

1.Run any pipeline using:
bash run_pipeline.sh top_callers

2.List output directory:
docker exec -it spark-master sh -c "ls /output/<job_name>/<run_id>"

3.View manifest file:
docker exec -it spark-master sh -c "cat /output/<job_name>/<run_id>/_MANIFEST.json"
---

## 🔐 Notes

* No secrets or credentials are included in this repository
* `.env.example` is provided for configuration reference
* The system is fully containerized and requires no manual setup

---

## 🎯 Conclusion

This project successfully demonstrates:

* Distributed data storage using Hadoop
* Scalable data processing with Spark
* Workflow orchestration using Airflow
* Fully automated pipeline execution using Docker Compose

---
"# Batch-Analytics" 
