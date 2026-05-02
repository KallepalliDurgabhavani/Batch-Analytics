from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="top_callers_by_spend_dag",
    start_date=datetime(2025, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:

    run_top_callers = BashOperator(
        task_id="run_top_callers_job",
        bash_command="""
        RUN_ID='{{ dag_run.conf.get("run_id", ts_nodash) }}'
        OUTPUT_DIR="/output/top_callers_by_spend/${RUN_ID}"
        INPUT_PATH="/data/cdr_data.csv"

        docker exec spark-master sh -c "
          mkdir -p ${OUTPUT_DIR} &&
          /opt/spark/bin/spark-submit /jobs/top_callers.py ${INPUT_PATH} ${OUTPUT_DIR} &&
          INPUT_COUNT=\$(tail -n +2 ${INPUT_PATH} | wc -l) &&
          OUTPUT_COUNT=\$(find ${OUTPUT_DIR} -type f -name 'part-*' -exec cat {} + | wc -l) &&
          cat > ${OUTPUT_DIR}/_MANIFEST.json <<EOF
{
  \\"job_name\\": \\"top_callers_by_spend\\",
  \\"run_id\\": \\"${RUN_ID}\\",
  \\"execution_timestamp_utc\\": \\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\\",
  \\"input_path\\": \\"${INPUT_PATH}\\",
  \\"output_path\\": \\"${OUTPUT_DIR}\\",
  \\"input_record_count\\": \${INPUT_COUNT},
  \\"output_record_count\\": \${OUTPUT_COUNT},
  \\"status\\": \\"SUCCESS\\"
}
EOF
        "
        """
    )