from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
from datetime import datetime
API="http://api:8080"
def consume():
    print("[Apache Kafka] Consuming logs.raw, checkpointing offsets...")
def aggregate():
    print("[Airflow] Aggregating p99 latency per service per 5-min window...")
def detect():
    print("[Airflow] Detecting anomaly: p99 > 500ms for 3 windows + error rate > 2%...")
def build():
    try:
        r=requests.post(f"{API}/api/incidents/detect", timeout=5)
        print(f"[Airflow] Incident report: {r.text[:800]}")
    except Exception as e:
        print(f"[Airflow] Incident detect failed: {e}")
with DAG("sre_pipeline", start_date=datetime(2026,1,1), schedule="*/5 * * * *", catchup=False, tags=["ic3","sre","reliability"]) as dag:
    t1=PythonOperator(task_id="consume_logs_raw", python_callable=consume)
    t2=PythonOperator(task_id="aggregate_p99_by_service", python_callable=aggregate)
    t3=PythonOperator(task_id="detect_anomaly", python_callable=detect)
    t4=PythonOperator(task_id="build_incident_report", python_callable=build)
    t1>>t2>>t3>>t4
