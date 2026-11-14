# IC3 SRE Lab - #5 Log Anomaly / Site Reliability Demo
## Inspired by FAA / NOTAM outage pattern (simplified demo, NOT a recreation)

### Plain English Story
Imagine an airline system like FAA NOTAM:
- Service A: NOTAM database (holds flight notices)
- Service B: Flight planning (reads NOTAMs)
- Service C: Air traffic control display (shows NOTAMs to controllers)

If Service A gets slow (p99 120ms -> 850ms), Service B waits, Service C waits, planes can't get notices.
In real FAA outage, it cascaded for hours because no one caught the first slowdown + failed failover.

This demo: Catch the slowdown in 5 minutes, recommend rollback, before cascade.

### What It Does
1. Blazor -> Simulate Normal Traffic (healthy logs, p99 ~120ms)
2. Blazor -> Inject Incident - Deploy #482 (checkout/notam service spikes to 850ms, error rate 4%)
3. Generate Logs -> Kafka `logs.raw` (1000 log events)
4. Airflow -> `sre_pipeline` DAG: consume -> aggregate p99 per service per 5-min window -> detect anomaly -> build incident report
5. Analyst Agent -> Plain English: "Checkout service p99 jumped 120ms -> 850ms at 14:32, correlates with Deploy #482. Error rate 0.2% -> 4.1%. Recommendation: ROLLBACK Deploy #482, failover to secondary."

### Kafka Topics
- `logs.raw` - {service, latencyMs, statusCode, deployId, timestamp}
- `incidents.detected` - {service, p99Before, p99After, errorRateBefore, errorRateAfter, deployId}

### Airflow DAG
```
consume_logs_raw -> aggregate_p99_by_service -> detect_anomaly -> build_incident_report
```

### Key Metrics Explained
- **p99 latency:** 99% of requests are faster than this. If p99 is 850ms, 1% of users wait >850ms. p99 is what users FEEL, average hides spikes.
- **Error rate:** % of requests that return 5xx. 0.2% normal, 4% = 20x worse.
- **Anomaly rule:** IF p99 > 500ms for 3 windows AND error rate > 2% AND correlates with deploy -> ALERT + ROLLBACK

### Run Order
1. docker compose up --build -d (in THIS folder, not ic3-lab-clean)
2. Blazor http://localhost:5001 -> Simulate Normal -> Generate Logs
3. Airflow http://localhost:8080 -> Trigger sre_pipeline -> green -> No anomaly
4. Blazor -> Inject Incident Deploy #482 -> Generate Logs again
5. Airflow -> Trigger again -> Incident detected
6. Blazor -> Ask Analyst -> ROLLBACK recommendation

### Ports (same as Experiment Lab, run ONE at a time)
Blazor 5001 | API 5000 | Kafka UI 8085 | Airflow 8080 admin/admin
If you want both labs running, change ports in docker-compose.yml for this one to 5002/5003/8086/8081.

### Roadmap
- #5 SRE / Log Anomaly (THIS) - reliability, p99, rollback
- #2 Order Pipeline (NEXT) - orders.created -> payments -> inventory -> shipments
- #1 Fraud Detection (THEN) - transactions.raw, anomaly per user
- #3 IoT Sensors, #4 Ride-Share Surge (later)
