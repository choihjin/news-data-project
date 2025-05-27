echo "[WAIT] Flink JobManager 준비 중..."
sleep 10

echo "[RUN] Flink Python Job 제출 중..."
flink run -py /opt/airflow/scripts/flink_collector.py

echo "[DONE] Flink Job 제출 완료!"
exit 0 