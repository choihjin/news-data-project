import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.email import EmailOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from pyspark.sql import SparkSession

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['jjin6573@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
}

with DAG(
    dag_id='daily_report_dag',
    default_args=default_args,
    description='매일 아침 9시에 Spark를 이용해 뉴스 리포트 생성',
    schedule_interval='0 9 * * *',
    start_date=datetime(2025, 5, 1, tzinfo=local_tz),
    catchup=False,
    tags=['daily', 'report', 'spark']
) as dag:
    start = DummyOperator(task_id='start')

    submit_spark_job = SparkSubmitOperator(
        task_id='spark_daily_report',
        application='/opt/airflow/scripts/spark_daily_report.py',
        conn_id='spark_default',
        application_args=['--date', '{{ ds }}'],
        conf={
            'spark.master': 'spark://spark-master:7077',
            'spark.executor.memory': '2g',
            'spark.driver.memory': '2g',
            'spark.sql.shuffle.partitions': '200',
            'spark.default.parallelism': '200',
            'spark.hadoop.fs.defaultFS': 'hdfs://namenode:8020'
        }
    )

    notify_report_generated = BashOperator(
        task_id='notify_report_generated',
        bash_command=(
            'echo "리포트가 생성되었습니다: {{ ds }} 날짜의 이메일 보내기 "'
        )
    )

    send_email = EmailOperator(
        task_id='send_report_email',
        to='jjin6573@gmail.com',
        subject='[뉴스 리포트] {{ ds }} 키워드 분석 결과',
        html_content="""<p>안녕하세요,</p>
                        <p>{{ ds }}자 뉴스 리포트를 첨부파일로 전달드립니다.</p>
                        <p>감사합니다.</p>""",
        files=["data/daily_report_{{ ds }}.pdf"],
        mime_charset='utf-8'
    )

    end = DummyOperator(task_id='end')

    start >> submit_spark_job >> notify_report_generated >> send_email >> end