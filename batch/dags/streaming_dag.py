import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator

local_tz = pendulum.timezone("Asia/Seoul")

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def run_producer_aitimes():
    import sys
    sys.path.append('/opt/airflow/scripts')
    from producer_aitimes import main
    main()

def run_producer_hankyung():
    import sys
    sys.path.append('/opt/airflow/scripts')
    from producer_hankyung import main
    main()

with DAG(
    dag_id='news_collect_streaming',
    default_args=default_args,
    description='Flink를 이용해 뉴스 데이터를 수집하고 처리',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 5, 1, tzinfo=local_tz),
    catchup=False,
    tags=['flink', 'kafka'],
) as dag:
    
    start = DummyOperator(
        task_id='start'
    )
    
    producer_aitimes = PythonOperator(
        task_id='RSS_aitimes',
        python_callable=run_producer_aitimes,
        pool='default_pool',  # 풀 지정
        execution_timeout=timedelta(minutes=30)  # 실행 시간 제한
    )

    producer_hankyung = PythonOperator(
        task_id='RSS_hankyung',
        python_callable=run_producer_hankyung,
        pool='default_pool',
        execution_timeout=timedelta(minutes=30)
    )

    # consumer = BashOperator(
    #     task_id='flink_consumer',
    #     bash_command='docker exec jobmanager flink run -py /opt/airflow/scripts/consumer.py --detached'
    # )

    end = DummyOperator(
        task_id='end'
    )

    start >> [producer_aitimes, producer_hankyung] >> end