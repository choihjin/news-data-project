from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
from elasticsearch import Elasticsearch
import os

# DAG 기본 설정
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 5, 16),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def sync_postgres_to_es():
    # PostgreSQL 연결
    pg_conn = psycopg2.connect(
        host = 'postgres',
        dbname = 'news',
        user = os.getenv('POSTGRES_USER'),
        password = os.getenv('POSTGRES_PASSWORD'),
        port = os.getenv('POSTGRES_PORT')
    )
    pg_cursor = pg_conn.cursor()

    # Elasticsearch 연결
    es = Elasticsearch("http://es01:9200")
    index_name = "news"

    # 최근 10분 내 갱신된 데이터만 가져오기
    pg_cursor.execute("""
        SELECT id, title, writer, write_date, category, content, url, keywords
        FROM news_article
        WHERE updated_at > now() - interval '10 minutes'
    """)
    rows = pg_cursor.fetchall()
    columns = [desc[0] for desc in pg_cursor.description]

    for row in rows:
        article = dict(zip(columns, row))
        es_doc = {
            "title": article["title"],
            "content": article["content"],
            "writer": article["writer"],
            "category": article["category"],
            "keywords": article["keywords"],
            "write_date": article["write_date"].strftime("%Y-%m-%dT%H:%M:%S")
        }
        es.update(
            index=index_name,
            id=article["url"],
            body={
                "doc": es_doc,
                "doc_as_upsert": True
            }
        )

    pg_cursor.close()
    pg_conn.close()

with DAG(
    dag_id='sync_postgres_to_es',
    default_args=default_args,
    schedule_interval='*/10 * * * *',
    catchup=False,
    tags=['sync', 'postgres', 'elasticsearch']
) as dag:

    sync_task = PythonOperator(
        task_id='sync_pg_to_es',
        python_callable=sync_postgres_to_es
    )

    sync_task
