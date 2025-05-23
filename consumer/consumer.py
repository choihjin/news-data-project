import os
import json
import psycopg2
from psycopg2.extras import Json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
from elasticsearch import Elasticsearch
from hdfs import InsecureClient
import gzip

from preprocess import (
    transform_classify_category,
    transform_extract_keywords,
    transform_to_embedding,
)
from news_model import NewsArticle  # Pydantic 모델
from datetime import datetime

# JSON 저장을 위한 설정
OUTPUT_DIR = "/mnt/c/Users/SSAFY/Desktop/data-pjt/batch/data/realtime"
os.makedirs(OUTPUT_DIR, exist_ok=True)
OUTPUT_PATH = os.path.join(OUTPUT_DIR, f"news_{datetime.now().strftime('%Y-%m-%d')}.json")

# HDFS 클라이언트를 함수 내부에서 생성하도록 수정
def get_hdfs_client():
    # 호스트 머신에서 도커의 namenode로 연결 설정
    return InsecureClient('http://localhost:9870', user='root')

# PostgreSQL 연결 함수
def get_db_connection():
    return psycopg2.connect(
        host="localhost",
        dbname="news",
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        port=5432
    )

# Elasticsearch 클라이언트 생성 함수
def get_es_client():
    return Elasticsearch("http://localhost:9200")

# PostgreSQL 저장 함수
def insert_article_to_db(article, category, keywords, embedding):
    try:
        conn = get_db_connection()
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO news_article (
                    title, writer, write_date, category,
                    content, url, keywords, embedding, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (url) DO NOTHING
                RETURNING id;
            """, (
                article.title,
                article.writer,
                article.write_date,
                category,
                article.content,
                article.url,
                Json(keywords, dumps=lambda obj: json.dumps(obj, ensure_ascii=False)),
                embedding,
                datetime.now()
            ))
            inserted_id = cur.fetchone()[0] if cur.rowcount > 0 else None
            conn.commit()
        conn.close()

        if inserted_id:
            es = get_es_client()
            doc = {
                "title": article.title,
                "writer": article.writer,
                "write_date": article.write_date.strftime("%Y-%m-%dT%H:%M:%S"),
                "category": category,
                "content": article.content,
                "url": article.url,
                "keywords": keywords
            }
            es.index(index="news", id=inserted_id, document=doc)
            print(f"[ES] 저장 완료: {article.title}")

        print(f"[DB] 저장 완료: {article.title}")
    except Exception as e:
        print(f"[DB/ES] 저장 실패: {e}")

# HDFS 저장 함수 수정
def save_to_hdfs(article_data, write_date):
    try:
        hdfs_client = get_hdfs_client()
        hdfs_path = f"/realtime/news_{write_date}.json"
        
        # HDFS 연결 테스트
        if not hdfs_client.status('/', strict=False):
            print("[HDFS] 연결 실패: HDFS 서버에 연결할 수 없습니다.")
            return
            
        # 디렉토리 존재 여부 확인 및 생성
        if not hdfs_client.status('/realtime', strict=False):
            hdfs_client.makedirs('/realtime')
            hdfs_client.set_permission('/realtime', '777')
        
        # 파일이 존재하는지 확인
        file_exists = hdfs_client.status(hdfs_path, strict=False)
        
        # JSON 문자열로 변환
        json_str = json.dumps(article_data, ensure_ascii=False) + "\n"
        
        # 파일이 존재하면 append 모드로, 없으면 새로 생성
        with hdfs_client.write(hdfs_path, append=file_exists) as writer:
            writer.write(json_str.encode('utf-8'))
            
        print(f"[HDFS] 저장 완료: {article_data['title']}")
    except Exception as e:
        print(f"[HDFS] 저장 실패: {e}")
        # 실패 시 재시도하지 않고 넘어가기
        return

# Kafka 메시지 처리 함수
def process_article(json_str):
    try:
        data = json.loads(json_str)
        article = NewsArticle(**data)

        category = transform_classify_category(article.content)
        keywords = transform_extract_keywords(article.content)
        embedding = transform_to_embedding(article.content)

        # DB와 ES에 저장
        insert_article_to_db(article, category, keywords, embedding)

        # 저장할 데이터 구성
        record = {
            "title": article.title,
            "write_date": str(article.write_date),
            "category": category,
            "content": article.content,
            "keywords": keywords,
            "url": article.url
        }

        # HDFS에 저장
        write_date_str = article.write_date.date().isoformat()
        save_to_hdfs(record, write_date_str)

        return json_str
    except Exception as e:
        print(f"[JSON] 처리 실패: {e}")
        return None

# Flink 실행 환경 설정
env = StreamExecutionEnvironment.get_execution_environment()

# Kafka connector JAR 등록
kafka_connector_path = os.path.join(os.path.dirname(__file__), "config", "flink-sql-connector-kafka-3.3.0-1.20.jar")
env.add_jars(f"file://{kafka_connector_path}")

# Kafka Consumer 설정
kafka_props = {
    'bootstrap.servers': 'localhost:9092',
    'group.id': 'flink_consumer_group'
}
consumer = FlinkKafkaConsumer(
    topics='news',
    deserialization_schema=SimpleStringSchema(),
    properties=kafka_props
)

# Kafka에서 메시지 수신 및 처리
stream = env.add_source(consumer)
stream.map(process_article, output_type=Types.STRING())

# Flink Job 실행
env.execute("Flink Kafka Consumer Job")