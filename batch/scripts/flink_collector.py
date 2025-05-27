import os
import json
import psycopg2
from psycopg2.extras import Json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
from pyflink.common.typeinfo import Types
from elasticsearch import Elasticsearch
from datetime import datetime
import logging
import sys
from hdfs import InsecureClient
import io

# Python 경로에 scripts 디렉토리 추가
sys.path.append('/opt/airflow/scripts')

# 변환 함수들 import
from preprocess import (
    transform_classify_category,
    transform_extract_keywords,
    transform_to_embedding
)
from news_model import NewsArticle

# 로그 출력을 위한 인코딩 설정
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')

# 로거 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'
)
logger = logging.getLogger(__name__)

# 로그 핸들러 추가
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# 환경 변수 설정
import os
os.environ['PYTHONIOENCODING'] = 'utf-8'

def get_db_connection():
    try:
        conn = psycopg2.connect(
            host="postgres",
            dbname="news",
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PASSWORD"),
            port=5432,
            client_encoding='utf8'
        )
        return conn
    except Exception as e:
        logger.error(f"[DB] 연결 실패: {str(e)}")
        raise

def get_es_client():
    try:
        es = Elasticsearch("http://es01:9200")
        index_name = "news"
        
        if not es.indices.exists(index=index_name):
            es.indices.create(
                index=index_name,
                body={
                    "settings": {
                        "analysis": {
                            "analyzer": {
                                "korean_analyzer": {
                                    "type": "custom",
                                    "tokenizer": "nori_tokenizer"
                                },
                                "ngram_analyzer": {
                                    "type": "custom",
                                    "tokenizer": "ngram_tokenizer",
                                    "filter": ["lowercase"]
                                }
                            },
                            "tokenizer": {
                                "ngram_tokenizer": {
                                    "type": "ngram",
                                    "min_gram": 2,
                                    "max_gram": 3,
                                    "token_chars": ["letter", "digit"]
                                }
                            }
                        }
                    },
                    "mappings": {
                        "properties": {
                            "title": {
                                "type": "text",
                                "analyzer": "korean_analyzer"
                            },
                            "content": {
                                "type": "text",
                                "analyzer": "korean_analyzer"
                            },
                            "writer": {
                                "type": "text",
                                "analyzer": "korean_analyzer"
                            },
                            "url": {
                                "type": "keyword"
                            },
                            "category": {
                                "type": "keyword"
                            },
                            "keywords": {
                                "type": "text",
                                "fields": {
                                    "keyword": {
                                        "type": "keyword"
                                    },
                                    "ngram": {
                                        "type": "text",
                                        "analyzer": "ngram_analyzer"
                                    }
                                }
                            },
                            "write_date": {
                                "type": "date"
                            }
                        }
                    }
                }
            )
        return es
    except Exception as e:
        logger.error(f"[ES] 클라이언트 생성 실패: {str(e)}")
        raise

def get_hdfs_client():
    try:
        return InsecureClient('http://namenode:9870', user='root')
    except Exception as e:
        logger.error(f"[HDFS] 클라이언트 생성 실패: {str(e)}")
        raise

def save_to_hdfs(article_data, write_date):
    try:
        hdfs_client = get_hdfs_client()
        hdfs_path = f"/realtime/news_{write_date}.json"
        
        # 디렉토리 존재 여부 확인 및 생성
        if not hdfs_client.status('/realtime', strict=False):
            hdfs_client.makedirs('/realtime')
        
        # 파일이 존재하는지 확인
        file_exists = hdfs_client.status(hdfs_path, strict=False)
        
        # JSON 문자열로 변환
        json_str = json.dumps(article_data, ensure_ascii=False) + "\n"
        
        # 파일이 존재하면 append 모드로, 없으면 새로 생성
        with hdfs_client.write(hdfs_path, append=file_exists) as writer:
            writer.write(json_str.encode('utf-8'))
            
    except Exception as e:
        logger.error(f"[HDFS] 저장 실패: {str(e)}")
        raise

def process_message(message):
    try:
        # JSON 파싱 및 Pydantic 모델로 변환
        data = json.loads(message)
        article = NewsArticle(**data)
        
        # 카테고리, 키워드, 임베딩 변환
        category = transform_classify_category(article.content)
        keywords = transform_extract_keywords(article.content)
        embedding = transform_to_embedding(article.content)
        
        # DB 연결
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # URL로 중복 체크
                cur.execute("SELECT id FROM news_article WHERE url = %s", (article.url,))
                if cur.fetchone() is not None:
                    return message
                
                # 새 기사 저장
                cur.execute("""
                    INSERT INTO news_article (
                        title, writer, write_date, category,
                        content, url, keywords, embedding, updated_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                
                # Elasticsearch에 저장
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
                
                # HDFS에 저장
                try:
                    record = {
                        "title": article.title,
                        "write_date": str(article.write_date),
                        "category": category,
                        "content": article.content,
                        "keywords": keywords,
                        "url": article.url
                    }
                    write_date_str = article.write_date.date().isoformat()
                    save_to_hdfs(record, write_date_str)
                except Exception as hdfs_error:
                    logger.error(f"[HDFS] 저장 실패: {article.url} - {str(hdfs_error)}")
                
        finally:
            conn.close()
        
        return message
    except Exception as e:
        logger.error(f"[SYSTEM] 메시지 처리 실패: {str(e)}")
        return None

def main():
    # Flink 실행 환경 설정
    env = StreamExecutionEnvironment.get_execution_environment()

    env.set_parallelism(2)
    
    # Kafka Consumer 설정
    kafka_props = {
        'bootstrap.servers': 'kafka:9092',
        'group.id': 'flink_consumer_group'
    }
    
    consumer = FlinkKafkaConsumer(
        topics='news',
        deserialization_schema=SimpleStringSchema(),
        properties=kafka_props
    )
    
    # Kafka에서 메시지 수신 및 처리
    stream = env.add_source(consumer)
    stream.map(process_message, output_type=Types.STRING())
    
    # Flink Job 실행
    env.execute("Flink Kafka Consumer Job")

if __name__ == "__main__":
    main()