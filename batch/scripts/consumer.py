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

# Python 경로에 scripts 디렉토리 추가
sys.path.append('/opt/airflow/scripts')

# 변환 함수들 import
from preprocess import (
    transform_classify_category,
    transform_extract_keywords,
    transform_to_embedding
)
from news_model import NewsArticle

# 로거 설정
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    encoding='utf-8'  # 한글 인코딩 설정
)
logger = logging.getLogger(__name__)

# 로그 핸들러 추가
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(handler)
logger.setLevel(logging.INFO)

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
        logger.info("DB 연결 성공")
        return conn
    except Exception as e:
        logger.error(f"DB 연결 실패: {str(e)}")
        raise

def get_es_client():
    try:
        es = Elasticsearch("http://es01:9200")
        index_name = "news"
        
        if not es.indices.exists(index=index_name):
            logger.info(f"'{index_name}' 인덱스 생성 시작")
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
            logger.info(f"'{index_name}' 인덱스 생성 완료")
        return es
    except Exception as e:
        logger.error(f"ES 클라이언트 생성 실패: {str(e)}")
        raise

def process_message(message):
    try:
        # JSON 파싱 및 Pydantic 모델로 변환
        logger.info("="*50)
        logger.info("새로운 메시지 처리 시작")
        logger.info(f"메시지 내용: {message[:100]}...")
        
        data = json.loads(message)
        url = data.get('url', 'URL 없음')
        logger.info(f"URL: {url}")
        
        article = NewsArticle(**data)
        logger.info(f"Pydantic 모델 변환 성공: {article.url}")
        
        # 카테고리, 키워드, 임베딩 변환
        logger.info("데이터 변환 시작...")
        category = transform_classify_category(article.content)
        keywords = transform_extract_keywords(article.content)
        embedding = transform_to_embedding(article.content)
        logger.info(f"변환 완료 - 카테고리: {category}, 키워드 수: {len(keywords)}")
        
        # DB 연결
        conn = get_db_connection()
        try:
            with conn.cursor() as cur:
                # URL로 중복 체크
                cur.execute("SELECT id FROM news_article WHERE url = %s", (article.url,))
                if cur.fetchone() is not None:
                    logger.info(f"이미 존재하는 기사: {article.url}")
                    return message
                
                # 새 기사 저장
                logger.info(f"DB 저장 시도: {article.url}")
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
                logger.info(f"DB 저장 완료 (ID: {inserted_id}): {article.url}")
                
                # Elasticsearch에 저장
                if inserted_id:
                    try:
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
                        logger.info(f"ES 저장 시도: {article.url}")
                        response = es.index(index="news", id=inserted_id, document=doc)
                        logger.info(f"ES 저장 완료: {article.title}")
                        logger.info(f"ES 응답: {response}")
                    except Exception as es_error:
                        logger.error("="*50)
                        logger.error(f"ES 저장 실패: {article.url}")
                        logger.error(f"에러 타입: {type(es_error)}")
                        logger.error(f"에러 내용: {str(es_error)}")
                        import traceback
                        logger.error(f"스택 트레이스:\n{traceback.format_exc()}")
                        logger.error("="*50)
                
                logger.info(f"처리 완료: {article.title}")
                logger.info("="*50)
        finally:
            conn.close()
            logger.info("DB 연결 종료")
        
        return message
    except Exception as e:
        logger.error("="*50)
        logger.error("메시지 처리 실패")
        logger.error(f"에러 타입: {type(e)}")
        logger.error(f"에러 내용: {str(e)}")
        logger.error(f"메시지 내용: {message[:100]}...")
        import traceback
        logger.error(f"스택 트레이스:\n{traceback.format_exc()}")
        logger.error("="*50)
        return None

def main():
    # Flink 실행 환경 설정
    env = StreamExecutionEnvironment.get_execution_environment()
    
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