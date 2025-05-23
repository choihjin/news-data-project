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

# Python 경로에 scripts 디렉토리 추가
import sys
sys.path.append('/opt/airflow/scripts')

# 변환 함수들 import
from preprocess import (
    transform_classify_category,
    transform_extract_keywords,
    transform_to_embedding
)

def get_db_connection():
    return psycopg2.connect(
        host="postgres",
        dbname="news",
        user=os.getenv("DB_USERNAME"),
        password=os.getenv("DB_PASSWORD"),
        port=5432,
        client_encoding='utf8'
    )

def get_es_client():
    es = Elasticsearch("http://es01:9200")

    index_name = "news"
    if not es.indices.exists(index=index_name):
        print(f"[ES] '{index_name}' 인덱스가 없어 새로 생성합니다.")
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
        print(f"[ES] '{index_name}' 인덱스 생성 완료")
    return es

def process_message(message):
    try:
        # JSON 파싱
        data = json.loads(message)
        
        # 카테고리, 키워드, 임베딩 변환
        category = transform_classify_category(data['content'])
        keywords = transform_extract_keywords(data['content'])
        embedding = transform_to_embedding(data['content'])
        
        # DB 연결
        conn = get_db_connection()
        with conn.cursor() as cur:
            # URL로 중복 체크
            cur.execute("SELECT id FROM news_article WHERE url = %s", (data['url'],))
            if cur.fetchone() is not None:
                print(f"이미 존재하는 기사: {data['url']}")
                return message
            
            # 새 기사 저장
            cur.execute("""
                INSERT INTO news_article (
                    title, writer, write_date, category,
                    content, url, keywords, embedding, updated_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (
                data['title'],
                data['writer'],
                data['write_date'],
                category,
                data['content'],
                data['url'],
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
                    "title": data['title'],
                    "writer": data['writer'],
                    "write_date": data['write_date'],
                    "category": category,
                    "content": data['content'],
                    "url": data['url'],
                    "keywords": keywords
                }
                es.index(index="news", id=inserted_id, document=doc)
                print(f"[ES] 저장 완료: {data['title']}")
            
            print(f"[DB] 저장 완료: {data['title']}")
        
        conn.close()
        return message
    except Exception as e:
        print(f"메시지 처리 실패: {e}")
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