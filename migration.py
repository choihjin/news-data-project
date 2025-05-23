import psycopg2
from elasticsearch import Elasticsearch
from datetime import datetime

# DB 연결
pg_conn = psycopg2.connect(
    dbname="news",
    user="ssafyuser",
    password="ssafy",
    host="192.168.210.75",
    port="5432"
)
pg_cur = pg_conn.cursor()

# Elasticsearch 연결
es = Elasticsearch("http://localhost:9200")

# PostgreSQL에서 뉴스 기사 가져오기
pg_cur.execute("""
    SELECT id, title, writer, write_date, category, content, url, keywords, embedding
    FROM news_article
""")
rows = pg_cur.fetchall()

for row in rows:
    doc_id = row[0]

    doc = {
        "title": row[1],
        "writer": row[2],
        "write_date": row[3].strftime("%Y-%m-%dT%H:%M:%S"),
        "category": row[4],
        "content": row[5],
        "url": row[6],
        "keywords": row[7] if row[7] else []  # None인 경우 빈 리스트로 처리
    }

    es.index(index="news", id=doc_id, document=doc)

print("✅ 색인 완료")
