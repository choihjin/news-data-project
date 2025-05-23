# DB TEST
import os
import psycopg2
from dotenv import load_dotenv


# .env 파일 로드
load_dotenv()

conn = psycopg2.connect(
    host = "localhost",
    port = 5432,
    dbname = "news",
    user = os.getenv("DB_USERNAME"),
    password = os.getenv("DB_PASSWORD")
)

# 커서 생성
cursor = conn.cursor()
# sql문을 문자열 형태로 인자로 넣음 -> sql 실행 가능
# ; 없어도 됨
query = "select * from news_article"
cursor.execute(query)

# 실행 결과 받아옴
rows = cursor.fetchall()

for r in rows:
    print(r)

# 커서 닫고
cursor.close()
# DB 연결 닫고
conn.close()