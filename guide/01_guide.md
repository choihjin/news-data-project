# SSAFY 맞춤형 뉴스 데이터 파이프라인 환경 설정 관통 PJT(1) 가이드

## 최종 목표

```
[Extract]                   [Transform]             [Load]
Kafka Topic  →  Flink  →  데이터 처리/변환   →  PostgreSQL(DB 저장)
(JSON or RSS) (스트리밍)  (카테고리 분류)    →  Elasticsearch(검색)
                  │        (키워드 추출)      
                  │        (벡터 임베딩)
                  │
                  ↓            
                HDFS  →  Spark  →  리포트 생성  →  HDFS 아카이브
              (임시저장)  (배치)     (pdf)          (장기 보관)
```

## 목차

1. PostgreSQL 설치 및 설정  
2. 필요한 라이브러리 설치  

---

## 1. PostgreSQL 설치 및 설정

### 1.1. PostgreSQL 설치 (Linux - Ubuntu)

1. **PostgreSQL 설치**  

```bash
sudo apt-get update
sudo apt-get install postgresql postgresql-contrib postgresql-16-vector
```

2. **서비스 상태 확인**  

```bash
sudo service postgresql status
```

### 1.2. PostgreSQL 데이터베이스 설정

1. **PostgreSQL 접속**  

```bash
sudo -i -u postgres
psql
```

2. **데이터베이스 생성**  

```sql
CREATE DATABASE news;
```

3. **사용자 생성 및 권한 부여**  

```sql
CREATE USER ssafyuser WITH PASSWORD 'ssafy';
GRANT ALL PRIVILEGES ON DATABASE news TO ssafyuser;
```

4. **접속 인증 설정 (pg_hba.conf 수정)**

```conf
# 파일 위치: /etc/postgresql/16/main/pg_hba.conf

# 아래와 같이 수정
local   all             all                                     md5
host    all             all             127.0.0.1/32            md5
host    all             all             ::1/128                 md5
```

5. **데이터베이스 접속 및 테이블 생성성**

```bash
\c news
```

```sql
-- pgvector 확장 설치 (최초 1회)
CREATE EXTENSION IF NOT EXISTS vector;

-- news_article 테이블 생성
CREATE TABLE news_article (
    id SERIAL PRIMARY KEY,
    title VARCHAR(200) NOT NULL,
    writer VARCHAR(255) NOT NULL,
    write_date TIMESTAMP NOT NULL,
    category VARCHAR(50) NOT NULL,
    content TEXT NOT NULL,
    url VARCHAR(200) UNIQUE NOT NULL,
    keywords JSON DEFAULT '[]'::json,
    embedding VECTOR(1536) NULL
);
```

6. **권한 부여**

```sql
ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON TABLES TO ssafyuser;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT ALL ON SEQUENCES TO ssafyuser;
GRANT CREATE ON SCHEMA public TO ssafyuser;
```

---

## 2. 필요한 라이브러리 설치

```bash
python3.11 -m venv ~/venvs/data-pjt
source ~/venvs/data-pjt/bin/activate

pip install -r requirements.txt
```

---

## 마무리

위의 단계들을 차례대로 진행하고 RSS의 뉴스를 잘 수집하고 저장하는 것이 목표입니다.  
이를 시작으로 PostgreSQL, Hadoop, Kafka, Airflow, Elasticsearch를 이용한 SSAFY 맞춤형 뉴스 데이터 파이프라인을 지속적으로 개발하며 프로젝트를 완성하게 됩니다.