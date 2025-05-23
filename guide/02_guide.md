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
sudo apt-get install postgresql-16 postgresql-contrib-16 postgresql-16-pgvector -y  
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

# "local" is for Unix domain socket connections only
local   all             all                                     md5
# IPv4 local connections:
host    all             all             127.0.0.1/32            md5
# IPv6 local connections:
host    all             all             ::1/128                 md5
# Allow replication connections from localhost, by a user with the
# replication privilege.
local   replication     all                                     md5
host    replication     all             127.0.0.1/32            md5
host    replication     all             ::1/128                 md5

```

5. **데이터베이스 접속 및 테이블 생성**

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

(news 데이터베이스 안에서 (news=# 인 상태))
```sql
GRANT ALL PRIVILEGES ON TABLE news_article TO ssafyuser;
GRANT ALL ON TABLE news_article TO ssafyuser;
GRANT ALL ON SEQUENCE news_article_id_seq TO ssafyuser;

```

---

## 2. 필요한 라이브러리 설치

```bash
python3.10 -m venv ~/venvs/data-pjt
source ~/venvs/data-pjt/bin/activate

pip install -r requirements.txt
```


## 3. RSS 토픽과 연결

### 1. 목표

Kafka를 통해 실시간 뉴스 데이터를 수집하기 위해, RSS 피드를 주기적으로 파싱하고 해당 내용을 Kafka 토픽으로 전송하는 Producer를 구현합니다.

- RSS 피드에서 최신 뉴스 항목 수집  
- 뉴스 본문을 크롤링하여 Kafka 토픽으로 전송  
- JSON 형식으로 Kafka에 직렬화하여 전송

---

### 2. Kafka 실행 (Zookeeper 및 Broker 실행)

```bash
# 터미널 1: Zookeeper 실행
$KAFKA_HOME/bin/zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties

# 터미널 2: Kafka Broker 실행
$KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties
```

---

### 3. Kafka Producer 코드 예시

```python
from kafka import KafkaProducer
import json

# Kafka 브로커 주소
KAFKA_BROKER = "localhost:9092"
# Kafka 토픽 이름
TOPIC = "news"

# Kafka Producer 생성 (value는 JSON 직렬화)
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 예시 데이터 전송
sample = {"title": "예시 뉴스", "link": "http://example.com"}
producer.send(TOPIC, sample)
```

---

## 4. Flink 기반 실시간 뉴스 처리

Kafka로부터 전송된 뉴스 데이터를 Flink에서 소비(Consume)하고, 이를 전처리하여 PostgreSQL에 저장하는 실시간 데이터 처리 환경을 구성합니다.

---

### 1. 목표

Kafka 토픽(`news`)으로 들어온 뉴스 메시지를 Flink가 수신하여 처리하고, 해당 결과를 PostgreSQL의 `news_article` 테이블에 적재합니다.

---

### 2. 핵심 흐름

- Kafka에서 메시지 수신 (`FlinkKafkaConsumer`)
- 수신된 뉴스 본문(`content`)을 기반으로 전처리 수행
  - 카테고리 추론: `transform_classify_category`
  - 키워드 추출: `transform_extract_keywords`
  - 벡터 임베딩: `transform_to_embedding`
- 전처리된 결과를 PostgreSQL에 저장 (`INSERT INTO news_article`)

---

### 3. 환경 변수 설정 (.env)

```env
OPENAI_API_KEY=your_openai_api_key
KAFKA_CONNECTOR_PATH=/path/to/flink-sql-connector-kafka-3.3.0-1.20.jar
```

---

### 4. Kafka Consumer 기본 예시 (Flink 코드)

```python
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream.connectors import FlinkKafkaConsumer
import os

env = StreamExecutionEnvironment.get_execution_environment()

# Kafka connector JAR 등록
kafka_connector_path = os.getenv("KAFKA_CONNECTOR_PATH")
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

# Kafka에서 메시지 수신
stream = env.add_source(consumer)

"""이후 전처리 및 저장 로직 추가"""

env.execute("Flink Kafka Consumer Job")
```

---

### 5. 뉴스 본문 전처리 방식

Kafka에서 수신한 뉴스 본문(`content`)은 다음과 같은 방식으로 전처리합니다.

1. 키워드 추출: 본문에서 핵심 키워드 5개를 추출  
2. 벡터 임베딩: OpenAI Embedding API를 이용해 1536차원 벡터 생성
3. 카테고리 분류: OpenAI API를 활용하여 뉴스의 주제를 자동 분류  

> 자세한 구현은 `preprocessing.py` 파일을 참고하여 추가 구현하세요.  
> OpenAI API 키가 필요하며, `.env` 파일에 등록되어 있어야 합니다.


---

## 마무리

위의 단계들을 차례대로 진행하고 RSS의 뉴스를 잘 수집하고 처리하여 저장하는 것이 목표입니다.  
이를 시작으로 PostgreSQL, Hadoop, Kafka, Airflow, Elasticsearch를 이용한 SSAFY 맞춤형 뉴스 데이터 파이프라인을 지속적으로 개발하며 프로젝트를 완성하게 됩니다.
