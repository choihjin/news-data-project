# 관통 PJT Airflow + Spark

## Airflow + Spark 환경 구축 요약

| 구성 요소 | 버전 / 특징 |
|-----------|--------------|
| Airflow | 2.10.5 (Dockerfile 빌드) |
| Spark | 3.5.4 Standalone Cluster (Master/Worker 구성) |
| PostgreSQL | 13 (5433 포트 사용) |
| 실행 방식 | Docker Compose 기반 통합 실행 |

## 1. 디렉토리 구조 및 초기 설정

```bash
# batch 안에서 실행한다는 가정
cd {해당 디렉토리}/batch

# 이미 존재하면 제외
mkdir -p ./dags ./logs ./plugins ./config ./dags/scripts ./data ./output

# 이미 존재하면 제외
echo -e "AIRFLOW_UID=$(id -u)" > .env

# Permission Denied가 뜬다면 하위 디렉토리에서도 설정
sudo chmod -R 777 . ./logs ./dags ./plugins ./config ./data ./output
```

## 2. 변경된 docker-compose 설정 요약

### Airflow Image → Dockerfile 빌드

```yaml
build:
  context: .
  dockerfile: Dockerfile.airflow
```

### PostgreSQL 포트 충돌 방지

```yaml
ports:
  - "5433:5432"
```

### 타임존 설정

```yaml
AIRFLOW__CORE__DEFAULT_TIMEZONE: Asia/Seoul
```

### 예제 DAG 비활성화

```yaml
AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
```

### DAG 자동 활성화 (선택)

```yaml
AIRFLOW__CORE__DAGS_ARE_PAUSED_AT_CREATION: 'false'
```

### 볼륨 추가

```yaml
volumes:
  - ${AIRFLOW_PROJ_DIR:-.}/dags:/opt/airflow/dags
  - ${AIRFLOW_PROJ_DIR:-.}/logs:/opt/airflow/logs
  - ${AIRFLOW_PROJ_DIR:-.}/config:/opt/airflow/config
  - ${AIRFLOW_PROJ_DIR:-.}/plugins:/opt/airflow/plugins
  - ${AIRFLOW_PROJ_DIR:-.}/dags/scripts:/opt/airflow/dags/scripts 
  - ${AIRFLOW_PROJ_DIR:-.}/data:/opt/airflow/data
  - ${AIRFLOW_PROJ_DIR:-.}/output:/opt/airflow/output
```

### 네트워크 구성

```yaml
networks:
  airflow:
    driver: bridge
```

### Spark Master/Worker 컨테이너 추가

**spark-master**

```yaml
spark-master:
  build:
    context: .
    dockerfile: Dockerfile.spark
  container_name: spark-master
  environment:
    - SPARK_MODE=master
    - SPARK_MASTER_HOST=spark-master
  ports:
    - "8083:8080"
    - "7077:7077"
  networks:
    - airflow
  volumes:
    - ./dags/scripts:/opt/airflow/dags/scripts
    - ./output:/opt/airflow/output
    - ./data:/opt/airflow/data
```

**spark-worker**

```yaml
spark-worker:
  build:
    context: .
    dockerfile: Dockerfile.spark
  container_name: spark-worker
  environment:
    - SPARK_MODE=worker
    - SPARK_MASTER_URL=spark://spark-master:7077
  depends_on:
    - spark-master
  ports:
    - "8084:8081"
  networks:
    - airflow
  volumes:
    - ./dags/scripts:/opt/airflow/dags/scripts
    - ./output:/opt/airflow/output
    - ./data:/opt/airflow/data
```

## 3. Docker Compose 실행

```bash
sudo docker compose up airflow-init
sudo docker compose up -d
```

## 4. 웹 UI 접속

- 주소: [http://localhost:8080](http://localhost:8080)
- 계정: airflow / airflow

## 5. SparkSubmitOperator 설정 예시

| 항목 | 값 |
|------|----|
| Connection Id | spark_default |
| Type | Spark |
| Host | spark://spark-master |
| Port | 7077 |
| Deploy mode | client |
| Spark binary | spark-submit |

## 6. EmailOperator SMTP 설정

```yaml
AIRFLOW__SMTP__SMTP_HOST: 'smtp.gmail.com'
AIRFLOW__SMTP__SMTP_USER: 'your_email@gmail.com'
AIRFLOW__SMTP__SMTP_PASSWORD: '앱 비밀번호'
AIRFLOW__SMTP__SMTP_PORT: 587
AIRFLOW__SMTP__SMTP_MAIL_FROM: 'your_email@gmail.com'
```


## 7. 디렉토리 구조 예시

```bash
batch/
├── dags/
│   ├── daily_report_dag.py
│   └── scripts/
│         └── spark_daily_report.py
├── logs/
├── plugins/
├── config/
├── data/
├── output/
├── docker-compose.yml
└── .env
```

## 마무리 요약

| 항목 | 설명 |
|------|------|
| Airflow | 커스텀 Dockerfile로 빌드, Spark 연동 |
| Spark | Master/Worker 구성, Standalone 모드 |
| PostgreSQL | 포트 충돌 방지 (5433 노출) |
| 네트워크 | airflow 네트워크 구성 |
| 실습 편의 | volumes 공유, DAG 자동 reload |
| 이메일 발송 | Gmail SMTP 설정 가능 |
