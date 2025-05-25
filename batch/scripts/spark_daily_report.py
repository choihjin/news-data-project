import sys
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType
from pyspark.sql.functions import col, explode, count, hour, to_timestamp, when
import matplotlib.pyplot as plt
import numpy as np
import io

# 뉴스 데이터 스키마 정의
NEWS_SCHEMA = StructType([
    StructField("title", StringType(), True),
    StructField("write_date", StringType(), True),
    StructField("category", StringType(), True),
    StructField("content", StringType(), True),
    StructField("keywords", ArrayType(StringType()), True),
    StructField("url", StringType(), True)
])

def main(report_date_str):
    try:
        # Spark 세션 생성
        spark = SparkSession.builder \
            .appName("DailyNewsReport") \
            .config("spark.master", "spark://spark-master:7077") \
            .config("spark.executor.memory", "2g") \
            .config("spark.driver.memory", "2g") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.default.parallelism", "200") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020") \
            .getOrCreate()

        # HDFS 경로 설정
        input_path = f"hdfs://namenode:8020/realtime/news_{report_date_str}.json"
        archive_path = f"hdfs://namenode:8020/news_archive/news_{report_date_str}.json"
        report_path = f"hdfs://namenode:8020/reports/daily_report_{report_date_str}.pdf"

        # JSON 파일 읽기 시 옵션 추가
        df = spark.read \
            .option("maxRecordsPerFile", "100") \
            .option("multiLine", "true") \
            .option("mode", "PERMISSIVE") \
            .option("columnNameOfCorruptRecord", "_corrupt_record") \
            .option("samplingRatio", "1.0") \
            .option("primitivesAsString", "true") \
            .option("maxCharsPerColumn", "1000000") \
            .schema(NEWS_SCHEMA) \
            .json(input_path)
        
        # 오류 레코드 필터링
        # df = df.filter(col("_corrupt_record").isNull())

        # 데이터 분석
        # 1. 키워드 분석
        keyword_counts = df.select(explode("keywords").alias("keyword")) \
            .groupBy("keyword") \
            .count() \
            .orderBy("count", ascending=False) \
            .limit(10)
        
        # 2. 출처별 기사 수
        source_counts = df.groupBy("category") \
            .count() \
            .orderBy("count", ascending=False)
        
        # 3. 시간대별 기사 수
        hourly_counts = df.withColumn("hour", 
            when(to_timestamp("write_date").isNotNull(), 
                 hour(to_timestamp("write_date")))
            .otherwise(0)) \
            .groupBy("hour") \
            .count() \
            .orderBy("hour")

        # 그래프 스타일 설정 추가
        plt.style.use('ggplot')  # 더 현대적인 스타일 적용

        import matplotlib.font_manager as fm
        font_path = '/usr/share/fonts/truetype/nanum/NanumGothic.ttf'  # 실제 경로 확인 필요
        fontprop = fm.FontProperties(fname=font_path)
        plt.rc('font', family=fontprop.get_name())

        # 그래프 생성
        plt.figure(figsize=(15, 10))
        
        # 1. 키워드 분석 그래프
        plt.subplot(3, 1, 1)
        keyword_data = keyword_counts.collect()
        keywords = [row['keyword'] for row in keyword_data]
        counts = [int(row['count']) for row in keyword_data]
        x = np.arange(len(keywords))
        plt.bar(x, counts, color='skyblue')
        plt.xticks(x, keywords, rotation=45, ha='right')
        plt.title('상위 10개 키워드', pad=20)
        plt.grid(True, alpha=0.3)
        
        # 2. 카테고리별 기사 수 그래프
        plt.subplot(3, 1, 2)
        source_data = source_counts.collect()
        categories = [row['category'] for row in source_data]
        counts = [int(row['count']) for row in source_data]
        x = np.arange(len(categories))
        plt.bar(x, counts, color='lightgreen')
        plt.xticks(x, categories, rotation=45, ha='right')
        plt.title('카테고리별 기사 수', pad=20)
        plt.grid(True, alpha=0.3)
        
        # 3. 시간대별 기사 수 그래프
        plt.subplot(3, 1, 3)
        hourly_data = hourly_counts.collect()
        hours = [int(row['hour']) for row in hourly_data]
        counts = [int(row['count']) for row in hourly_data]
        plt.plot(hours, counts, marker='o', color='coral', linewidth=2)
        plt.title('시간대별 기사 수', pad=20)
        plt.xlabel('시간')
        plt.ylabel('기사 수')
        plt.grid(True, alpha=0.3)
        
        plt.tight_layout()
        
        # 그래프를 바이트로 저장
        buffer = io.BytesIO()
        plt.savefig(buffer, format='pdf')
        buffer.seek(0)
        pdf_data = buffer.getvalue()
        buffer.close()

        # PDF를 HDFS와 로컬에 저장
        try:
            # HDFS에 저장
            spark.sparkContext.parallelize([pdf_data]).saveAsTextFile(report_path)
            print(f"HDFS 리포트 저장 완료: {report_path}")
            
            # 로컬 data 디렉토리에 저장
            local_report_path = f"/opt/airflow/data/daily_report_{report_date_str}.pdf"
            with open(local_report_path, 'wb') as f:
                f.write(pdf_data)
            print(f"로컬 리포트 저장 완료: {local_report_path}")
        except Exception as e:
            print(f"리포트 저장 실패:d {str(e)}")
            raise

        # 원본 데이터를 아카이브로 이동
        try:
            df.write.mode("overwrite").json(archive_path)
            print(f"데이터 아카이브 완료: {archive_path}")
        except Exception as e:
            print(f"데이터 아카이브 실패: {str(e)}")
            raise

    except Exception as e:
        print(f"오류 발생: {str(e)}")
        raise

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='일일 뉴스 리포트 생성')
    parser.add_argument('--date', required=True, help='리포트 날짜 (YYYY-MM-DD)')
    args = parser.parse_args()
    main(args.date) 