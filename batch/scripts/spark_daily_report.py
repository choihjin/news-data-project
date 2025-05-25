import sys
import argparse
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, ArrayType
from pyspark.sql.functions import col, explode, count, hour, to_timestamp, when, to_json, struct
import matplotlib.pyplot as plt
import numpy as np
import io
import matplotlib.font_manager as fm
from matplotlib.backends.backend_pdf import PdfPages

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
    print(f"시작 날짜: {report_date_str}")

    FONT_PATH = "/usr/share/fonts/truetype/nanum/NanumGothic.ttf"
    # HDFS 경로 설정
    INPUT_PATH = f"hdfs://namenode:8020/realtime/news_{report_date_str}.json"
    ARCHIVE_PATH = f"hdfs://namenode:8020/news_archive/news_{report_date_str}.json"
    REPORT_PATH = f"/opt/airflow/data/daily_report_{report_date_str}.pdf"
    font_prop = fm.FontProperties(fname=FONT_PATH, size=12)

    spark = SparkSession.builder \
        .appName("DailyNewsReport") \
        .config("spark.master", "spark://spark-master:7077") \
        .config("spark.executor.memory", "2g") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()

    try:
        # JSON 파일 읽기 - 한 줄씩 읽기
        df = spark.read \
            .option("multiLine", "true") \
            .option("mode", "PERMISSIVE") \
            .json(INPUT_PATH)
        
        df.printSchema()
        print(f"데이터 로드 완료: {df.count()}개 기사")

        # 리포트 생성
        with PdfPages(REPORT_PATH) as pdf:
            # 키워드 분석
            if "keywords" in df.columns:
                df_kw = df.withColumn("keyword", explode(col("keywords")))
                keyword_df = df_kw.groupBy("keyword").count().orderBy("count", ascending=False).limit(10)
                keyword_pd = keyword_df.toPandas()
                
                plt.figure(figsize=(8, 5))
                keyword_pd.plot(kind="bar", x="keyword", y="count", legend=False, color="skyblue", ax=plt.gca())
                plt.title(f"{report_date_str} 키워드 TOP 10", fontproperties=font_prop)
                plt.xticks(rotation=45, fontproperties=font_prop)
                plt.tight_layout()
                pdf.savefig()
                plt.close()

            # 카테고리별 기사 수
            if "category" in df.columns:
                category_df = df.groupBy("category").count().orderBy("count", ascending=False)
                category_pd = category_df.toPandas()

                plt.figure(figsize=(8, 5))
                category_pd.plot(kind="bar", x="category", y="count", legend=False, color="orange", ax=plt.gca())
                plt.title(f"{report_date_str} 카테고리별 기사 수", fontproperties=font_prop)
                plt.xticks(rotation=45, fontproperties=font_prop)
                plt.tight_layout()
                pdf.savefig()
                plt.close()

            # 시간대별 기사 수
            if "write_date" in df.columns:
                df = df.withColumn("hour", col("write_date").substr(12, 2))
                hour_df = df.groupBy("hour").count().orderBy("hour")
                hour_pd = hour_df.toPandas()

                plt.figure(figsize=(8, 5))
                hour_pd.plot(kind="bar", x="hour", y="count", legend=False, color="green", ax=plt.gca())
                plt.title(f"{report_date_str} 시간대별 기사 수", fontproperties=font_prop)
                plt.xticks(rotation=0, fontproperties=font_prop)
                plt.tight_layout()
                pdf.savefig()
                plt.close()

        print(f"리포트 저장 완료: {REPORT_PATH}")

        # HDFS 파일 아카이빙 (줄 단위 JSON으로 저장)
        df.select(to_json(struct([col(c) for c in df.columns])).alias("value")) \
            .write \
            .mode("append") \
            .option("compression", "none") \
            .text(ARCHIVE_PATH)
        print("원본 파일 아카이빙 완료.")

        # 원본 파일 삭제
        spark._jsc.hadoopConfiguration().set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem")
        spark._jsc.hadoopConfiguration().set("fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        fs.delete(spark._jvm.org.apache.hadoop.fs.Path(INPUT_PATH), True)
        print("원본 파일 삭제 완료.")

    except Exception as e:
        print("처리 중 오류 발생:", e)
        raise
    finally:
        spark.stop()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Spark를 이용한 일일 뉴스 리포트 생성")
    parser.add_argument("--date", required=True, help="보고서 기준 날짜 (YYYY-MM-DD)")
    args = parser.parse_args()
    main(args.date) 