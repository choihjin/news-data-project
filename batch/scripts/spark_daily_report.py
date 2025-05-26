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

def append_or_create_json(df, spark, report_date_str):
    ARCHIVE_PATH = f"/news_archive/news_{report_date_str}.json"
    full_path = f"hdfs://namenode:8020{ARCHIVE_PATH}"

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    Path = spark._jvm.org.apache.hadoop.fs.Path
    archive_path = Path(full_path)

    try:
        if fs.exists(archive_path):
            print(f"기존 아카이브 파일 존재: {ARCHIVE_PATH}")
            existing_df = spark.read.json(full_path)
            merged_df = existing_df.unionByName(df)
        else:
            print(f"신규 아카이브 파일 생성: {ARCHIVE_PATH}")
            merged_df = df

        # 디렉토리에 우선 저장
        tmp_dir = ARCHIVE_PATH + "_tmp"
        tmp_full_path = f"hdfs://namenode:8020{tmp_dir}"

        merged_df.select(to_json(struct([col(c) for c in merged_df.columns])).alias("value")) \
            .coalesce(1) \
            .write \
            .mode("overwrite") \
            .json(tmp_full_path)

        print("임시 파일 저장 완료")
        return tmp_dir  # 임시 디렉토리 경로 리턴
    except Exception as e:
        print("아카이브 저장 중 오류 발생:", e)
        raise


def save_single_json(spark, tmp_dir_path, final_file_path):
    """
    tmp_dir_path: '/news_archive/news_2025-05-26.json_tmp'
    final_file_path: '/news_archive/news_2025-05-26.json'
    """
    hdfs_prefix = "hdfs://namenode:8020"
    tmp_path = f"{hdfs_prefix}{tmp_dir_path}"
    final_path = f"{hdfs_prefix}{final_file_path}"

    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    Path = spark._jvm.org.apache.hadoop.fs.Path

    tmp_path_obj = Path(tmp_path)
    final_path_obj = Path(final_path)

    file_list = fs.listStatus(tmp_path_obj)
    found = False
    for file_status in file_list:
        name = file_status.getPath().getName()
        if name.startswith("part-") and name.endswith(".json"):
            part_file = file_status.getPath()
            if fs.exists(final_path_obj):
                fs.delete(final_path_obj, False)
            fs.rename(part_file, final_path_obj)
            print(f"최종 파일 저장 완료: {final_file_path}")
            found = True
            break

    if not found:
        raise Exception("part-0000 파일을 찾을 수 없음")

    fs.delete(tmp_path_obj, True)
    print("임시 디렉토리 삭제 완료")


def main(report_date_str):
    print(f"시작 날짜: {report_date_str}")

    FONT_PATH = "/usr/share/fonts/truetype/nanum/NanumGothic.ttf"
    # HDFS 경로 설정
    INPUT_PATH = f"hdfs://namenode:8020/realtime/news_{report_date_str}.json"
    ARCHIVE_PATH = f"hdfs://namenode:8020/news_archive/news_{report_date_str}.json"
    REPORT_PATH = f"/opt/airflow/data/daily_report_{report_date_str}.pdf"
    HDFS_REPORT_PATH = f"hdfs://namenode:8020/reports/daily_report_{report_date_str}.pdf"
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
            .option("multiLine", "false") \
            .option("mode", "PERMISSIVE") \
            .schema(NEWS_SCHEMA) \
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

        # HDFS에 PDF 리포트 저장
        fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
        Path = spark._jvm.org.apache.hadoop.fs.Path
        
        # reports 디렉토리 생성
        reports_dir = Path("hdfs://namenode:8020/reports")
        if not fs.exists(reports_dir):
            fs.mkdirs(reports_dir)
        
        # 로컬 PDF 파일을 HDFS로 복사
        local_path = Path(REPORT_PATH)
        hdfs_path = Path(HDFS_REPORT_PATH)
        fs.copyFromLocalFile(False, True, local_path, hdfs_path)
        print(f"HDFS 리포트 저장 완료: {HDFS_REPORT_PATH}")

        # HDFS 파일 아카이빙 (JSON 형식으로 저장)
        tmp_dir_path = append_or_create_json(df, spark, report_date_str)
        ARCHIVE_PATH_SHORT = f"/news_archive/news_{report_date_str}.json"
        save_single_json(spark, tmp_dir_path, ARCHIVE_PATH_SHORT)

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