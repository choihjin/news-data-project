from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from datetime import datetime

class NewsDataProcessor:
    def __init__(self):
        self.spark = SparkSession.builder \
            .appName("NewsDataProcessor") \
            .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000") \
            .getOrCreate()
    
    def process_news_data(self, input_path, output_path):
        """뉴스 데이터 처리 및 분석"""
        # HDFS에서 데이터 읽기
        df = self.spark.read.json(input_path)
        
        # 데이터 전처리
        processed_df = df.select(
            col("title"),
            col("content"),
            col("write_date"),
            col("category"),
            col("keywords"),
            col("url")
        )
        
        # 결과를 HDFS에 저장
        processed_df.write \
            .mode("overwrite") \
            .json(output_path)
    
    def generate_daily_report(self, date):
        """일일 뉴스 분석 리포트 생성"""
        input_path = "/realtime/*.json"
        output_path = f"/reports/daily_{date}"
        
        df = self.spark.read.json(input_path)
        
        # 카테고리별 기사 수 집계
        category_stats = df.groupBy("category") \
            .count() \
            .orderBy("count", ascending=False)
        
        # 결과를 HDFS에 저장
        category_stats.write \
            .mode("overwrite") \
            .json(output_path)
