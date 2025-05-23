from hdfs import InsecureClient
import json
import os
from datetime import datetime

class HDFSManager:
    def __init__(self, hdfs_url="http://localhost:9870"):
        self.client = InsecureClient(hdfs_url)
        self._initialize_directories()
    
    def _initialize_directories(self):
        """필요한 HDFS 디렉토리 생성"""
        directories = [
            '/realtime',  # 실시간 뉴스 저장
            '/news_archive',  # 아카이브 뉴스 저장
            '/processed',  # 처리된 데이터 저장
            '/reports'  # 분석 리포트 저장
        ]
        
        for directory in directories:
            if not self.client.status(directory, strict=False):
                self.client.makedirs(directory)
    
    def save_news_data(self, news_data, data_type='realtime'):
        """뉴스 데이터를 HDFS에 저장"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        if data_type == 'realtime':
            path = f"/realtime/news_{timestamp}.json"
        else:
            path = f"/news_archive/news_{timestamp}.json"
            
        try:
            with self.client.write(path, overwrite=True) as writer:
                json.dump(news_data, writer, ensure_ascii=False)
            return path
        except Exception as e:
            print(f"HDFS 저장 오류: {e}")
            return None
    
    def read_news_data(self, path):
        """HDFS에서 뉴스 데이터 읽기"""
        try:
            with self.client.read(path) as reader:
                return json.load(reader)
        except Exception as e:
            print(f"HDFS 읽기 오류: {e}")
            return None
