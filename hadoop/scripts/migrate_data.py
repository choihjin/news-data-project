import os
import json
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.hdfs_manager import HDFSManager

def migrate_news_data():
    hdfs_manager = HDFSManager()
    
    # 실시간 뉴스 데이터 이전
    realtime_dir = "/mnt/c/Users/SSAFY/Desktop/data-pjt/batch/data/realtime"
    for filename in os.listdir(realtime_dir):
        if filename.endswith('.json'):
            file_path = os.path.join(realtime_dir, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    # 한 줄씩 읽어서 처리
                    for line in f:
                        line = line.strip()
                        if line:  # 빈 줄 무시
                            try:
                                news_data = json.loads(line)
                                hdfs_manager.save_news_data(news_data, 'realtime')
                            except json.JSONDecodeError as e:
                                print(f"JSON 파싱 오류 in {filename}: {e}")
                                continue
                print(f"Completed: {filename}")
            except Exception as e:
                print(f"파일 읽기 오류 {filename}: {e}")
                continue
    
    # 아카이브 뉴스 데이터 이전
    archive_dir = "/mnt/c/Users/SSAFY/Desktop/data-pjt/batch/data/news_archive"
    for filename in os.listdir(archive_dir):
        if filename.endswith('.json'):
            file_path = os.path.join(archive_dir, filename)
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    # 한 줄씩 읽어서 처리
                    for line in f:
                        line = line.strip()
                        if line:  # 빈 줄 무시
                            try:
                                news_data = json.loads(line)
                                hdfs_manager.save_news_data(news_data, 'archive')
                            except json.JSONDecodeError as e:
                                print(f"JSON 파싱 오류 in {filename}: {e}")
                                continue
                print(f"Completed: {filename}")
            except Exception as e:
                print(f"파일 읽기 오류 {filename}: {e}")
                continue

if __name__ == "__main__":
    migrate_news_data()
