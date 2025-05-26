import json
import random
from datetime import datetime, timedelta
import subprocess
import os

def generate_test_data(num_articles=100):
    # 카테고리 목록
    categories = ["정치", "경제", "사회", "국제", "문화", "스포츠", "IT"]
    
    # 키워드 목록
    keywords_list = [
        ["코로나", "백신", "확진자", "방역", "마스크"],
        ["주식", "금리", "환율", "부동산", "주택"],
        ["사고", "범죄", "재난", "안전", "사망"],
        ["미국", "중국", "일본", "러시아", "북한"],
        ["영화", "음악", "예술", "공연", "전시"],
        ["축구", "야구", "농구", "올림픽", "월드컵"],
        ["AI", "빅데이터", "클라우드", "메타버스", "블록체인"]
    ]
    
    # 테스트 데이터 생성
    articles = []
    base_date = datetime.now()
    
    for i in range(num_articles):
        # 랜덤 카테고리 선택
        category_idx = random.randint(0, len(categories)-1)
        category = categories[category_idx]
        
        # 랜덤 시간 생성 (최근 24시간 내)
        random_hours = random.randint(0, 23)
        random_minutes = random.randint(0, 59)
        article_date = base_date - timedelta(hours=random_hours, minutes=random_minutes)
        
        # 랜덤 키워드 선택 (2-4개)
        num_keywords = random.randint(2, 4)
        keywords = random.sample(keywords_list[category_idx], num_keywords)
        
        # 기사 데이터 생성
        article = {
            "title": f"테스트 기사 {i+1} - {category}",
            "write_date": article_date.strftime("%Y-%m-%d %H:%M:%S"),
            "category": category,
            "content": f"이것은 {category} 카테고리의 테스트 기사 {i+1}의 내용입니다. " * 5,
            "keywords": keywords,
            "url": f"http://test.com/article/{i+1}"
        }
        articles.append(article)
    
    return articles

def save_to_hdfs(articles, date_str):
    # 로컬 임시 파일에 저장
    temp_file = f"temp_news_{date_str}.json"
    with open(temp_file, 'w', encoding='utf-8') as f:
        for article in articles:
            f.write(json.dumps(article, ensure_ascii=False) + '\n')
    
    try:
        # Docker 컨테이너에 파일 복사
        subprocess.run(['docker', 'cp', temp_file, 'namenode:/tmp/'], check=True)
        
        # HDFS 디렉토리 생성
        # subprocess.run(['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-mkdir', '-p', '/realtime'], check=True)
        
        # 파일 업로드
        hdfs_path = f"hdfs://namenode:8020/realtime/news_{date_str}.json"
        subprocess.run(['docker', 'exec', 'namenode', 'hdfs', 'dfs', '-put', '-f', f'/tmp/{temp_file}', hdfs_path], check=True)
        
        print(f"{len(articles)}개의 테스트 데이터가 {hdfs_path}에 저장되었습니다.")
        
    except subprocess.CalledProcessError as e:
        print(f"에러 발생: {e}")
    finally:
        # 임시 파일 정리
        if os.path.exists(temp_file):
            os.remove(temp_file)
        try:
            subprocess.run(['docker', 'exec', 'namenode', 'rm', '-f', f'/tmp/{temp_file}'], check=True)
        except subprocess.CalledProcessError:
            pass

def main():
    # 특정 날짜 지정
    test_date = "2025-05-26"
    articles = generate_test_data(num_articles=100)
    save_to_hdfs(articles, test_date)

if __name__ == "__main__":
    main()