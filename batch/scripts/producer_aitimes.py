from kafka import KafkaProducer
import json
import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime
import time

# Kafka 설정
KAFKA_BROKER = 'kafka:9092'
TOPIC = 'news'

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

RSS_FEED_URL = "https://www.aitimes.com/rss/allArticle.xml"

def get_article_content(url: str) -> str:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0"
        }
        res = requests.get(url, headers=headers, timeout=5)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        content_div = soup.select_one('#article-view-content-div')
        
        if not content_div:
            print(f"❗ 본문 div 없음: {url}")
            return None
            
        # 불필요한 요소 제거
        for tag in content_div.select('div.article_ad, .copyright, script, style'):
            tag.decompose()
            
        # 각 단락을 추출하여 정제
        paragraphs = []
        for p in content_div.find_all(['p', 'div']):
            text = p.get_text(strip=True)
            if text and not any(skip in text.lower() for skip in ['▶', '©', '저작권자', '무단전재', '배포금지', '▼', '관련기사', '기자']):
                # 불필요한 공백 제거 및 문장 정리
                text = ' '.join(text.split())
                paragraphs.append(text)
        
        # 문단 사이에 빈 줄을 추가하여 결합
        content_text = '\n\n'.join(paragraphs)
        
        # 본문 길이 체크
        if len(content_text.strip()) < 100:
            print(f"⛔ 본문 너무 짧음: {url}")
            return None
            
        return content_text.strip()
    except Exception as e:
        print(f"❗ 크롤링 에러: {url} → {e}")
        return None

def send_to_kafka(article):
    try:
        producer.send(TOPIC, value=article)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] ✅ Kafka 전송 완료: {article['title']}")
    except Exception as e:
        print(f"❌ Kafka 전송 실패: {e}")

seen_links = set()

def main():
    print("📡 AI타임스 뉴스 수집 시작\n")
    feed = feedparser.parse(RSS_FEED_URL)
    print(f"총 기사 수: {len(feed.entries)}")

    for entry in feed.entries:
        if entry.link in seen_links:
            continue
        seen_links.add(entry.link)

        try:
            published = entry.get("published", "2025-01-01 00:00:00")
            published_dt = datetime.strptime(published, "%Y-%m-%d %H:%M:%S")
        except:
            published_dt = datetime.now()

        content = get_article_content(entry.link)
        if content is None:
            print(f"⛔ 본문 누락 → Kafka 전송 생략: {entry.link}")
            continue

        article = {
            "title": entry.title,
            "writer": entry.get("author", "AI타임스"),
            "write_date": published_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "content": content,
            "url": entry.link,
            "keywords": []
        }

        send_to_kafka(article)
        time.sleep(0.5)  # 요청 간격을 0.5초로 줄임
        
    producer.flush()
    print("\n🛠️ Kafka Producer 작업 완료!")

if __name__ == "__main__":
    main()