import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser as date_parser
from kafka import KafkaProducer
import json
import time

# Kafka 설정
KAFKA_BROKER = "kafka:9092"
TOPIC = "news"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

# RSS 피드 URL
RSS_FEED_URL = "https://rss.etnews.com/03.xml"

# 본문 크롤링 함수
def get_article_content(url: str) -> str:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0"
        }
        res = requests.get(url, headers=headers, timeout=5)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')

        # 정확한 본문 div 선택
        article_div = soup.select_one("div.article_body#articleBody")
        if not article_div:
            print(f"본문 div 없음: {url}")
            return None

        # 광고 및 불필요 태그 제거
        for tag in article_div.select("script, style, .ad_newsroom1234, #newsroom_oview_promotion"):
            tag.decompose()

        # <br> 기준으로 문단 분리
        for br in article_div.find_all("br"):
            br.replace_with("\n")

        raw_text = article_div.get_text(separator="\n", strip=True)

        # 문단 정리
        paragraphs = [p.strip() for p in raw_text.split("\n") if len(p.strip()) > 10]

        # 불필요한 문구 제거
        skip_patterns = [
            "기자", "제보", "무단전재", "배포금지", "좋아요", "구독", "공유", "메일보내기"
        ]
        filtered = [p for p in paragraphs if not any(skip in p for skip in skip_patterns)]

        content = "\n\n".join(filtered)

        if len(content) < 50:  # 더 완화 가능
            print(f"본문 너무 짧음: {url}")
            return None

        return content

    except Exception as e:
        print(f"크롤링 에러: {url} → {e}")
        return None

# 메인 로직
def main():
    print("전자신문 IT 뉴스 수집 시작\n")
    feed = feedparser.parse(RSS_FEED_URL)
    print(f"총 기사 수: {len(feed.entries)}")

    for entry in feed.entries:
        content = get_article_content(entry.link)

        if content is None:
            print(f"본문 누락 → Kafka 전송 생략: {entry.link}")
            continue

        article = {
            "title": entry.title,
            "writer": entry.get("author", "전자신문"),
            "write_date": date_parser.parse(entry.get("published", "2025-01-01T00:00:00")).isoformat(),
            "content": content,
            "url": entry.link,
            "keywords": []
        }

        # Kafka 전송
        producer.send(TOPIC, value=article)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] Kafka 전송 완료: {article['title']}")

    print("\nKafka Producer 작업 완료!")

if __name__ == "__main__":
    main()