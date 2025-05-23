import feedparser
import requests
from bs4 import BeautifulSoup
from datetime import datetime
from dateutil import parser as date_parser
from kafka import KafkaProducer
import json
import time

# ✅ Kafka 설정
KAFKA_BROKER = "kafka:9092"
TOPIC = "news"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8')
)

# ✅ RSS 피드 URL
RSS_FEED_URL = "https://www.hankyung.com/feed/it"

# ✅ 본문 크롤링 함수
def get_article_content(url: str) -> str:
    try:
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36"
        }
        res = requests.get(url, headers=headers, timeout=5)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, 'html.parser')
        
        # 여러 가능한 본문 영역 선택자
        possible_selectors = [
            '.article-body', 
            '.article-text', 
            '#articletxt',
            '.article-contents',
            '#newsView',
            '.news-contents',
            '[itemprop="articleBody"]'
        ]
        
        article_div = None
        for selector in possible_selectors:
            article_div = soup.select_one(selector)
            if article_div:
                break

        if not article_div:
            print(f"❗ 본문 div 없음: {url}")
            return None

        # 불필요한 요소 제거
        for tag in article_div.select('.promotion_wrap, .article_ad, .article_footer, script, style, .article-tools, .article-bottom, .article-sns, .copyright, iframe, .article-relation-news'):
            tag.decompose()

        # 본문 문단 추출 및 정제
        paragraphs = []
        
        # 1. 먼저 구조화된 문단 찾기
        content_elements = article_div.select('p, .paragraph, .content, [itemprop="articleBody"] > div')
        
        if not content_elements:
            # 2. 구조화된 문단이 없으면 직접 텍스트 분리
            text = article_div.get_text('\n', strip=True)
            paragraphs = [p.strip() for p in text.split('\n') if p.strip()]
        else:
            for element in content_elements:
                text = element.get_text(strip=True)
                if text:
                    paragraphs.append(text)

        # 불필요한 문단 필터링
        filtered_paragraphs = []
        skip_patterns = [
            '▶', '©', '저작권자', '무단전재', '배포금지', '▼', 
            '관련기사', '한경닷컴', '네이버에서 한국경제 뉴스를',
            '구독하기', '좋아요', '공유하기', '메일보내기',
            '본문 내용과 URL을 복사합니다', '닫기'
        ]
        
        for p in paragraphs:
            if p and len(p) > 10 and not any(skip in p for skip in skip_patterns):
                filtered_paragraphs.append(p)

        # 문단 사이에 빈 줄을 추가하여 결합
        content = '\n\n'.join(filtered_paragraphs)
        
        # 본문 길이 체크 기준 추가 완화
        if len(content.strip()) < 30:  # 기준값을 더 낮춤
            print(f"⛔ 본문 너무 짧음: {url}")
            return None

        return content.strip()

    except Exception as e:
        print(f"❗ 크롤링 에러: {url} → {e}")
        return None

# ✅ 메인 로직
def main():
    print("📡 한국경제 IT 뉴스 수집 시작\n")
    feed = feedparser.parse(RSS_FEED_URL)
    print(f"총 기사 수: {len(feed.entries)}")

    for entry in feed.entries:
        content = get_article_content(entry.link)

        if content is None:
            print(f"⛔ 본문 누락 → Kafka 전송 생략: {entry.link}")
            continue

        article = {
            "title": entry.title,
            "writer": entry.get("author", "한국경제"),
            "write_date": date_parser.parse(entry.get("published", "2025-01-01T00:00:00")).isoformat(),
            "content": content,
            "url": entry.link,
            "keywords": []
        }

        # ✅ Kafka 전송
        producer.send(TOPIC, value=article)
        print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] ✅ Kafka 전송 완료: {article['title']}")

    print("\n🛠️ Kafka Producer 작업 완료!")

if __name__ == "__main__":
    main()
