from sqlalchemy.orm import Session
from .models import NewsArticle
from datetime import datetime

def create_news_article(db: Session, article_data: dict):
    """뉴스 기사를 데이터베이스에 저장"""
    db_article = NewsArticle(
        title=article_data["title"],
        writer=article_data["writer"],
        write_date=datetime.fromisoformat(article_data["write_date"]),
        category=article_data.get("category", "기타"),  # 카테고리가 없으면 "기타"로 설정
        content=article_data["content"],
        url=article_data["url"],
        keywords=article_data.get("keywords", []),
        embedding=article_data.get("embedding", [0.0] * 1536)  # 기본 임베딩 벡터 (예: 1536 차원)
    )
    db.add(db_article)
    db.commit()
    db.refresh(db_article)
    return db_article

def get_news_article_by_url(db: Session, url: str):
    """URL로 뉴스 기사 조회"""
    return db.query(NewsArticle).filter(NewsArticle.url == url).first()

def get_all_news_articles(db: Session, skip: int = 0, limit: int = 100):
    """모든 뉴스 기사 조회"""
    return db.query(NewsArticle).offset(skip).limit(limit).all() 