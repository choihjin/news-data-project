from sqlalchemy import Column, Integer, String, DateTime, Text, JSON, func
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import VECTOR
from .db_config import Base

class NewsArticle(Base):
    __tablename__ = "news_article"

    id = Column(Integer, primary_key=True, autoincrement=True)
    title = Column(String(200), nullable=False)
    writer = Column(String(255), nullable=False)
    write_date = Column(DateTime, nullable=False)
    category = Column(String(50), nullable=False)
    content = Column(Text, nullable=False)
    url = Column(String(200), nullable=False, unique=True)
    keywords = Column(JSONB, default=list)
    embedding = Column(VECTOR, nullable=False)
    updated_at = Column(DateTime, server_default=func.now()) 