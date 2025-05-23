from pydantic import BaseModel
from typing import List
from datetime import datetime
from typing import Optional

class NewsArticle(BaseModel):
    title: str
    writer: str
    write_date: datetime
    category: Optional[str] = None
    content: str
    url: str
    keywords: List[str] = []
    embedding: List[float] = []