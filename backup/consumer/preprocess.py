from openai import OpenAI
from dotenv import load_dotenv
load_dotenv()

def preprocess_content(content):
    """
    데이터 전처리 - 텍스트 길이 제한  (5000 토큰)
    토큰 수를 제한하여 처리 효율성 확보
    """
    import tiktoken

    if not content:
        return ""
        
    encoding = tiktoken.get_encoding("cl100k_base")
    tokens = encoding.encode(content)
    
    if len(tokens) > 5000:
        truncated_tokens = tokens[:5000]
        return encoding.decode(truncated_tokens)
    
    return content


def transform_extract_keywords(text):
    """
    텍스트 데이터 변환 - 키워드 5개 추출  
    입력 텍스트에서 핵심 키워드를 추출하는 변환 로직
    """
    text = preprocess_content(text)

    client = OpenAI()
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {
                "role": "system",
                "content": "당신은 뉴스 기사의 핵심 키워드를 추출하는 언어 모델입니다. 주어진 텍스트에서 중요한 단어를 5개 선택하세요. 불용어나 일반 단어는 제외하고 핵심적인 개념이나 주제를 중심으로 추출해주세요."
            },
            {
                "role": "user",
                "content": f"{text}\n\n[요청] 위 기사에서 핵심 키워드 5개를 쉼표로 구분하여 출력해주세요."
            }
        ],
        max_tokens=100,
        temperature=0.3
    )

    keywords = response.choices[0].message.content.strip()
    return [k.strip() for k in keywords.split(',') if k.strip()]


def transform_to_embedding(text: str) -> list[float]:
    """
    텍스트 데이터 변환 - 벡터 임베딩  
    텍스트를 수치형 벡터로 변환하는 변환 로직
    """
    text = preprocess_content(text)

    client = OpenAI()
    response = client.embeddings.create(input=text, model="text-embedding-3-small")
    return response.data[0].embedding


def transform_classify_category(content):
    """
    텍스트 데이터 변환 - 카테고리 분류  
    뉴스 내용을 기반으로 적절한 카테고리로 분류하는 변환 로직
    """

    content = preprocess_content(content)

    CATEGORY_LIST = [
        "AI", "빅데이터", "블록체인", "사물인터넷", "클라우드", "로봇", "자율주행",
        "반도체", "스타트업", "핀테크", "보안", "메타버스", "양자컴퓨팅", "AR/VR", "기타"
    ]

    prompt = (
        "다음 뉴스 기사의 내용을 보고 아래 소분류 기술 카테고리 중 가장 적절한 하나를 선택하세요.\n"
        f"카테고리 목록: {', '.join(CATEGORY_LIST)}\n\n"
        f"기사 내용:\n{content}\n\n"
        "선택된 카테고리만 한글 그대로 출력해주세요."
    )

    client = OpenAI()
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=[
            {"role": "system", "content": "너는 IT 기술 기사를 분류하는 뉴스 카테고리 분류기야."},
            {"role": "user", "content": prompt}
        ],
        max_tokens=30
    )

    model_output = response.choices[0].message.content.strip()

    if model_output not in CATEGORY_LIST:
        model_output = "기타"

    return model_output