from hdfs import InsecureClient
import json

# namenode의 IP 주소로 연결 (Docker 네트워크 내부 IP)
client = InsecureClient('http://172.18.0.11:9870')  # 실제 IP 주소는 docker inspect namenode로 확인

# 테스트 데이터
test_data = {
    "title": "테스트 제목",
    "content": "테스트 내용",
    "write_date": "2025-05-24"
}

# JSON으로 변환
json_str = json.dumps(test_data, ensure_ascii=False) + "\n"

# HDFS에 저장
try:
    with client.write('/realtime/test.json', append=True) as writer:
        writer.write(json_str.encode('utf-8'))
    print("HDFS 저장 성공")
except Exception as e:
    print(f"HDFS 저장 실패: {str(e)}")