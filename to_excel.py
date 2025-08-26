import json
import pandas as pd

# JSON 파일 읽기
with open("final.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# dict → DataFrame
df = pd.DataFrame(list(data.values()))

# 필요한 컬럼만 선택 및 순서 지정
df = df[["Title", "Channel", "UploadDate", "Video URL"]]

# 날짜 포맷 변환 (yyyy-mm-dd)
df["UploadDate"] = pd.to_datetime(df["UploadDate"], utc=True, errors="coerce")

# 정렬: Channel 오름차순, UploadDate 내림차순
df.sort_values(by=["Channel", "UploadDate"], ascending=[True, False], inplace=True)

# index 추가 (1부터 시작)
df.reset_index(drop=True, inplace=True)
df.index = df.index + 1
df.index.name = "Index"

# UploadDate를 yyyy-mm-dd 문자열로 변환
df["UploadDate"] = df["UploadDate"].dt.strftime("%Y-%m-%d")

# 엑셀 저장
output_file = "data_us_sorted.xlsx"
df.to_excel(output_file, index=True)

print(f"[INFO] 엑셀 저장 완료: {output_file}")
