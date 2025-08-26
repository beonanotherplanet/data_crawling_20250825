import json
import pandas as pd

# JSON 파일 읽기
with open("temp.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# JSON → DataFrame 변환
df = pd.DataFrame(list(data.values()))

# 원하는 컬럼 순서로 정리
df = df[["Title", "Channel", "UploadDate", "Video URL"]]

# 날짜 포맷 yyyy-mm-dd 로 변환
df["UploadDate"] = pd.to_datetime(df["UploadDate"], utc=True, errors="coerce").dt.strftime("%Y-%m-%d")

# 엑셀 저장
output_file = "mbc.xlsx"
df.to_excel(output_file, index=False)

print(f"[INFO] 엑셀 파일로 저장 완료: {output_file}")
