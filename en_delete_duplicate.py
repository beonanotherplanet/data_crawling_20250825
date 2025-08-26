import pandas as pd

# 엑셀 파일 경로
INPUT_XLSX  = "final_nbc_websearch_2015-08-01_2025-08-01.xlsx"   # 원본 파일
OUTPUT_XLSX = "nbc_websearch_2015-08-01_2025-08-01_FILTERED.xlsx" # 결과 저장

# 키워드 리스트
KEYWORDS = [
    'climate', 'warming', 'carbon', 'carbon dioxide', 'renewable',
    'sea level', 'heat wave', 'extreme weather', 'extinction', 'record-breaking',
    'historic high', 'unusual weather', 'freak weather', 'ecosystem', 'greenhouse gas',
    'abnormal weather', 'unusual weather', 'scorching', 'Arctic',
    'El Niño', 'El Nino', 'La Niña', 'La Nina',
    'temperature'
]

# 1) 엑셀 불러오기
df = pd.read_excel(INPUT_XLSX, dtype=str)

# 2) title_final이 문자열이 되도록 변환
df["title_final"] = df["title_final"].astype(str)

# 3) title_final에 키워드가 하나라도 포함되어 있는지 체크
mask = df["title_final"].str.contains(
    "|".join([kw.lower() for kw in KEYWORDS]),  # 여러 키워드 OR 조건
    case=False,  # 대소문자 무시
    na=False     # NaN은 False 처리
)

# 4) 조건을 만족하는 행만 남기기
df_filtered = df[mask].copy()

# 5) 저장
df_filtered.to_excel(OUTPUT_XLSX, index=False)

print(f"[INFO] 원본 {len(df)}행 → 필터링 후 {len(df_filtered)}행 저장 완료: {OUTPUT_XLSX}")
