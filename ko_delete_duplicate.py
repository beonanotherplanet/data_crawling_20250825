# pip install pandas openpyxl
import re
import pandas as pd

INPUT_XLSX  = "mbc_titles_20150801_20250801.xlsx"   # 원본 파일명
OUTPUT_XLSX = "mbc_titles_20150801_20250801_filtered.xlsx"  # 저장 파일명

KEYWORDS = [
    '기후', '기후변화', '기후위기','온난화','탄소', '온실가스', '해수면', '이상기후',
    '재앙', '종말', '살인 폭염', '이상기온', '이례적', '유례없는', '극한', '재앙',
    '인류', '역사상', '펄펄', '최악의 더위', '북극', '열대화', '엘니뇨', '라니냐', '기온 급상승', '수온', '재생'
]

def build_pattern(keywords):
    # 중복 제거 + 공백 유연 매칭(키워드 중간 공백을 \s* 로 치환)
    uniq = list(dict.fromkeys(k.strip() for k in keywords if k and k.strip()))
    parts = []
    for kw in uniq:
        esc = re.escape(kw)
        # esc에는 공백이 ' '로 들어있으니 이를 \s+ (또는 \s*)로 바꿔 유연 매칭
        esc = esc.replace(r"\ ", r"\s+")
        parts.append(esc)
    # OR 패턴으로 결합
    return r"(?:%s)" % "|".join(parts)

if __name__ == "__main__":
    df = pd.read_excel(INPUT_XLSX, dtype=str)

    if "title" not in df.columns:
        raise ValueError("엑셀에 'title' 컬럼이 없습니다.")

    # title 전처리(문자열화)
    df["title"] = df["title"].astype(str)

    # 키워드 패턴 생성 (대소문자 무시, 공백 유연)
    pattern = build_pattern(KEYWORDS)

    # 키워드 포함(True)만 남김
    mask = df["title"].str.contains(pattern, regex=True, na=False, flags=re.IGNORECASE)
    df_filtered = df[mask].copy()

    # 저장
    df_filtered.to_excel(OUTPUT_XLSX, index=False)

    print(f"[INFO] input rows: {len(df)}")
    print(f"[INFO] kept rows : {len(df_filtered)}")
    print(f"[INFO] saved to  : {OUTPUT_XLSX}")
