# pip install requests pandas openpyxl python-dateutil
import os
import time
import requests
import pandas as pd
from urllib.parse import urlencode
from dateutil import parser as dtparser

BASE = "https://searchapi.news.sbs.co.kr/search/news"

# === 수집 설정 ===
START_DATE = "2015-08-01"
END_DATE   = "2025-08-01"
SECTION_CD = "01,02,03,07,08,09,14"     # 필요시 조정
SEARCH_FIELD = "all"
COLLECTION = "news_sbs"
PAGE_SIZE = 100
SLEEP = 0.25

OUTPUT_XLSX = f"sbs_titles_{START_DATE}_to_{END_DATE}.xlsx"

KEYWORDS = [
    '기후', '기후변화', '기후위기','온난화','탄소', '온실가스', '해수면', '이상기후',
    '재앙', '종말', '살인 폭염', '이상기온', '이례적', '유례없는', '극한', '재앙',
    '인류', '역사상', '펄펄', '최악의 더위', '북극', '열대화', '엘니뇨', '라니냐', '기온 급상승', '수온', '재생'
]

# 요청 헤더(간단 브라우저 흉내)
HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept": "application/json",
    "Referer": "https://news.sbs.co.kr/news/search/main.do",
    "Origin": "https://news.sbs.co.kr",
}

def normalize_date(s: str) -> str | None:
    if not s:
        return None
    s = str(s).strip().replace(".", "-")
    try:
        return pd.to_datetime(s, errors="raise", utc=True).date().isoformat()
    except Exception:
        # 다른 필드로 재시도할 수 있게 None
        return None

def fetch_page(query: str, offset: int, limit: int = PAGE_SIZE) -> dict:
    params = {
        "query": query,
        "startDate": START_DATE,
        "endDate": END_DATE,
        "searchField": SEARCH_FIELD,
        "sectionCd": SECTION_CD,
        "collection": COLLECTION,
        "offset": offset,
        "limit": limit,
        # "sort": "date.desc",  # 필요 시 정렬 지정
    }
    url = f"{BASE}?{urlencode(params, safe=',')}"
    r = requests.get(url, headers=HEADERS, timeout=20)
    r.raise_for_status()
    return r.json()

def extract_rows(payload: dict):
    items = payload.get("news_sbs", []) or []
    rows = []
    for it in items:
        title = (it.get("TITLE") or "").strip()
        d = normalize_date(it.get("DATE") or it.get("EDIT_DATE") or "")
        # 링크가 직접 없으면 DOCID로 구성
        link = it.get("URL") or it.get("link") or ""
        if not link and it.get("DOCID"):
            link = f"https://news.sbs.co.kr/news/endPage.do?news_id={it['DOCID']}"
        rows.append({"title": title, "published": d, "link": link})
    return rows

def crawl_one_keyword(query: str) -> pd.DataFrame:
    all_rows = []
    offset = 0
    while True:
        payload = fetch_page(query, offset)
        rows = extract_rows(payload)
        if not rows:
            break
        all_rows.extend(rows)
        offset += PAGE_SIZE

        total = payload.get("total") or payload.get("numFound")
        if total is not None and offset >= int(total):
            break
        time.sleep(SLEEP)

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df

    # 날짜 파싱 및 기간 최종 보정
    df["published_dt"] = pd.to_datetime(df["published"], errors="coerce", utc=True)
    start = pd.Timestamp(START_DATE, tz="UTC")
    end   = pd.Timestamp(END_DATE + " 23:59:59", tz="UTC")
    df = df[(df["published_dt"] >= start) & (df["published_dt"] <= end)]

    # 보기 좋게 정리
    df["published"] = df["published_dt"].dt.strftime("%Y-%m-%d")
    df.drop(columns=["published_dt"], inplace=True)
    df["keyword"] = query

    # 중복 제거(링크 기준)
    if "link" in df.columns:
        df = df.drop_duplicates(subset=["link"])
    else:
        df = df.drop_duplicates(subset=["title", "published"])

    # 정렬
    df = df.sort_values(["published", "title"], ascending=[False, True])
    return df[["keyword", "title", "published", "link"]]

def append_to_excel(path: str, df_new: pd.DataFrame):
    """기존 엑셀과 합쳐 중복 제거 후 저장(안전한 append)."""
    if os.path.exists(path):
        try:
            df_old = pd.read_excel(path, dtype=str)
        except Exception:
            df_old = pd.DataFrame(columns=["keyword", "title", "published", "link"])
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new.copy()

    # 중복 제거(링크 우선)
    if "link" in df_all.columns:
        df_all = df_all.drop_duplicates(subset=["link"])
    else:
        df_all = df_all.drop_duplicates(subset=["title", "published"])

    # 정렬 및 저장
    # published가 문자열일 수 있으니 정렬용 컬럼 잠시 생성
    dt = pd.to_datetime(df_all["published"], errors="coerce", utc=True)
    df_all = df_all.assign(_dt=dt).sort_values(["_dt", "title"], ascending=[False, True]).drop(columns=["_dt"])
    df_all.to_excel(path, index=False)
    return len(df_all)

if __name__ == "__main__":
    total_before = 0
    if os.path.exists(OUTPUT_XLSX):
        try:
            total_before = len(pd.read_excel(OUTPUT_XLSX))
        except Exception:
            total_before = 0
    print(f"[INFO] starting… existing rows in excel: {total_before}")

    for kw in KEYWORDS:
        print(f"[INFO] crawling keyword: {kw}")
        df_kw = crawl_one_keyword(kw)
        print(f"  - fetched rows: {len(df_kw)}")
        if not df_kw.empty:
            total_now = append_to_excel(OUTPUT_XLSX, df_kw)
            print(f"  - saved. total rows in excel: {total_now}")
        else:
            print("  - no rows")

    print("[INFO] done.")
