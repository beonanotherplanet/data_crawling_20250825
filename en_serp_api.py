# pip install requests pandas python-dateutil openpyxl
import os, time, random, math
import requests, pandas as pd
from datetime import datetime, timezone
from dateutil import parser as du

API_KEY = "" # serpApi apk key
DOMAIN  = "abcnews.go.com"  # ← CBS: cbsnews.com, NBC: nbcnews.com

START = datetime(2015, 8, 1, tzinfo=timezone.utc)
END   = datetime(2025, 8, 1, tzinfo=timezone.utc)

KEYWORDS = [
    'climate', 'warming', 'carbon', 'carbon dioxide', 'renewable',
    'sea level', 'heat wave', 'extreme weather', 'extinction', 'record-breaking',
    'historic high', 'unusual weather', 'freak weather', 'ecosystem', 'greenhouse gas',
    'abnormal weather', 'unusual weather', 'scorching', 'Arctic',
    'El Niño','El Nino','La Niña','La Nina',
    'temperature'
]

# 중복 제거
KEYWORDS = list(dict.fromkeys(KEYWORDS))

BASE = "https://serpapi.com/search.json"
HEADERS = {"User-Agent": "Mozilla/5.0"}

def mdy(d: datetime) -> str:
    return d.strftime("%m/%d/%Y").lstrip("0").replace("/0", "/")

def normalize_date(s: str | None):
    if not s: return None
    if "ago" in s.lower():  # 상대표현은 건너뜀
        return None
    try:
        dt = du.parse(s)
        if dt.tzinfo is None: dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def fetch_google(q: str, start_idx: int = 0):
    """
    SerpAPI engine=google (웹검색).
    start_idx: 0, 10, 20 ... (구글은 보통 100~200 사이가 실용 한계)
    """
    params = {
        "engine": "google",
        "q": q,
        "gl": "us",
        "hl": "en",
        "api_key": API_KEY,
        "start": start_idx,   # 10단위
        "num": 10,            # 고정
        "tbs": None,          # 아래에서 연도별로 세팅
    }
    r = requests.get(BASE, params=params, headers=HEADERS, timeout=30)
    r.raise_for_status()
    return r.json()

def crawl_year(year: int):
    y_start = datetime(year, 1, 1, tzinfo=timezone.utc)
    y_end   = datetime(year, 12, 31, tzinfo=timezone.utc)
    # 경계년도 보정
    if year == START.year and y_start < START: y_start = START
    if year == END.year and y_end > END:       y_end = END

    # 기간 필터 (Custom Date Range; 웹검색도 지원)
    tbs = f"cdr:1,cd_min:{mdy(y_start)},cd_max:{mdy(y_end)}"

    # 제목에 1개 이상 포함: intitle:(kw1|kw2|...)
    # 괄호/공백 포함 키워드는 큰따옴표로 감싸주자
    def quote(k): return f'"{k}"' if " " in k or "ñ" in k.lower() else k
    or_expr = " | ".join(quote(k) for k in KEYWORDS)
    q = f'site:{DOMAIN} intitle:({or_expr})'

    seen = set()
    rows = []

    # start=0,10,20... 페이지네이션 (너무 깊이 들어가면 의미없음 → 200 정도 제한)
    for start_idx in range(0, 200, 10):
        payload = fetch_google(q, start_idx=start_idx)
        # tbs는 params로 못 넣으니 q에 붙이지 말고, serpapi google의 'tbs' 파라미터 지원을 직접 추가
        # -> 일부 계정에선 전달됨. 안전하게는 다음처럼 재요청:
        payload = requests.get(BASE, params={
            "engine":"google","q":q,"gl":"us","hl":"en","api_key":API_KEY,
            "start":start_idx,"num":10,"tbs":tbs
        }, headers=HEADERS, timeout=30).json()

        organic = payload.get("organic_results") or []
        if not organic:
            break

        for it in organic:
            link  = (it.get("link") or "").strip()
            title = (it.get("title") or "").strip()
            date_raw = (it.get("date") or "").strip()  # e.g., "Aug 1, 2017"

            if not link or not title:
                continue
            if link in seen:
                continue
            seen.add(link)

            dt = normalize_date(date_raw)
            if dt is not None and not (y_start <= dt <= y_end):
                continue

            rows.append({
                "site": DOMAIN,
                "title": title,
                "published": dt.strftime("%Y-%m-%d") if dt else None,
                "published_raw": date_raw,
                "link": link,
                "year_bucket": year,
            })

        time.sleep(random.uniform(0.4, 0.8))

    return rows

if __name__ == "__main__":
    if not API_KEY or API_KEY == "PUT_YOUR_KEY_HERE":
        raise SystemExit("❌ SERPAPI_API_KEY 설정이 필요합니다.")

    all_rows = []
    for y in range(START.year, END.year + 1):
        print(f"[INFO] year {y}…", flush=True)
        all_rows.extend(crawl_year(y))

    df = pd.DataFrame(all_rows)
    if df.empty:
        print("[INFO] no results")
    else:
        # 전역 dedup
        df["link"] = df["link"].astype(str).str.strip()
        df = df.drop_duplicates(subset=["link"])

        # 정렬
        dt = pd.to_datetime(df["published"], errors="coerce", utc=True)
        df = df.assign(_dt=dt).sort_values(["_dt","title"], ascending=[False,True]).drop(columns=["_dt"])

        out = f"nbc_websearch_{START.date()}_{END.date()}.xlsx"
        df.to_excel(out, index=False)
        print(f"[DONE] saved {len(df)} rows -> {out}")
