# pip install requests pandas openpyxl urllib3
import os
import re
import time
import json
import random
from datetime import datetime, timedelta
from urllib.parse import urlencode

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import pandas as pd

# ============================ 설정 ============================
BASE = "https://reco.kbs.co.kr/v2/search"

GLOBAL_START = "2015.08.01"     # YYYY.MM.DD
GLOBAL_END   = "2025.08.01"     # YYYY.MM.DD

PAGE_SIZE  = 100                # 가급적 크게
SLEEP_REQ  = 0.15               # 요청 간 딜레이(초)
SLEEP_SLICE= 0.25               # 슬라이스 경계 딜레이
KW_COOLDOWN = (2.0, 4.0)        # 키워드 간 쿨다운(2~4초 랜덤)

OUTPUT_XLSX = f"kbs_titles_{GLOBAL_START}_to_{GLOBAL_END}.xlsx"

KEYWORDS = [
    '기후', '기후변화', '기후위기','온난화','탄소', '온실가스', '해수면', '이상기후',
    '종말', '살인 폭염', '이상기온', '이례적', '유례없는', '극한',
    '인류', '역사상', '펄펄', '최악의 더위', '북극', '열대화', '엘니뇨', '라니냐', '기온 급상승', '수온', '재생'
]  # 중복 제거(‘재앙’ 삭제)

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "Referer": "https://news.kbs.co.kr/",
    "Origin": "https://news.kbs.co.kr",
    "Connection": "keep-alive",
}

# ======================= 유틸/세션/파서 =======================
def make_session() -> requests.Session:
    s = requests.Session()
    retries = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.5,                   # 0.5, 1, 2, 4, ...
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s

SESSION = make_session()

JSONP_PAYLOAD_RE = re.compile(r'\((\s*{.*}\s*)\)\s*;?\s*$', re.S)
XSSI_PREFIX_RE   = re.compile(r"^\)\]\}',?\s*")

def parse_json_or_jsonp(text: str) -> dict:
    s = text.strip()
    if not s:
        return {}
    if s.startswith("<"):                      # 차단/오류 HTML
        return {}
    m = JSONP_PAYLOAD_RE.search(s)             # JSONP → JSON
    if m:
        try:
            return json.loads(m.group(1))
        except Exception:
            return {}
    s = XSSI_PREFIX_RE.sub("", s)              # XSSI prefix 제거
    try:
        return json.loads(s)                   # 순수 JSON
    except Exception:
        return {}

def now_ms() -> int:
    return int(time.time() * 1000)

def normalize_date(rdatetime: str | None, service_time: str | None) -> str | None:
    # rdatetime: 'YYYYMMDD', service_time: 'YYYYMMDD HHMMSS'
    if rdatetime:
        s = str(rdatetime).strip()
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    if service_time:
        s = str(service_time).split()[0].strip().replace(".", "").replace("-", "")
        if len(s) == 8 and s.isdigit():
            return f"{s[:4]}-{s[4:6]}-{s[6:8]}"
    return None

# ======================= API 호출/파싱 ========================
def fetch_page(keyword: str, page: int, sdate: str, edate: str,
               tag_type: str = "m", page_size: int = PAGE_SIZE) -> dict:
    cb = f"jQuery{random.randint(10**14, 10**15-1)}_{now_ms()}"
    params = {
        "callback": cb,
        "target": "newstotal",
        "keyword": keyword,
        "page": page,
        "page_size": page_size,
        "sort_option": "date",
        "searchfield": "all",
        "categoryfield": "",
        "sdate": sdate,              # YYYY.MM.DD
        "edate": edate,              # YYYY.MM.DD
        "include": "",
        "exclude": "",
        "tag_type": tag_type,        # 'm' 또는 'w'
        "searchperiod": "custom",    # 기간 강제
        "_": now_ms(),
    }
    url = f"{BASE}?{urlencode(params)}"

    r = SESSION.get(url, timeout=(5, 12))      # (connect, read)
    print(f"[DEBUG] {keyword} {sdate}~{edate} tag={tag_type} p={page} -> {r.status_code} len={len(r.text)}", flush=True)
    r.raise_for_status()
    payload = parse_json_or_jsonp(r.text)
    return payload

def extract_rows(payload: dict, keyword: str):
    items = payload.get("data", []) or []
    total = int(payload.get("total_count", 0) or 0)
    rows = []
    for it in items:
        title = (it.get("title") or "").strip()
        link  = (it.get("target_url") or "").strip()
        date  = normalize_date(it.get("rdatetime"), it.get("service_time"))
        rows.append({"keyword": keyword, "title": title, "published": date, "link": link})
    return rows, total

# ======================= 기간 슬라이싱 ========================
def month_slices(start_str: str, end_str: str):
    """[start, end]를 월 단위로 쪼갠다 (YYYY.MM.DD 입력/출력)."""
    start = datetime.strptime(start_str, "%Y.%m.%d").date()
    end   = datetime.strptime(end_str,   "%Y.%m.%d").date()
    cur = start.replace(day=1)
    while cur <= end:
        # 다음 달 1일
        next_month = cur.replace(year=cur.year + (cur.month // 12),
                                 month=1 if cur.month == 12 else cur.month + 1,
                                 day=1)
        last_day = next_month - timedelta(days=1)
        s = max(cur, start)
        e = min(last_day, end)
        yield s.strftime("%Y.%m.%d"), e.strftime("%Y.%m.%d")
        cur = next_month

# ========================= 크롤 로직 =========================
def crawl_one_keyword(keyword: str) -> pd.DataFrame:
    all_rows = []
    for sdate, edate in month_slices(GLOBAL_START, GLOBAL_END):
        for tag in ("m", "w"):  # 모바일/웹 모두 시도(결과폭 상이할 수 있음)
            page = 1
            total_hint = None
            cap_warned = False

            while True:
                try:
                    payload = fetch_page(keyword, page, sdate, edate, tag)
                except Exception as e:
                    print(f"[WARN] request error: {e}", flush=True)
                    break

                rows, total = extract_rows(payload, keyword)

                if total_hint is None:
                    total_hint = total
                    if total_hint and total_hint <= 200 and not cap_warned:
                        print(f"[WARN] possible cap: total_count={total_hint} ({keyword} {sdate}~{edate} tag={tag})", flush=True)
                        cap_warned = True

                if not rows:
                    break

                all_rows.extend(rows)
                page += 1

                if total_hint and (page - 1) * PAGE_SIZE >= total_hint:
                    break

                time.sleep(SLEEP_REQ)
            time.sleep(SLEEP_SLICE)

    df = pd.DataFrame(all_rows)
    if df.empty:
        return df

    # 날짜/정렬/중복 제거
    df["published_dt"] = pd.to_datetime(df["published"], errors="coerce", utc=True)
    df["published"] = df["published_dt"].dt.strftime("%Y-%m-%d")
    df.drop(columns=["published_dt"], inplace=True)

    df["link"] = df["link"].astype(str).str.strip()
    df = df[df["link"] != ""]
    df = df.drop_duplicates(subset=["link"])   # 링크 기준 전역 중복 제거

    df = df.sort_values(["published", "title"], ascending=[False, True])
    return df[["keyword", "title", "published", "link"]]

def append_to_excel(path: str, df_new: pd.DataFrame):
    cols = ["keyword", "title", "published", "link"]
    if os.path.exists(path):
        try:
            df_old = pd.read_excel(path, dtype=str)
        except Exception:
            df_old = pd.DataFrame(columns=cols)
        df_all = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_all = df_new.copy()

    df_all["link"] = df_all["link"].astype(str).str.strip()
    df_all = df_all[df_all["link"] != ""]
    df_all = df_all.drop_duplicates(subset=["link"])

    dt = pd.to_datetime(df_all["published"], errors="coerce", utc=True)
    df_all = df_all.assign(_dt=dt).sort_values(["_dt", "title"], ascending=[False, True]).drop(columns=["_dt"])
    df_all.to_excel(path, index=False)
    return len(df_all)

# ============================ 실행 ============================
if __name__ == "__main__":
    existing = 0
    if os.path.exists(OUTPUT_XLSX):
        try:
            existing = len(pd.read_excel(OUTPUT_XLSX))
        except Exception:
            pass
    print(f"[INFO] start. existing rows: {existing}", flush=True)

    total_rows = existing
    for kw in KEYWORDS:
        print(f"[INFO] crawling: {kw}", flush=True)
        try:
            df_kw = crawl_one_keyword(kw)
            print(f"  - fetched: {len(df_kw)}", flush=True)
            if not df_kw.empty:
                total_rows = append_to_excel(OUTPUT_XLSX, df_kw)
                print(f"  - saved. total rows: {total_rows}", flush=True)
            else:
                print("  - no rows", flush=True)
        except Exception as e:
            print(f"[ERROR] keyword '{kw}' failed: {e}", flush=True)
        # 키워드 간 쿨다운(레이트리밋 회피)
        time.sleep(random.uniform(*KW_COOLDOWN))

    print("[INFO] done.", flush=True)
