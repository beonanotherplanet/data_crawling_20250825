# pip install requests pandas openpyxl urllib3
import os, re, json, time, random
from datetime import datetime, timedelta
from urllib.parse import urlencode

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE = "https://searchapi.imnews.imbc.com/search"

KEYWORDS = [
    '기후', '기후변화', '기후위기','온난화','탄소', '온실가스', '해수면', '이상기후',
    '재앙', '종말', '살인 폭염', '이상기온', '이례적', '유례없는', '극한', '재앙',
    '인류', '역사상', '펄펄', '최악의 더위', '북극', '열대화', '엘니뇨', '라니냐', '기온 급상승', '수온', '재생'
]

GLOBAL_START = "20150801"
GLOBAL_END   = "20250801"
PAGESIZE     = 100
REQ_SLEEP    = 0.08
KW_COOLDOWN  = (0.5, 1.2)
OUTPUT_XLSX  = f"mbc_titles_{GLOBAL_START}_{GLOBAL_END}.xlsx"

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/139.0 Safari/537.36",
    "Accept": "*/*",
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8",
    "Referer": "https://imnews.imbc.com/",
    "Origin": "https://imnews.imbc.com",
    "Connection": "keep-alive",
}

# ---------------- utils ----------------
def log(msg: str):
    print(msg, flush=True)

def make_session():
    s = requests.Session()
    retries = Retry(
        total=5, connect=5, read=5,
        backoff_factor=0.4,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
    )
    s.mount("https://", HTTPAdapter(max_retries=retries))
    s.headers.update(HEADERS)
    return s

SESSION = make_session()

JSONP_RE = re.compile(r'^[\w$]+\((.*)\)\s*;?\s*$', re.S)
def parse_jsonp(text: str) -> dict:
    s = text.strip()
    if not s:
        return {}
    m = JSONP_RE.match(s)
    if m:
        s = m.group(1)
    try:
        return json.loads(s)
    except Exception:
        return {}

def now_ms():
    return int(time.time() * 1000)

def fetch_page(keyword: str, page: int, startdate: str, enddate: str) -> dict:
    params = {
        "callback": f"search_{now_ms()}",
        "query": keyword,
        "page": page,               # 0,1,2,...
        "pagesize": PAGESIZE,
        "sorttype": "date",
        "startdate": startdate,     # YYYYMMDD
        "enddate": enddate,         # YYYYMMDD
        "_": now_ms(),
    }
    url = f"{BASE}?{urlencode(params)}"
    r = SESSION.get(url, timeout=(4, 10))
    log(f"[PAGE] kw='{keyword}' {startdate}~{enddate} p={page} -> HTTP {r.status_code} len={len(r.text)}")
    r.raise_for_status()
    return parse_jsonp(r.text)

def month_ranges(start_yyyymmdd: str, end_yyyymmdd: str):
    sd = datetime.strptime(start_yyyymmdd, "%Y%m%d").date()
    ed = datetime.strptime(end_yyyymmdd,   "%Y%m%d").date()
    cur = sd.replace(day=1)
    while cur <= ed:
        nxt = (cur.replace(day=28) + timedelta(days=4)).replace(day=1)  # next month 1st
        last = nxt - timedelta(days=1)
        s = max(sd, cur)
        e = min(ed, last)
        yield s.strftime("%Y%m%d"), e.strftime("%Y%m%d")
        cur = nxt

def extract_rows(payload: dict, keyword: str):
    result = payload.get("result", {}) or {}
    rows = result.get("rows", []) or []
    out = []
    for row in rows:
        f = row.get("fields", {}) or {}
        title = (f.get("artsubject") or "").strip()
        if not title:
            continue
        operday = (f.get("operday") or "").strip()  # "YYYYMMDDHHMMSS"
        pub = None
        if len(operday) >= 8 and operday[:8].isdigit():
            pub = f"{operday[:4]}-{operday[4:6]}-{operday[6:8]}"
        linkurl = (f.get("linkurl") or "").strip()
        link = f"https://imnews.imbc.com{linkurl}" if linkurl.startswith("/") else linkurl
        artid = (f.get("artid") or "").strip()
        out.append({
            "keyword": keyword,
            "title": title,
            "published": pub,
            "link": link,
            "artid": artid or None,
        })
    return out

def crawl_keyword(keyword: str) -> pd.DataFrame:
    collected = []
    total_added = 0
    slice_idx = 0

    for s, e in month_ranges(GLOBAL_START, GLOBAL_END):
        slice_idx += 1
        log(f"[SLICE] kw='{keyword}' slice#{slice_idx} {s}~{e}")

        page = 0
        empty_hits = 0
        added_this_slice = 0

        while True:
            try:
                payload = fetch_page(keyword, page, s, e)
            except Exception as e:
                log(f"[WARN] request error at p={page}: {e}")
                empty_hits += 1
                if empty_hits >= 2:
                    break
                page += 1
                time.sleep(REQ_SLEEP)
                continue

            rows = extract_rows(payload, keyword)
            got = len(rows)
            log(f"  └─ got={got} rows on page {page}")
            if got == 0:
                empty_hits += 1
                if empty_hits >= 2:
                    log(f"  └─ stop slice (two empty pages)")
                    break
            else:
                collected.extend(rows)
                total_added += got
                added_this_slice += got
                empty_hits = 0

            page += 1
            if page >= 1500:   # 안전장치
                log("  └─ stop slice (page cap reached)")
                break
            time.sleep(REQ_SLEEP)

        log(f"[SLICE-END] kw='{keyword}' {s}~{e} added={added_this_slice}, total={total_added}")
        time.sleep(REQ_SLEEP)

    df = pd.DataFrame(collected)
    if df.empty:
        log(f"[KEYWORD-END] kw='{keyword}' -> 0 rows")
        return df

    # dedup: artid → link → (title,published)
    if "artid" in df.columns:
        before = len(df)
        df = df.drop_duplicates(subset=["artid"], keep="first")
        log(f"[DEDUP] by artid: {before} -> {len(df)}")
    df["link"] = df["link"].astype(str).str.strip()
    before = len(df)
    df = df.drop_duplicates(subset=["link"])
    log(f"[DEDUP] by link : {before} -> {len(df)}")
    before = len(df)
    df = df.drop_duplicates(subset=["title", "published"])
    log(f"[DEDUP] by (title,published): {before} -> {len(df)}")

    df["published_dt"] = pd.to_datetime(df["published"], errors="coerce", utc=True)
    df = df.sort_values(["published_dt", "title"], ascending=[False, True])
    df["published"] = df["published_dt"].dt.strftime("%Y-%m-%d")
    df = df.drop(columns=["published_dt"])

    log(f"[KEYWORD-END] kw='{keyword}' final_rows={len(df)}")
    return df[["keyword", "title", "published", "link", "artid"]]

# ---------------- main ----------------
if __name__ == "__main__":
    log(f"[START] saving to {OUTPUT_XLSX}")
    all_df = []
    for kw in KEYWORDS:
        log(f"[INFO] crawling: {kw}")
        try:
            d = crawl_keyword(kw)
            log(f"[INFO] kw='{kw}' fetched={len(d)}")
            if not d.empty:
                all_df.append(d)
        except Exception as e:
            log(f"[ERROR] kw='{kw}' failed: {e}")
        time.sleep(random.uniform(*KW_COOLDOWN))

    if all_df:
        out = pd.concat(all_df, ignore_index=True)
        # 전역 dedup 한 번 더
        if "artid" in out.columns:
            out = out.drop_duplicates(subset=["artid"])
        out["link"] = out["link"].astype(str).str.strip()
        out = out.drop_duplicates(subset=["link"])
        out = out.drop_duplicates(subset=["title", "published"])

        out.to_excel(OUTPUT_XLSX, index=False)
        log(f"[DONE] saved {len(out)} rows -> {OUTPUT_XLSX}")
    else:
        log("[DONE] no results")
