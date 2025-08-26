import re, json, time, random, pandas as pd, requests
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

INPUT_XLSX  = "abc_websearch_2015-08-01_2025-08-01.xlsx"  # 너의 파일명으로 교체
OUTPUT_XLSX = "abc_websearch_2015-08-01_2025-08-01_FIXED.xlsx"

TITLE_COL = "title"
LINK_COL  = "link"

# 너무 많을 때 나눠서 돌리고 싶으면 숫자 지정 (예: 300). 전체 돌리려면 None.
LIMIT_PER_RUN = None
SLEEP_RANGE   = (0.25, 0.6)  # 요청 간 랜덤 딜레이

ELLIPSIS_RE = re.compile(r"(…|\.{3})")

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
    s.headers.update({
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9,ko;q=0.7",
        "Connection": "keep-alive",
    })
    return s

SESSION = make_session()

def pick_best_title(soup: BeautifulSoup) -> str | None:
    # 1) og:title / twitter:title
    for key, attr in [("og:title","property"), ("twitter:title","name")]:
        m = soup.find("meta", attrs={attr: key})
        if m and m.get("content"):
            t = m["content"].strip()
            if t: return t
    # 2) JSON-LD headline/name
    for tag in soup.find_all("script", type=lambda v: v and "ld+json" in v.lower()):
        try:
            data = json.loads(tag.string or "")
        except Exception:
            continue
        candidates = data if isinstance(data, list) else [data]
        for obj in candidates:
            if not isinstance(obj, dict):
                continue
            t = obj.get("headline") or obj.get("name")
            if isinstance(t, str) and t.strip():
                return t.strip()
    # 3) 본문 H1
    h1 = soup.find("h1")
    if h1:
        t = h1.get_text(" ", strip=True)
        if t: return t
    # 4) <title>
    if soup.title and soup.title.string:
        return soup.title.string.strip()
    return None

def fetch_full_title(url: str, timeout=(6, 15)) -> str | None:
    try:
        r = SESSION.get(url, timeout=timeout, allow_redirects=True)
        r.raise_for_status()
    except Exception:
        return None
    soup = BeautifulSoup(r.text, "lxml")
    t = pick_best_title(soup)
    if not t:
        return None
    # 공백 정리
    t = re.sub(r"\s+", " ", t).strip()
    return t

def fix_ellipsis_in_excel(in_path: str, out_path: str,
                          title_col: str = TITLE_COL, link_col: str = LINK_COL,
                          limit: int | None = LIMIT_PER_RUN):
    df = pd.read_excel(in_path, dtype=str)

    if title_col not in df.columns or link_col not in df.columns:
        raise ValueError(f"엑셀에 '{title_col}' 또는 '{link_col}' 컬럼이 없습니다.")

    df[title_col] = df[title_col].astype(str)
    df[link_col]  = df[link_col].astype(str)

    # .../… 로 끝나는 행만 대상
    target_idx = df[df[title_col].str.contains(ELLIPSIS_RE, na=False)].index.tolist()

    if limit:
        target_idx = target_idx[:limit]

    fixed = 0
    new_titles = []

    for i, idx in enumerate(target_idx, 1):
        old_title = df.at[idx, title_col]
        url = df.at[idx, link_col]
        if not url:
            new_titles.append(None)
            continue

        new_title = fetch_full_title(url)
        if new_title and not ELLIPSIS_RE.search(new_title):
            df.at[idx, title_col] = new_title       # ← 덮어쓰기 (원하면 주석 처리)
            df.at[idx, "title_fixed"] = new_title   # ← 보조 컬럼에도 저장
            fixed += 1
            print(f"[{i}/{len(target_idx)}] FIXED  {old_title!r} -> {new_title!r}")
        else:
            print(f"[{i}/{len(target_idx)}] SKIP   {old_title!r}")
        time.sleep(random.uniform(*SLEEP_RANGE))

    # 최종 title_final(있으면 고정본, 없으면 기존)
    if "title_fixed" in df.columns:
        df["title_final"] = df["title_fixed"].fillna(df[title_col])
    else:
        df["title_final"] = df[title_col]

    df.to_excel(out_path, index=False)
    print(f"[INFO] rows: {len(df)} | candidates: {len(target_idx)} | fixed: {fixed}")
    print(f"[DONE] saved -> {out_path}")

if __name__ == "__main__":
    fix_ellipsis_in_excel(INPUT_XLSX, OUTPUT_XLSX)
