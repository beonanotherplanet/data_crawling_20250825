import os
import time
import json
import random
import pandas as pd
from datetime import datetime, timezone
from typing import Dict
from googleapiclient.discovery import build
import googleapiclient.errors

# -----------------------------
# CONFIGURATION
# -----------------------------
API_KEY = ""
# CHANNEL_ID = "UCF4Wxdo3inmxP-Y59wXDsFw"  # MBC
# CHANNEL_ID = "UCkinYTS9IHqOEwR1Sze2JTw"  # SBS
# CHANNEL_ID = "UCkinYTS9IHqOEwR1Sze2JTw"  # SBS
# CHANNEL_ID = "UCBi2mrWuNuyYy4gbM6fU18Q"  # ABC
# CHANNEL_ID = "UCeY0bbntWzzVIaj2z3QigXg"  # NBC
CHANNEL_ID = "UC8p1vwvWtl6T73JiExfWs1g"  # CBS

# 저장 파일명: 채널 ID 반영
SAVE_FILE = f"playlist_videos_{CHANNEL_ID}_2015_2022.xlsx"
TEMP_FILE = "temp_us.json"
PLAYLISTS_JSON = f"playlists_{CHANNEL_ID}.json"  # (선택) 재생목록 스냅샷 저장

KOREAN_KEYWORDS = [
    '기후', '기후변화', '기후위기', '온난화', '탄소', '온실가스', '해수면', '이상기후',
    '종말', '살인 폭염', '이상기온', '이례적', '유례없는', '극한', '인류',
    '역사상', '펄펄', '최악의 더위', '북극', '열대화', '엘니뇨', '라니냐', '기온 급상승', '수온', '재생'
]  # 중복 제거(‘재앙’ 중복 제거)

ENG_KEYWORDS = [
    'climate', 'warming', 'carbon', 'carbon dioxide', 'renewable',
    'sea level', 'heat wave', 'extreme weather', 'extinction', 'record-breaking',
    'historic high', 'unusual weather', 'freak weather', 'ecosystem', 'greenhouse gas',
    'abnormal weather', 'unusual weather', 'scorching', 'Arctic', 'El Niño', 'La Niña', 'temperature'
]

# 타임존 포함(UTC aware)로 통일
START_DATE = datetime(2015, 8, 1, tzinfo=timezone.utc)
END_DATE   = datetime(2025, 8, 1, tzinfo=timezone.utc)

# -----------------------------
# UTIL / HELPERS
# -----------------------------
def backoff_sleep(attempt: int):
    """지수 백오프(최대 5초)"""
    delay = min(5.0, (2 ** attempt) * 0.5) + random.uniform(0, 0.3)
    time.sleep(delay)

def load_existing_data() -> Dict[str, dict]:
    if os.path.exists(TEMP_FILE):
        with open(TEMP_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_temp_data(data: Dict[str, dict]):
    with open(TEMP_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[INFO] Temp data saved ({len(data)} videos)")

def save_final_data(data: Dict[str, dict]):
    if data:
        df = pd.DataFrame(list(data.values()))
        # 안전하게 datetime 파싱(UTC)
        df["UploadDate"] = pd.to_datetime(df["UploadDate"], utc=True, errors="coerce")
        df.sort_values("UploadDate", inplace=True)
        df.to_excel(SAVE_FILE, index=False)
        print(f"[INFO] Final data saved to {SAVE_FILE}")
    else:
        print("[INFO] No data to save.")

def keyword_filter(title: str) -> bool:
    t = title.lower()
    return any(kw.lower() in t for kw in ENG_KEYWORDS)

def pretty_print_playlists(playlists: Dict[str, str]):
    """코드 복붙하기 좋게 콘솔에 출력"""
    print("\nPLAYLISTS = {")
    for pid, title in playlists.items():
        safe_title = title.replace('"', '\\"')
        print(f'    "{pid}": "{safe_title}",')
    print("}\n")

def save_playlists_json(playlists: Dict[str, str]):
    with open(PLAYLISTS_JSON, "w", encoding="utf-8") as f:
        json.dump(playlists, f, ensure_ascii=False, indent=2)
    print(f"[INFO] Playlists saved to {PLAYLISTS_JSON}")

# -----------------------------
# DISCOVERY
# -----------------------------
def get_all_playlists(api_key: str, channel_id: str) -> Dict[str, str]:
    youtube = build("youtube", "v3", developerKey=api_key)
    playlists: Dict[str, str] = {}
    next_page_token = None
    attempt = 0

    while True:
        try:
            res = youtube.playlists().list(
                part="id,snippet",
                channelId=channel_id,
                maxResults=50,
                pageToken=next_page_token
            ).execute()

            for item in res.get("items", []):
                pid = item["id"]
                title = item["snippet"]["title"]
                playlists[pid] = title

            next_page_token = res.get("nextPageToken")
            if not next_page_token:
                break

            time.sleep(0.25 + random.uniform(0, 0.2))
            attempt = 0  # 성공했으면 재시도 카운터 초기화

        except googleapiclient.errors.HttpError as e:
            # rateLimitExceeded 등일 수 있음 → 지수 백오프 후 재시도
            attempt += 1
            if attempt > 5:
                print(f"[ERROR] playlists.list repeated failure: {e}")
                break
            print(f"[WARN] HttpError on playlists.list (attempt {attempt}): {e}")
            backoff_sleep(attempt)
        except Exception as e:
            print(f"[ERROR] Unexpected in get_all_playlists: {e}")
            break

    return playlists

# -----------------------------
# CORE
# -----------------------------
def fetch_from_playlist(youtube, playlist_id: str, playlist_name: str, existing_data: dict) -> int:
    total_new = 0
    next_page_token = None
    attempt = 0

    while True:
        try:
            res = youtube.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token,
            ).execute()

            items = res.get("items", [])

            for item in items:
                snippet = item.get("snippet", {}) or {}
                cdet    = item.get("contentDetails", {}) or {}

                # videoId
                vid = cdet.get("videoId") or snippet.get("resourceId", {}).get("videoId")
                if not vid:
                    continue

                # 제목/채널명
                title = snippet.get("title", "")
                channel_title = snippet.get("channelTitle") or playlist_name

                # 업로드 시각(가능하면 contentDetails.videoPublishedAt 사용)
                published_at = cdet.get("videoPublishedAt") or snippet.get("publishedAt")
                if not published_at:
                    continue  # private/unavailable 등

                # RFC3339 → aware datetime(UTC)
                dt = datetime.fromisoformat(published_at.replace("Z", "+00:00"))

                # 날짜/키워드 필터
                if not (START_DATE <= dt <= END_DATE):
                    continue
                if not keyword_filter(title):
                    continue

                if vid not in existing_data:
                    existing_data[vid] = {
                        "Video URL": f"https://www.youtube.com/watch?v={vid}",
                        "Title": title,
                        "Channel": channel_title,
                        "UploadDate": dt.isoformat(),
                        "SourcePlaylist": playlist_name,
                        "PlaylistId": playlist_id,
                    }
                    total_new += 1
                    print(f"[MATCH] {playlist_name} | {dt.isoformat()} | {title}")

            next_page_token = res.get("nextPageToken")
            if not next_page_token:
                break

            time.sleep(0.25 + random.uniform(0, 0.35))
            attempt = 0  # 성공시 재시도 카운터 초기화

        except googleapiclient.errors.HttpError as e:
            attempt += 1
            if attempt > 5:
                print(f"[ERROR] playlistItems.list repeated failure: {e}")
                break
            print(f"[WARN] HttpError on playlistItems.list (attempt {attempt}): {e}")
            backoff_sleep(attempt)
        except Exception as e:
            print(f"[ERROR] Unexpected in fetch_from_playlist: {e}")
            break

    return total_new

# -----------------------------
# MAIN
# -----------------------------
def main():
    # 1) 재생목록 수집
    playlists = get_all_playlists(API_KEY, CHANNEL_ID)

    # 2) 터미널에 보기 좋게 출력(복붙용)
    pretty_print_playlists(playlists)

    # (선택) JSON으로 저장
    save_playlists_json(playlists)

    # 3) 재생목록 순회하며 영상 수집
    youtube = build("youtube", "v3", developerKey=API_KEY)
    existing_data = load_existing_data()
    total_new = 0

    for pid, pname in playlists.items():
        print(f"[INFO] Processing playlist: {pname} ({pid})")
        added = fetch_from_playlist(youtube, pid, pname, existing_data)
        total_new += added
        save_temp_data(existing_data)

    print(f"[INFO] Completed. Total new videos added: {total_new}")
    save_final_data(existing_data)

if __name__ == "__main__":
    main()
