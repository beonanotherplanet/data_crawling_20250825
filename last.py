import os
import time
import json
import random
import pandas as pd
from datetime import datetime, timedelta
import googleapiclient.discovery
import googleapiclient.errors

# -----------------------------
# CONFIGURATION
# -----------------------------
API_KEY = ""

CHANNELS = {
    # 'UCBi2mrWuNuyYy4gbM6fU18Q': "ABC 뉴스"
    # 'UCeY0bbntWzzVIaj2z3QigXg': 'NBC 뉴스',
    #   'UC8p1vwvWtl6T73JiExfWs1g': 'CBS News',
        # "UCcQTRi69dsVYHN3exePtZ1A": "KBS 뉴스",
    # "UCF4Wxdo3inmxP-Y59wXDsFw": "MBC 뉴스",
    "UCkinYTS9IHqOEwR1Sze2JTw": "SBS 뉴스",
}

KOREAN_KEYWORDS = []

START_DATE = datetime(2015, 8, 1)
END_DATE = datetime(2015, 8, 1)

TEMP_FILE = "temp.json"
SAVE_FILE = "news_videos_kbs_1.xlsx"

# -----------------------------
# FUNCTIONS
# -----------------------------
def load_existing_data():
    if os.path.exists(TEMP_FILE):
        with open(TEMP_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_temp_data(data):
    with open(TEMP_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    print(f"[INFO] Temp data saved ({len(data)} videos)")

def save_final_data(data):
    if data:
        df = pd.DataFrame(list(data.values()))
        df.sort_values("UploadDate", inplace=True)
        df.to_excel(SAVE_FILE, index=False)
        print(f"[INFO] Final data saved to {SAVE_FILE}")
    else:
        print("[INFO] No data to save.")

def keyword_filter(title):
    title_lower = title.lower()
    for kw in KOREAN_KEYWORDS:
        if kw.lower() in title_lower:
            return True
    return False

def fetch_videos(youtube, channel_id, channel_name, existing_data):
    # 3개월 단위로 구간 쪼개기
    delta = timedelta(days=90)
    current_start = START_DATE
    total_new = 0

    while current_start < END_DATE:
        current_end = min(current_start + delta, END_DATE)
        next_page_token = None

        while True:
            try:
                time.sleep(random.uniform(0.3, 0.8))
                request = youtube.search().list(
                    part="snippet",
                    channelId=channel_id,
                    type="video",
                    order="date",
                    publishedAfter=current_start.isoformat("T") + "Z",
                    publishedBefore=current_end.isoformat("T") + "Z",
                    maxResults=50,
                    pageToken=next_page_token
                )
                response = request.execute()
                items = response.get("items", [])

                for item in items:
                    video_id = item["id"]["videoId"]
                    snippet = item["snippet"]
                    title = snippet["title"]
                    upload_date = snippet["publishedAt"].replace("Z", "")
                    url = f"https://www.youtube.com/watch?v={video_id}"

                    if video_id not in existing_data and keyword_filter(title):
                        existing_data[video_id] = {
                            "Video URL": url,
                            "Title": title,
                            "Channel": channel_name,
                            "UploadDate": upload_date
                        }
                        total_new += 1
                        print(f"[MATCH] {channel_name} | {upload_date} | {title}")

                next_page_token = response.get("nextPageToken")
                if not next_page_token:
                    break

            except googleapiclient.errors.HttpError as e:
                print(f"[ERROR] HTTP Error: {e}")
                break
            except Exception as e:
                print(f"[ERROR] Exception: {e}")
                break

        current_start = current_end

    return total_new

# -----------------------------
# MAIN
# -----------------------------
def main():
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_KEY)
    existing_data = load_existing_data()
    total_new = 0

    for channel_id, channel_name in CHANNELS.items():
        print(f"[INFO] Processing channel: {channel_name}")
        new_videos = fetch_videos(youtube, channel_id, channel_name, existing_data)
        total_new += new_videos
        save_temp_data(existing_data)

    print(f"[INFO] Crawling completed. Total new videos added: {total_new}")
    save_final_data(existing_data)

if __name__ == "__main__":
    main()
