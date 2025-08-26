import os
import time
import json
import random
import pandas as pd
from datetime import datetime
import googleapiclient.discovery
import googleapiclient.errors

# -----------------------------
# CONFIGURATION
# -----------------------------
API_KEY = ""

CHANNELS = {
    # 'UCBi2mrWuNuyYy4gbM6fU18Q': "ABC News",
    'UCeY0bbntWzzVIaj2z3QigXg': 'NBC 뉴스',
}

ENGLISH_KEYWORDS = []

START_DATE = datetime(2019, 9, 19)
END_DATE = datetime(2024, 3, 7)

TEMP_FILE = "temp.json"
SAVE_FILE = "news_videos_abc.xlsx"

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
        df.to_excel(SAVE_FILE, index=False)
        print(f"[INFO] Final data saved to {SAVE_FILE}")
    else:
        print("[INFO] No data to save.")

def keyword_filter(title):
    title_lower = title.lower()
    for kw in ENGLISH_KEYWORDS:
        if kw.lower() in title_lower:
            return True
    return False

def get_uploads_playlist_id(youtube, channel_id):
    try:
        request = youtube.channels().list(
            part="contentDetails",
            id=channel_id
        )
        response = request.execute()
        return response["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]
    except Exception as e:
        print(f"[ERROR] Cannot get uploads playlist for channel {channel_id}: {e}")
        return None

def fetch_playlist_videos(youtube, playlist_id, channel_name, existing_data):
    next_page_token = None
    new_count = 0
    done_old_videos = False

    while True:
        try:
            time.sleep(random.uniform(0.3, 0.8))
            request = youtube.playlistItems().list(
                part="snippet",
                playlistId=playlist_id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            items = response.get("items", [])

            # --- 오래된 순으로 가져오기 위해 reverse 적용 ---
            for item in reversed(items):
                snippet = item["snippet"]
                video_id = snippet["resourceId"]["videoId"]
                title = snippet["title"]
                upload_date = snippet["publishedAt"].replace("Z", "")
                upload_dt = datetime.fromisoformat(upload_date)
                url = f"https://www.youtube.com/watch?v={video_id}"

                # 날짜 필터링
                if upload_dt < START_DATE:
                    done_old_videos = True
                    break
                if START_DATE <= upload_dt <= END_DATE:
                    if video_id not in existing_data and keyword_filter(title):
                        existing_data[video_id] = {
                            "Video URL": url,
                            "Title": title,
                            "Channel": channel_name,
                            "UploadDate": upload_date
                        }
                        new_count += 1
                        print(f"[MATCH] {channel_name} | {upload_date} | {title}")

            if done_old_videos:
                break

            next_page_token = response.get("nextPageToken")
            if not next_page_token:
                break

        except googleapiclient.errors.HttpError as e:
            print(f"[ERROR] HTTP Error for playlist {playlist_id}: {e}")
            break
        except Exception as e:
            print(f"[ERROR] Exception for playlist {playlist_id}: {e}")
            break

    return new_count

# -----------------------------
# MAIN
# -----------------------------
def main():
    youtube = googleapiclient.discovery.build("youtube", "v3", developerKey=API_KEY)
    existing_data = load_existing_data()
    total_new = 0

    for channel_id, channel_name in CHANNELS.items():
        print(f"[INFO] Processing channel: {channel_name}")
        playlist_id = get_uploads_playlist_id(youtube, channel_id)
        if not playlist_id:
            continue

        new_videos = fetch_playlist_videos(youtube, playlist_id, channel_name, existing_data)
        total_new += new_videos
        save_temp_data(existing_data)

    print(f"[INFO] Crawling completed. Total new videos added: {total_new}")
    save_final_data(existing_data)

if __name__ == "__main__":
    main()
