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
CHANNEL_ID = "UCkinYTS9IHqOEwR1Sze2JTw"  # SBS

# 저장 파일명: 채널 ID 반영
SAVE_FILE = f"playlist_videos_{CHANNEL_ID}_2015_2022.xlsx"
TEMP_FILE = "temp.json"
PLAYLISTS_JSON = f"playlists_{CHANNEL_ID}.json"  # (선택) 재생목록 스냅샷 저장

KOREAN_KEYWORDS = [
    '기후', '기후변화', '기후위기', '온난화', '탄소', '온실가스', '해수면', '이상기후',
    '종말', '살인 폭염', '이상기온', '이례적', '유례없는', '극한', '인류',
    '역사상', '펄펄', '최악의 더위', '북극', '열대화', '엘니뇨', '라니냐', '기온 급상승', '수온', '재생'
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
    return any(kw.lower() in t for kw in KOREAN_KEYWORDS)

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

PLAYLISTS = {
    "PLUHG6IBxDr3ju5Yu9Sp4VueO06EdaZq_Q": "2022 베이징 동계올림픽",
  "PLUHG6IBxDr3gBmU3LHR5A8vDq4DL1UR7d": "SBS D포럼 | 2021",
  "PLUHG6IBxDr3gxX7u3Z_owUT4VXvChVc8z": "라이브 중계 | 실시간 다시보기 (~2023.08)",
  "PLUHG6IBxDr3jYrIVMVU5mENyALRQL8TYa": "짧은영상 | #Shorts",
  "PLUHG6IBxDr3i8GaZ6wur2VGjr8HJjbGaz": "8뉴스 ｜ 리포트 다시보기 (~2022.04.20)",
  "PLUHG6IBxDr3jb3jmLf5IT22Ppt9tJO9pt": "초유의 팬데믹 올림픽",
  "PLUHG6IBxDr3gJx6NUvKZzzRC83OtLwWIa": "2020 도쿄올림픽🎖패럴림픽",
  "PLUHG6IBxDr3hPN2u4m6ehd3MDbo-6-mnq": "경제 맛집 SBS",
  "PLUHG6IBxDr3hG0UxqBY5faR5PkggJEUwN": "더스페셜리스트｜'한 우물만 파 온' 기자들이 뭉쳤다",
  "PLUHG6IBxDr3gq8XXTvnp6oavi_EJu6X_G": "4·7 재보궐선거｜국민의 선택",
  "PLUHG6IBxDr3i0YzGFD25mE2U_K2G_k0It": "SBS 디지털 오리지널",
  "PLUHG6IBxDr3ivHF8KyMyTcsN9kiWYG6lq": "D리포트ㅣ글로벌D리",
  "PLUHG6IBxDr3iDV46uQXy6CKsqFzGD-MPJ": "8뉴스｜리포트 다시보기(2021.01.28~2021.09.05)",
  "PLUHG6IBxDr3haswjj3cXnNvMWE0AYBmWc": "'코로나19' 비상",
  "PLUHG6IBxDr3gD6_fLX3jC4_TlNQc8agpB": "2020 미국의 선택",
  "PLUHG6IBxDr3gCvSmOqEpSb311_vZJMeX9": "2020 SDF | Art Project",
  "PLUHG6IBxDr3gOHLnt6wP0Uvzhav_80vD0": "SBS 뉴스토리 | 후스토리",
  "PLUHG6IBxDr3g_92zg5Y-SWTxGNFULNBbv": "현직 시장 '극단적 선택'",
  "PLUHG6IBxDr3j_73AVFtWvaVUXArN-HiXB": "2020 국민의 선택",
  "PLUHG6IBxDr3jzaCzBtYK3vjtNVdR_3SmX": "4·15 총선 리포트",
  "PLUHG6IBxDr3h0bT6uTKU3oS8_XG44LwN5": "찍자! 우리 사이로[4.15]",
  "PLUHG6IBxDr3hjZ3bFsnrfFj8Kp5U-qmNx": "인-잇터뷰",
  "PLUHG6IBxDr3jN6Vfxk3xKSjWlXz_KmEhg": "SBS 연예뉴스",
  "PLUHG6IBxDr3jdIuxMBkv72C4UrVnIuPsB": "오스카 휩쓴 봉준호 '기생충'",
  "PLUHG6IBxDr3h61KfTrExWlEdIp6uC-4O0": "'코로나19' 비상(~20.11.30)",
  "PLUHG6IBxDr3hfclkX8Evgz8maTUV8NpxH": "문 대통령 신년 기자회견",
  "PLUHG6IBxDr3grxkWZkQaTtUAelJBIWHIE": "2020 뺏지쇼 | 두 가지 맛 정치 토크",
  "PLUHG6IBxDr3gNJFbPJHBIAzFvCddCl5EY": "본격 뉴스 배달쇼! 팔보채",
  "PLUHG6IBxDr3i8E7r9c7Y3Fwv1aJ1c1DLI": "2019 WBSC 프리미어12",
  "PLUHG6IBxDr3jxXLy4Q-9tQk8pA5-5eqJ9": "2019 HIT NEWS",
  "PLUHG6IBxDr3hBXWno_VxgikzrxPwZ8HlJ": "화성 연쇄살인사건 재수사",
  "PLUHG6IBxDr3gatHjvW8gV5pJz2ySQXrWx": "아프리카돼지열병 비상",
  "PLUHG6IBxDr3joSw7_SkKQhoJkga2E283i": "조국 법무장관 사퇴",
  "PLUHG6IBxDr3iYAjsCokH4iMYbihUFt9pY": "주영진의 뉴스브리핑 | 전체보기",
  "PLUHG6IBxDr3iG0rqZDtb1gvr7tT8nb3Nu": "청년 흥신소",
  "PLUHG6IBxDr3hSdTfbvfggiWGI03RmqFkM": "나이트라인 초대석",
  "PLUHG6IBxDr3gn2FIzX2BH3Kh8KLs3CRxT": "한일 경제 전쟁",
  "PLUHG6IBxDr3ieyMqiV235k8YxRl2d7L5b": "2019 U-20 월드컵｜우리는 원팀!",
  "PLUHG6IBxDr3jbYixwjpXHvt4EYBCIInBR": "재재특급｜신문물을 LIVE로 전파하라!",
  "PLUHG6IBxDr3gs6naBqU4h-uFpOhP7Nf6_": "김현우의 취조&어젠더스 ┃8뉴스 메인 앵커가 떴다!",
  "PLUHG6IBxDr3gBsouKy_zFgtCfmGjWWssm": "김범주의 이건머니 │돈 버는 경제쇼",
  "PLUHG6IBxDr3iXEUfk8FBcVhQQ-EDMdLh-": "ㅅㅅㅅ｜배거슨 라이브",
  "PLUHG6IBxDr3h9tLUJAyQ7gbouz959oypM": "비오다갬 │세상의 모든 날씨",
  "PLUHG6IBxDr3gJSCE1NNcREevcsmd66QPg": "비스킷 │뉴스를 점령하라",
  "PLUHG6IBxDr3h7ILkP7r_WSzOrt6mAXDd0": "8뉴스｜리포트 다시보기 (~2019.11.17)",
  "PLUHG6IBxDr3hOxAOLwZ7gkQM7QZVNxjql": "모바일24｜이 주의 PICK",
  "PLUHG6IBxDr3iwvtMd8XIgMaXZ_wWraVc1": "SBS News Global",
  "PLUHG6IBxDr3gkRxYl5GWC-nFJ-NknHTbZ": "제2차 북미정상회담｜평화를 그리다",
  "PLUHG6IBxDr3jmyNmkroBHKjXy896vo-1h": "제2차 북미정상회담｜특별 생방송 다시보기",
  "PLUHG6IBxDr3honPkX6_NDIY8Fir6UGhRi": "끝까지 판다｜의원님의 부적절한 처신",
  "PLUHG6IBxDr3j8Edc0I_WwFGKJ_d4BQgvo": "2019 문재인 대통령 신년 기자회견｜하이라이트",
  "PLUHG6IBxDr3geeJbXHRkiGjatTjC5SX6A": "특집｜체육계 미투 파문 확산",
  "PLUHG6IBxDr3h47g9WIjC1N5OA1TV7yWm-": "청와대 특별감찰반 논란｜하이라이트",
  "PLUHG6IBxDr3juTIWJlRr87BmLYZk9-6cc": "2018 분야별 뉴스 5｜하이라이트",
  "PLUHG6IBxDr3gRU1E0e3jjtbRL3JlmD9DA": "특집｜컬링 '팀 킴' 단독보도",
  "PLUHG6IBxDr3hSDkm58pdgMFoQ4Dv6CAyu": "끝까지 판다｜삼성 '차명 부동산' 집중 추적",
  "PLUHG6IBxDr3jx6QIJsr1G1GI-b5-oDYwa": "제3차 남북정상회담｜특별 생방송 다시보기",
  "PLUHG6IBxDr3ibi8dHbvybnamRiTFjSW2o": "제3차 남북정상회담｜남과 북, 다시 평양에서",
  "PLUHG6IBxDr3ij6u3kv61V5ysVuHUZ_ESd": "다시 뛰는 태극전사｜벤투호가 간다",
  "PLUHG6IBxDr3hXMHp0kX2gFoOm85toDdNX": "8뉴스｜리포트 다시보기 (~2019.4.7)",
  "PLUHG6IBxDr3ijGGg_Ua-M89er9c5bRkvW": "2018 자카르타 · 팔렘방｜최용수와 함께",
  "PLUHG6IBxDr3gYE_lVI4xk39U4In63JecQ": "특집｜역대 동계올림픽 영광의 순간",
  "PLUHG6IBxDr3isQQGWh1Nv-YHwQneKDRWg": "2018 자카르타 · 팔렘방｜영광의 순간들",
  "PLUHG6IBxDr3jSE4biCUw5JWHCRhlmduF5": "2018 자카르타 · 팔렘방｜1분의 감동",
  "PLUHG6IBxDr3jxDTz7BIRuqBC64Eqi0a8H": "2018 자카르타 · 팔렘방｜국가대표 스페셜",
  "PLUHG6IBxDr3g2q4AMStONqjl_u7OaGZYX": "보이스V｜새로운 뉴스의 시작",
  "PLUHG6IBxDr3hVrmSsW_pMhP3X0e7F4xlG": "탐사보도｜끝까지 판다",
  "PLUHG6IBxDr3hPdU5cpQCL837WVzed4FoF": "2018 러시아 월드컵｜박지성과 함께",
  "PLUHG6IBxDr3g86-ONYoUK2x3ljCHfik-f": "2018 러시아 월드컵｜다시 뜨겁게!",
  "PLUHG6IBxDr3hON-eE4vNv0vkYRHwRzCWn": "2018 국민의 선택｜출구조사 발표",
  "PLUHG6IBxDr3gAuclUfPxlNqCqG6CsLmFj": "2018 국민의 선택｜돌아온 절대강자",
  "PLUHG6IBxDr3jWH-tXVXHqsIsuursJcc1l": "2018 국민의 선택｜전체 다시보기",
  "PLUHG6IBxDr3jO9mD2U3YlJcjgObgkr2OE": "2018 북미정상회담｜평화를 그리다",
  "PLUHG6IBxDr3iPYP27b9S87hMcgS3oOSN2": "2018 북미정상회담｜전체 다시보기",
  "PLUHG6IBxDr3g-MDSPvgbG1Gzbkhdpy5XD": "2018 러시아 월드컵｜다시 보는 골장면",
  "PLUHG6IBxDr3iB3kO6DbHJ9rIQakgP_oTh": "2018 남북정상회담｜북미회담 성사를 위하여",
  "PLUHG6IBxDr3hZOi9hH6nsZxm416qIuL9r": "특집｜'라돈 침대' 연속 단독보도",
  "PLUHG6IBxDr3jfUHaR19wiDl7_ETS3wi6N": "끝까지 판다｜5.18 전두환 마지막 비밀",
  "PLUHG6IBxDr3hWANYeDAPmPwZJIJq0H57f": "2018 국민의 선택｜다시 보는 선거방송 레전드",
  "PLUHG6IBxDr3gbr_8OaVnzYIB_V-jqpnsw": "2018 남북정상회담｜전체 다시보기",
  "PLUHG6IBxDr3g4Wc5c7GodtyVO0Vb6uTz1": "2018 남북정상회담｜평화, 새로운 시작",
  "PLUHG6IBxDr3g5j6relIqLOZ-zT_wnI90y": "집중 보도｜특별사면, 삼성의 은밀한 뒷거래",
  "PLUHG6IBxDr3gPx6LWHkD3k648YvQxvdFV": "끝까지 판다｜삼성 에버랜드의 수상한 땅값",
  "PLUHG6IBxDr3hLFmeWBAVOLayJ7Lm5pYvG": "모닝와이드｜전체 다시보기",
  "PLUHG6IBxDr3h67IX01o9pulMZw4SHWUAA": "8뉴스｜리포트 다시보기 (~2018.8.26)",
  "PLUHG6IBxDr3iokWwEy6GOMNVeVeQ67aX3": "특집｜이명박 전 대통령 구속",
  "PLUHG6IBxDr3iMVeT7b3DrOYJewNb5nGNT": "THE JOURNALIST",
  "PLUHG6IBxDr3gosdZB9zi4YIcTE_GEM_j9": "특집｜국정원 댓글 공작 연속보도",
  "PLUHG6IBxDr3gbvky61BcMSuYvujO-ZupR": "특집｜5·18 광주민주화 운동 연속보도",
  "PLUHG6IBxDr3jsEYiJyHTSPM9BjmlTwXsP": "특집｜SBS LIVE 풀영상 다시보기",
  "PLUHG6IBxDr3hxix9OMPwHk4GxF1G2OSEu": "특집｜재벌가 손자 · 연예인 아들 사립초 폭행 파문",
  "PLUHG6IBxDr3inaBwKesk15cAJZfPIA9Id": "특집｜5.18 광주 민주화운동 기념식",
  "PLUHG6IBxDr3jF57rcRRc4AFRjgmfrq0U9": "특집｜2017 국민의 선택",
  "PLUHG6IBxDr3hTQNmdP9yMef6rulGZXfjm": "특집｜문재인 대통령 취임",
  "PLUHG6IBxDr3gCfEy-ulvTguBkH1KEtJQ1": "2018 국민의 선택｜아이보트챌린지",
  "PLUHG6IBxDr3jKodEB2H_6DlFtXTVx0vf0": "8뉴스｜전체 다시보기",
  "PLUHG6IBxDr3ha1rzNYBVMggR6fGdpOjsl": "팟캐스트 | SBS 골라듣는 뉴스룸",
  "PLUHG6IBxDr3jp-qTcBsF_Vuy8ePIUDaEQ": "제보영상｜시청자와 함께 만드는 뉴스",
  "PLUHG6IBxDr3hHFFLhukmApH5uL822rNEI": "주영진 뉴스브리핑 ｜ 모아보기 (~2022.03.13)",
  "PLUHG6IBxDr3gFFmk6F9XuFiEcHDnzzC5P": "SBS 특종 | 단독 보도"
}


# -----------------------------
# MAIN
# -----------------------------
def main():
    # 1) 재생목록 수집
    # playlists = get_all_playlists(API_KEY, CHANNEL_ID)
    playlists = PLAYLISTS

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
