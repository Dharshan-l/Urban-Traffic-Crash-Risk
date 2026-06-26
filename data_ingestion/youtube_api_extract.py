from googleapiclient.discovery import build
import json
import os
from datetime import datetime
import time

# ================= LOAD API KEY =================
with open("../config.json") as f:
    config = json.load(f)

API_KEY = config["YOUTUBE_API_KEY"]

# Initialize YouTube API
YOUTUBE = build("youtube", "v3", developerKey=API_KEY)

# ================= CONFIG =================
QUERIES = [
    "road accidents",
    "traffic crash news",
    "road safety awareness",
    "dangerous roads india",
    "car accident footage",
    "highway accidents",
    "traffic violations india"
]

MAX_RESULTS = 50
TOTAL_PAGES = 5   # per query

BRONZE_PATH = "../data_lake/bronze/youtube_sentiment/"
os.makedirs(BRONZE_PATH, exist_ok=True)


# ================= FETCH FUNCTION =================
def fetch_all_data():
    final_data = []

    for query in QUERIES:
        print(f"\n🔍 Fetching for query: {query}")

        next_page_token = None

        for page in range(TOTAL_PAGES):
            print(f"📥 Page {page+1}")

            request = YOUTUBE.search().list(
                q=query,
                part="snippet",
                maxResults=MAX_RESULTS,
                pageToken=next_page_token,
                type="video"
            )

            response = request.execute()
            items = response.get("items", [])

            final_data.extend(items)

            next_page_token = response.get("nextPageToken")

            if not next_page_token:
                print("⚠️ No more pages for this query")
                break

            time.sleep(1)  # avoid rate limit

    return final_data


# ================= SAVE FUNCTION =================
def save_data(data):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_path = f"{BRONZE_PATH}youtube_{timestamp}.json"

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2)

    print(f"\n💾 Total Saved: {len(data)} records")


# ================= MAIN =================
if __name__ == "__main__":
    try:
        data = fetch_all_data()
        save_data(data)
        print("🎉 YouTube ingestion completed successfully")

    except Exception as e:
        print("❌ Error:", e)