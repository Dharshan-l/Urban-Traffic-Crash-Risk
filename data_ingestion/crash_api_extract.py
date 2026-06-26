import requests
import json
import os
import time
import logging
from datetime import datetime

# ================= CONFIG =================
BASE_URL = "https://data.cityofchicago.org/resource/85ca-t3if.json"
LIMIT = 1000
BRONZE_PATH = "../data_lake/bronze/traffic_crashes/"
OFFSET_FILE = "crash_offset.txt"

# ================= SETUP =================
os.makedirs(BRONZE_PATH, exist_ok=True)

logging.basicConfig(
    filename="crash_ingestion.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)

# ================= OFFSET FUNCTIONS =================
def get_last_offset():
    if os.path.exists(OFFSET_FILE):
        with open(OFFSET_FILE, "r") as f:
            return int(f.read().strip())
    return 0


def update_offset(offset):
    with open(OFFSET_FILE, "w") as f:
        f.write(str(offset))


# ================= FETCH DATA =================
def fetch_data():
    offset = get_last_offset()

    print(f"🚀 Starting from offset: {offset}")
    logging.info(f"Starting ingestion from offset {offset}")

    while True:
        url = f"{BASE_URL}?$limit={LIMIT}&$offset={offset}"

        print(f"\n📥 Fetching offset {offset}")
        print(f"🔗 URL: {url}")

        # Retry logic
        for attempt in range(3):
            try:
                response = requests.get(url, timeout=30)
                print("✅ Status Code:", response.status_code)

                response.raise_for_status()
                data = response.json()
                break

            except Exception as e:
                print(f"⚠️ Retry {attempt+1} failed:", e)
                time.sleep(2)
        else:
            print("❌ All retries failed")
            logging.error(f"Failed at offset {offset}")
            return False

        # Stop if no data
        if not data:
            print("✅ No more data available.")
            break

        # Save batch (Bronze Layer - RAW JSON)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_path = f"{BRONZE_PATH}crashes_{timestamp}_offset_{offset}.json"

        with open(file_path, "w") as f:
            json.dump(data, f, indent=2)

        print(f"💾 Saved {len(data)} records")
        logging.info(f"Saved {len(data)} records at offset {offset}")

        # Update offset (incremental ingestion)
        offset += LIMIT
        update_offset(offset)

        # Rate limiting
        time.sleep(1)

    return True


# ================= MAIN =================
if __name__ == "__main__":
    success = fetch_data()

    if success:
        print("🎉 Crash data ingestion completed successfully")
        logging.info("Crash ingestion completed successfully")
    else:
        print("❌ Ingestion failed")
        logging.error("Ingestion failed")