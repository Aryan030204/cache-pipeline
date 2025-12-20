import os
import requests
import json
from dotenv import load_dotenv

load_dotenv()

UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")
BRANDS = [b.strip() for b in os.getenv("BRANDS", "").split(",") if b.strip()]
TOTAL_CONFIG_COUNT = int(os.getenv("TOTAL_CONFIG_COUNT", "0"))

if not BRANDS and TOTAL_CONFIG_COUNT > 0:
    for i in range(TOTAL_CONFIG_COUNT):
        tag = os.getenv(f"BRAND_TAG_{i}") or os.getenv(f"X_BRAND_NAME_{i}") or os.getenv(f"SHOP_NAME_{i}")
        if tag:
            BRANDS.append(tag)

if not UPSTASH_REDIS_REST_URL or not UPSTASH_REDIS_REST_TOKEN:
    print("UPSTASH_REDIS_REST_URL or UPSTASH_REDIS_REST_TOKEN not set in .env. Exiting.")
    raise SystemExit(1)

headers = {"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"}

for brand in BRANDS:
    key = f"metrics:{brand}"
    url = UPSTASH_REDIS_REST_URL.rstrip("/") + f"/get/{key}"
    try:
        r = requests.get(url, headers=headers, timeout=10)
        print(f"\n--- {key} (status {r.status_code}) ---")
        try:
            data = r.json()
            print(json.dumps(data, indent=2))
        except Exception:
            print(r.text)
    except Exception as e:
        print(f"Error fetching {key}: {e}")
