import os
import redis
import json
import requests
from dotenv import load_dotenv

load_dotenv()

def view_cache():
    redis_url = os.getenv("REDIS_URL")
    upstash_rest_url = os.getenv("UPSTASH_REDIS_REST_URL")
    upstash_rest_token = os.getenv("UPSTASH_REDIS_REST_TOKEN")

    keys = []
    
    print("-" * 40)
    print("Redis Cache Viewer")
    print("-" * 40)

    # 1. Try Standard Redis Connection
    if redis_url:
        print(f"Attempting connection to Redis (TCP)...")
        try:
            r = redis.from_url(redis_url, decode_responses=True)
            # Fetch all keys matching the pattern
            keys = sorted(r.keys("metrics:*:*"))
            print(f"Connected via TCP. Found {len(keys)} keys.\n")
        except Exception as e:
            print(f"Redis TCP connection failed: {e}\n")

    # 2. Key Listing & grouping
    if not keys and upstash_rest_url and upstash_rest_token:
        print("Attempting connection to Upstash Redis (REST)...")
        try:
            # Note: SCAN is better for production, KEYS is simple for debug
            url = f"{upstash_rest_url.rstrip('/')}/keys/metrics:*:*?_token={upstash_rest_token}"
            resp = requests.get(url, timeout=10)
            if resp.status_code == 200:
                result = resp.json().get("result", [])
                keys = sorted(result)
                print(f"Connected via REST. Found {len(keys)} keys.\n")
            else:
                print(f"Upstash REST failed: {resp.status_code} {resp.text}\n")
        except Exception as e:
            print(f"Upstash REST error: {e}\n")

    if not keys:
        print("Cache is empty or no keys found matching 'metrics:*:*'.")
        return

    # Group by Brand
    grouped = {}
    for k in keys:
        # Format: metrics:<brand>:<date>
        parts = k.split(":")
        if len(parts) >= 3:
            brand = parts[1]
            date = parts[2]
            if brand not in grouped:
                grouped[brand] = []
            grouped[brand].append(date)
        else:
            print(f"Unknown Key Format: {k}")

    # Display
    for brand, dates in grouped.items():
        print(f"Brand: {brand.upper()}")
        # Sort dates descending (newest first)
        dates.sort(reverse=True)
        for date in dates:
            print(f"  - {date}")
        print("")

    print("-" * 40)

if __name__ == "__main__":
    view_cache()
