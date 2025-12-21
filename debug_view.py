import os
import redis
import json
import sys
from dotenv import load_dotenv

load_dotenv()

def view():
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        print("Error: REDIS_URL not set")
        return

    try:
        r = redis.from_url(redis_url, decode_responses=True)
        key = "metrics:bbb:2025-12-16"
        val = r.get(key)
        print(f"--- RAW VALUE FOR {key} ---")
        print(val)
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    view()
