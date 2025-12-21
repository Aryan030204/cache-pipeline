import os
import redis
from dotenv import load_dotenv

load_dotenv()

def debug_key():
    redis_url = os.getenv("REDIS_URL")
    r = redis.from_url(redis_url, decode_responses=True)
    
    key = "metrics:bbb:2025-12-16"
    print(f"Inspecting Key: {key}")
    
    raw_val = r.get(key)
    print(f"--- RAW VALUE START ---")
    print(raw_val)
    print(f"--- RAW VALUE END ---")

if __name__ == "__main__":
    debug_key()
