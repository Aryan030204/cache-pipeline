import os
import redis
from dotenv import load_dotenv

load_dotenv()

def clear_all():
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        print("REDIS_URL not found.")
        return

    print(f"Connecting to {redis_url.split('@')[-1]}...")
    try:
        r = redis.from_url(redis_url, decode_responses=True)
        r.flushdb()
        print("FLUSHDB command executed successfully. Cache is empty.")
    except Exception as e:
        print(f"Error flushing DB: {e}")

if __name__ == "__main__":
    clear_all()
