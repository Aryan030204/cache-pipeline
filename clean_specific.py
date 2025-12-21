import os
import redis
from dotenv import load_dotenv

load_dotenv()

def clean_corrupted():
    redis_url = os.getenv("REDIS_URL")
    if not redis_url:
        return

    r = redis.from_url(redis_url, decode_responses=True)
    
    # We know 2025-12-16 was the date affected by the bad run
    pattern = "metrics:*:2025-12-16"
    
    print(f"Scanning for {pattern}...")
    keys = []
    cursor = '0'
    while cursor != 0:
        cursor, data = r.scan(cursor=cursor, match=pattern, count=100)
        keys.extend(data)
        
    if keys:
        print(f"Deleting {len(keys)} keys: {keys}")
        r.delete(*keys)
        print("Deleted.")
    else:
        print("No keys found.")

if __name__ == "__main__":
    clean_corrupted()
