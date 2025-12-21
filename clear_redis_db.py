import os
import redis
import requests
from dotenv import load_dotenv

load_dotenv()

def clear_redis():
    print("WARNING: This will delete ALL data in the configured Redis database.")
    confirm = input("Are you sure? (y/n): ")
    if confirm.lower() != 'y':
        print("Aborted.")
        return

    redis_url = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")
    upstash_rest_url = os.getenv("UPSTASH_REDIS_REST_URL")
    upstash_rest_token = os.getenv("UPSTASH_REDIS_REST_TOKEN")

    # 1. Try Standard Redis (TCP)
    if redis_url:
        print(f"Connecting to Redis via TCP: {redis_url.split('@')[-1] if '@' in redis_url else '...'}")
        try:
            r = redis.from_url(redis_url, decode_responses=True)
            r.flushdb()
            print("SUCCESS: Redis (TCP) FLUSHDB executed. All data deleted.")
            return
        except Exception as e:
            print(f"Redis TCP flush failed: {e}")
            # Don't return, allow fallback to REST if configured

    # 2. Try Upstash REST
    if upstash_rest_url and upstash_rest_token:
        print("Connecting to Redis via Upstash REST...")
        try:
            # Upstash REST "FLUSHDB" logic
            # Usually POST /flushdb or execute raw command
            # The generic endpoint is /
            # Or /flushdb directly? Upstash docs say /flushdb is valid for some versions, 
            # but the standard way is often just sending the command.
            # Let's try the direct command endpoint pattern: /flushdb
            
            url = f"{upstash_rest_url.rstrip('/')}/flushdb"
            headers = {"Authorization": f"Bearer {upstash_rest_token}"}
            
            # Using POST usually implies command execution
            resp = requests.post(url, headers=headers, timeout=10)
            
            if resp.status_code == 200 and resp.json().get("result") == "OK":
                 print("SUCCESS: Upstash REST FLUSHDB executed. All data deleted.")
            else:
                # Fallback: try deleting keys if flushdb is not exposed on REST
                # Or maybe it was a GET?
                # Let's try to interpret the response
                print(f"Upstash REST FLUSHDB response: {resp.status_code} {resp.text}")
                
        except Exception as e:
            print(f"Upstash REST failed: {e}")
    else:
        print("No valid Redis configuration found.")

if __name__ == "__main__":
    clear_redis()
