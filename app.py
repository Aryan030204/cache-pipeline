from flask import Flask, request, jsonify
import os
import json
import datetime
from decimal import Decimal
from dotenv import load_dotenv
import redis
import requests
from sqlalchemy import create_engine, text
import concurrent.futures
import multiprocessing
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(processName)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("cache_pipeline")

load_dotenv()

app = Flask(__name__)

# Environment configuration
# Primary brand list: use BRANDS if provided; otherwise derive brands from indexed config variables
BRANDS = [b.strip() for b in os.getenv("BRANDS", "").split(",") if b.strip()]
TOTAL_CONFIG_COUNT = int(os.getenv("TOTAL_CONFIG_COUNT", "0"))
UPSTASH_REDIS_URL = os.getenv("UPSTASH_REDIS_URL")
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")
METRICS_TTL = int(os.getenv("CACHE_TTL_SECONDS", os.getenv("METRICS_TTL_SECONDS", "86400")))
CACHE_PRESERVE_OLD_SECONDS = int(os.getenv("CACHE_PRESERVE_OLD_SECONDS", os.getenv("CACHE_PRESERVE_OLD", "3600")))
QSTASH_TOKEN = os.getenv("QSTASH_TOKEN")
BACKFILL_MODE = os.getenv("BACKFILL_MODE", "false").lower() == "true"

if not BRANDS and TOTAL_CONFIG_COUNT > 0:
    # derive brand tags from BRAND_TAG_i or SHOP_NAME_i
    derived = []
    for i in range(TOTAL_CONFIG_COUNT):
        tag = os.getenv(f"BRAND_TAG_{i}") or os.getenv(f"X_BRAND_NAME_{i}") or os.getenv(f"SHOP_NAME_{i}")
        if tag:
            derived.append(tag)
    BRANDS = derived

if not BRANDS:
    logger.warning("No brands configured. Set BRANDS or TOTAL_CONFIG_COUNT with BRAND_TAG_<i> in your .env.")
else:
    logger.info(f"Loaded brands: {BRANDS}")


# Redis Configuration
# Prioritize standard REDIS_URL for official/native Redis
REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")

# Legacy/Upstash REST specific (Fallback)
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

redis_client = None
use_redis_rest = False

if REDIS_URL:
    try:
        # Standard Redis Client (TCP)
        # This handles redis:// and rediss:// (SSL)
        redis_client = redis.from_url(REDIS_URL, decode_responses=True)
        redis_client.ping() # Check connection
        logger.info("Connected to Redis via TCP (Standard Client)")
    except Exception as e:
        logger.error(f"Failed to connect to Redis (TCP): {e}")
        redis_client = None

# Fallback to REST only if Native Client failed or not configured
if not redis_client and UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN:
    use_redis_rest = True
    logger.info("Configured for Upstash Redis REST API")


def brand_index_map():
    """Return mapping brand -> index based on indexed env vars when BRANDS wasn't explicitly set.

    If BRANDS was provided by name, we still attempt to find an index by matching BRAND_TAG_i or SHOP_NAME_i.
    """
    mapping = {}
    # check indices up to TOTAL_CONFIG_COUNT
    for i in range(TOTAL_CONFIG_COUNT):
        tag = os.getenv(f"BRAND_TAG_{i}") or os.getenv(f"X_BRAND_NAME_{i}") or os.getenv(f"SHOP_NAME_{i}")
        if tag:
            mapping[tag] = i
    return mapping


def get_conn_str_for_brand(brand: str):
    # try brand-indexed env like MYSQL_CONNECT_<i>
    mapping = brand_index_map()
    if brand in mapping:
        i = mapping[brand]
        val = os.getenv(f"MYSQL_CONNECT_{i}")
        if val:
            return val

    # fallback checks (some flexibility)
    candidates = [
        f"{brand.upper()}_DATABASE_URL",
        f"BRAND_{brand.upper()}_DATABASE_URL",
        f"{brand}_DATABASE_URL",
    ]
    for var in candidates:
        val = os.getenv(var)
        if val:
            return val
    return None


def get_brand_indices():
    """Return list of config indices available."""
    return range(TOTAL_CONFIG_COUNT)

def fetch_pagespeed_api(brand_key: str, date_str: str) -> dict:
    """Fetch pagespeed data from external API."""
    brand_map = {
        "PTS": "SkincarePersonalTouch",
        "BBB": "BlaBliBluLife",
        "MILA": "MilaBeaute",
        "TMC": "TMC"
    }
    api_brand = brand_map.get(brand_key, brand_key)
    
    # Use the speed-audit-service URL found in verification
    url = "https://speed-audit-service.onrender.com/api/pagespeed"
    
    try:
        resp = requests.get(url, params={
            "brand_key": api_brand,
            "start_date": date_str,
            "end_date": date_str
        }, timeout=10)
        
        if resp.status_code == 200:
            return resp.json()
        logger.error(f"[{brand_key}] PageSpeed API failed: {resp.status_code} {resp.text}")
    except Exception as e:
        logger.error(f"[{brand_key}] PageSpeed API error: {e}")
        
    return {}

def fetch_metrics_for_brand(brand: str, target_date_str: str) -> dict:
    """Fetch metrics from overall_summary table for a specific date."""
    logger.info(f"[{brand}] Fetching metrics for {target_date_str}...")
    conn_str = get_conn_str_for_brand(brand)
    
    metrics_data = {}

    if not conn_str:
        logger.error(f"[{brand}] No connection string found.")
        return {"error": "missing_connection_string"}

    try:
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            # Query overall_summary
            # Note: The table has 'date' column.
            q_summary = text("""
                SELECT 
                    total_orders, 
                    total_sales, 
                    total_sessions, 
                    total_atc_sessions
                FROM overall_summary
                WHERE date = :d
            """)
            
            res = conn.execute(q_summary, {"d": target_date_str}).first()
            
            if not res:
                logger.warning(f"[{brand}] No data found in overall_summary for {target_date_str}")
                # Return zeros or handle as empty? Usually better to return 0s so frontend doesn't break
                metrics_data = {
                     "total_orders": 0,
                     "total_sales": 0.0,
                     "average_order_value": 0.0,
                     "conversion_rate": 0.0,
                     "total_sessions": 0,
                     "total_atc_sessions": 0
                }
            else:
                total_orders = float(res.total_orders or 0)
                total_sales = float(res.total_sales or 0)
                total_sessions = int(res.total_sessions or 0)
                total_atc_sessions = int(res.total_atc_sessions or 0)
                
                # Calculations
                aov = (total_sales / total_orders) if total_orders > 0 else 0.0
                cvr = (total_orders / total_sessions * 100) if total_sessions > 0 else 0.0
                
                metrics_data = {
                     "total_orders": total_orders,
                     "total_sales": total_sales,
                     "average_order_value": aov,
                     "conversion_rate": cvr,
                     "total_sessions": total_sessions,
                     "total_atc_sessions": total_atc_sessions
                }
                
            logger.info(f"[{brand}] {target_date_str} -> {metrics_data}")

    except Exception as e:
        logger.error(f"[{brand}] Query error: {e}")
        metrics_data["error"] = str(e)
        logger.exception("Traceback:")

    return metrics_data


def atomic_cache_replace(key: str, value: dict, ex: int, preserve_seconds: int):
    """Replace primary cache key with `value`.
       Note: The previous 'old' preservation logic is removed as per user instruction:
       'remove the old logic whatever it was I dont care'.
       However, strict atomic replacement is good practice, so we'll keep the simple SET.
    """
    def _normalize(o):
        if o is None: return None
        if isinstance(o, Decimal): return float(o)
        if isinstance(o, (datetime.date, datetime.datetime)): return o.isoformat()
        return o

    payload = json.dumps(value, default=_normalize, indent=2)

    if redis_client:
        try:
            redis_client.set(key, payload, ex=ex)
            return True
        except Exception as e:
            logger.error(f"Redis SET failed: {e}")
            return False
    elif use_redis_rest:
        try:
            url = UPSTASH_REDIS_REST_URL.rstrip("/") + f"/set/{key}"
            if ex: url += f"?ex={ex}"
            headers = {"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"}
            r = requests.post(url, headers=headers, data=payload, timeout=10)
            return r.status_code in (200, 201)
        except Exception as e:
            logger.error(f"Upstash REST SET failed: {e}")
            return False
            
    return False

def delete_cache_key(key: str):
    """Delete a specific cache key."""
    if redis_client:
        try:
            redis_client.delete(key)
            logger.info(f"Deleted Redis key: {key}")
        except Exception as e:
            logger.error(f"Redis DELETE failed: {e}")
    elif use_redis_rest:
        try:
            url = UPSTASH_REDIS_REST_URL.rstrip("/") + f"/del/{key}"
            headers = {"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"}
            requests.get(url, headers=headers, timeout=5) # /del via REST usually GET or POST
            logger.info(f"Deleted Upstash key: {key}")
        except Exception as e:
            logger.error(f"Upstash REST DELETE failed: {e}")

def fetch_and_cache_all() -> dict:
    """
    Orchestrator:
    1. Determine Anchor Date (Today or Target).
    2. Cache Anchor + prev 4 days (Total 5).
    3. Delete (Anchor - 5 days).
    """
    brands_list = BRANDS
    if not brands_list:
        brands_list = [f"brand_{i}" for i in range(1, 6)]
    
    # --- 1. Determine Anchor Date ---
    # Logic:
    # - If BACKFILL_MODE=true AND TARGET_DATE set -> Anchor = TARGET_DATE
    # - Else -> Anchor = Today (IST)
    
    utc_now = datetime.datetime.utcnow()
    ist_now = utc_now + datetime.timedelta(hours=5, minutes=30)
    today_ist = ist_now.date() # Date object
    
    anchor_date = today_ist
    
    if BACKFILL_MODE:
        env_target = os.getenv("TARGET_DATE")
        if env_target:
            try:
                anchor_date = datetime.datetime.strptime(env_target, "%Y-%m-%d").date()
                logger.info(f"BACKFILL_MODE=True. Using TARGET_DATE: {anchor_date}")
            except ValueError:
                logger.warning(f"Invalid TARGET_DATE format: {env_target}. Fallback to Today.")
        else:
            logger.info("BACKFILL_MODE=True but no TARGET_DATE. Using Today.")
            
    logger.info(f"Anchor Date: {anchor_date}")

    # --- 2. Calculate Dates ---
    dates_to_cache = []
    for i in range(5):
        d = anchor_date - datetime.timedelta(days=i)
        dates_to_cache.append(d.strftime("%Y-%m-%d"))
        
    date_to_delete = (anchor_date - datetime.timedelta(days=5)).strftime("%Y-%m-%d")
    
    logger.info(f"Caching: {dates_to_cache}")
    logger.info(f"Deleting: {date_to_delete}")

    results = {}

    # --- 3. Parallel Execution ---
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_item = {}
        
        # Schedule Fetches
        for brand in brands_list:
            # Task: Fetch & Cache for 'dates_to_cache'
            for date_str in dates_to_cache:
                future = executor.submit(fetch_metrics_for_brand, brand, date_str)
                future_to_item[future] = (brand, date_str, "CACHE")
            
            # Task: Delete 'date_to_delete'
            # We can just do this synchronously or via simple helper, but needs to happen per brand
            # Let's just do it directly here to ensure it runs
            del_key = f"metrics:{brand}:{date_to_delete}"
            delete_cache_key(del_key)

        # Process Results
        for future in concurrent.futures.as_completed(future_to_item):
            brand, date_str, action = future_to_item[future]
            try:
                data = future.result()
                
                if "error" in data:
                     logger.error(f"[{brand}] Failed {date_str}: {data['error']}")
                     if brand not in results: results[brand] = {}
                     results[brand][date_str] = f"Error: {data['error']}"
                     continue

                # Cache
                cache_key = f"metrics:{brand}:{date_str}"
                # logic: if backfill=false => run normally (cache 5 days)
                # logic: if backfill=true => cache target date window
                # The user request in point 8 & 9 implies caching always happens effectively unless there's a specific constraint.
                # Point 8: "this pipeline cached the data for 13-17th"
                # Point 9: "run normally" vs "cache the data of target date and its last five days"
                # My 'dates_to_cache' logic covers both cases by adjusting the anchor.
                
                success = atomic_cache_replace(cache_key, data, METRICS_TTL, CACHE_PRESERVE_OLD_SECONDS)
                status = "OK" if success else "CACHE_FAIL"
                
                if brand not in results: results[brand] = {}
                results[brand][date_str] = status
                
            except Exception as e:
                logger.error(f"[{brand}] Exception {date_str}: {e}")
                if brand not in results: results[brand] = {}
                results[brand][date_str] = str(e)

    return results


@app.route("/qstash", methods=["POST"])
def qstash_hook():
    # Optional token-based verification for QStash — set `QSTASH_TOKEN` in .env and make QStash send
    # `Authorization: Bearer <token>` header. If not set, the endpoint accepts requests without verification.
    if QSTASH_TOKEN:
        auth = request.headers.get("Authorization", "")
        logger.info(f"Received QStash webhook. Auth header present: {bool(auth)}")
        if auth != f"Bearer {QSTASH_TOKEN}":
            logger.warning("Unauthorized QStash request.")
            return ("unauthorized", 401)
    else:
        logger.info("Received QStash webhook (No Token Verification enabled).")

    results = fetch_and_cache_all()
    logger.info("QStash pipeline run completed. Returning results.")
    return jsonify({"status": "ok", "results": results})


@app.route("/api/metrics", methods=["GET"])
def get_metrics():
    """
    Public API to fetch cached metrics.
    Usage: GET /api/metrics?brand=<brand>&date=<YYYY-MM-DD>
    """
    brand = request.args.get("brand")
    date_str = request.args.get("date")

    if not brand or not date_str:
        return jsonify({"error": "Missing 'brand' or 'date' query parameter"}), 400

    if brand not in BRANDS:
        return jsonify({"error": "Invalid brand"}), 400

    cache_key = f"metrics:{brand}:{date_str}"
    
    # Try fetching from Redis
    if redis_client:
        try:
            cached_val = redis_client.get(cache_key)
            if cached_val:
                # cached_val is a JSON string, load it to return proper JSON object
                return jsonify(json.loads(cached_val))
            else:
                return jsonify({"error": "Data not found for this date. Run pipeline first."}), 404
        except Exception as e:
            logger.error(f"Redis get failed: {e}")
            return jsonify({"error": "Internal Redis error"}), 500
    
    elif use_redis_rest:
        # Fallback to Upstash REST
        try:
            u = f"{UPSTASH_REDIS_REST_URL.rstrip('/')}/get/{cache_key}?_token={UPSTASH_REDIS_REST_TOKEN}"
            r = requests.get(u, timeout=5)
            if r.status_code == 200:
                j = r.json()
                val = j.get("result")
                if val:
                    return jsonify(json.loads(val))
                else:
                    return jsonify({"error": "Data not found"}), 404
            else:
                return jsonify({"error": "Upstash REST error"}), 502
        except Exception as e:
             logger.error(f"Upstash fetch failed: {e}")
             return jsonify({"error": "Internal Upstash error"}), 500
             
    else:
        return jsonify({"error": "No Cache Configured"}), 500


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "brands": BRANDS})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
