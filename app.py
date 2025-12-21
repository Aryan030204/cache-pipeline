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
    """Fetch all dashboard metrics for a brand for a specific date."""
    logger.info(f"[{brand}] Fetching metrics for {target_date_str}...")
    conn_str = get_conn_str_for_brand(brand)
    
    today_str = target_date_str
    
    metrics_data = {}

    if not conn_str:
        logger.error(f"[{brand}] No connection string found.")
        return {"error": "missing_connection_string"}

    try:
        # 1. Fetch Page Speed (API) - Independent of DB, use same date
        ps_data = fetch_pagespeed_api(brand, today_str)
        metrics_data["pagespeed"] = ps_data
        
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            
            # --- Common Date Filter ---
            # Most tables use 'created_at' or 'date' column.
            # We filter for the specific day.
            
            # 1. TOTAL ORDERS
            q_orders = text("""
                SELECT COUNT(DISTINCT order_id) as val 
                FROM shopify_orders_update 
                WHERE DATE(created_at) = :d
            """)
            res_orders = conn.execute(q_orders, {"d": today_str}).first()
            total_orders = int(res_orders.val) if res_orders and res_orders.val is not None else 0
            
            metrics_data["total_orders"] = {
                "metric": "TOTAL_ORDERS",
                "range": {"start": today_str, "end": today_str},
                "total_orders": total_orders
            }

            # 2. TOTAL SALES
            q_sales = text("""
                SELECT SUM(total_price) as val 
                FROM shopify_orders_update 
                WHERE DATE(created_at) = :d
            """)
            res_sales = conn.execute(q_sales, {"d": today_str}).first()
            total_sales = float(res_sales.val) if res_sales and res_sales.val is not None else 0.0
            
            metrics_data["total_sales"] = {
                "metric": "TOTAL_SALES",
                "range": {"start": today_str, "end": today_str},
                "total_sales": total_sales
            }
            
            # 3. AOV
            # aov = sales / orders
            aov_val = (total_sales / total_orders) if total_orders > 0 else 0.0
            metrics_data["aov"] = {
                "metric": "AOV",
                "range": {"start": today_str, "end": today_str},
                "total_sales": total_sales,
                "total_orders": total_orders,
                "aov": aov_val
            }
            
            # 4. SESSIONS (needed for CVR, Funnel)
            # Try sessions_summary first for aggregate
            q_sess = text("""
                SELECT number_of_sessions, number_of_atc_sessions 
                FROM sessions_summary 
                WHERE date = :d LIMIT 1
            """)
            res_sess = conn.execute(q_sess, {"d": today_str}).first()
            
            total_sessions = 0
            total_atc = 0
            if res_sess:
                total_sessions = int(res_sess.number_of_sessions or 0)
                total_atc = int(res_sess.number_of_atc_sessions or 0)
            
            # 5. CVR
            # cvr = orders / sessions
            cvr_val = (total_orders / total_sessions) if total_sessions > 0 else 0.0
            cvr_percent = cvr_val * 100
            
            metrics_data["cvr"] = {
                "metric": "CVR",
                "range": {"start": today_str, "end": today_str},
                "total_orders": total_orders,
                "total_sessions": total_sessions,
                "cvr": cvr_val,
                "cvr_percent": cvr_percent
            }
            
            # 6. FUNNEL STATS
            metrics_data["funnel_stats"] = {
                "metric": "FUNNEL_STATS",
                "range": {"start": today_str, "end": today_str},
                "total_sessions": total_sessions,
                "total_atc_sessions": total_atc,
                "total_orders": total_orders
            }
            
            # 7. HOURLY TREND
            # Initialize 24 hours
            hourly_points = []
            for h in range(24):
                hourly_points.append({
                    "hour": h,
                    "label": f"{h:02d}:00",
                    "metrics": {
                        "sales": 0, "sessions": 0, "orders": 0, "atc": 0, "cvr_ratio": 0, "cvr_percent": 0
                    }
                })
            
            # Aggregate Sales/Orders by hour
            q_hourly_sales = text("""
                SELECT HOUR(created_at) as hr, COUNT(DISTINCT order_id) as orders, SUM(total_price) as sales
                FROM shopify_orders_update 
                WHERE DATE(created_at) = :d
                GROUP BY HOUR(created_at)
            """)
            res_hourly = conn.execute(q_hourly_sales, {"d": today_str})
            for row in res_hourly:
                h_idx = row.hr
                if 0 <= h_idx < 24:
                    hourly_points[h_idx]["metrics"]["orders"] = row.orders
                    hourly_points[h_idx]["metrics"]["sales"] = float(row.sales or 0)

            # Aggregate Sessions by hour (from `sessions` table if available, else 0)
            # Note: sessions table might be empty, code handles incomplete data gracefully
            try:
                q_hourly_sess = text("""
                    SELECT HOUR(createdAt) as hr, COUNT(*) as sess
                    FROM sessions 
                    WHERE DATE(createdAt) = :d
                    GROUP BY HOUR(createdAt)
                """)
                res_hsess = conn.execute(q_hourly_sess, {"d": today_str})
                for row in res_hsess:
                    h_idx = row.hr
                    if 0 <= h_idx < 24:
                        hourly_points[h_idx]["metrics"]["sessions"] = row.sess
                        # Recalculate CVR
                        ords = hourly_points[h_idx]["metrics"]["orders"]
                        sess = row.sess
                        if sess > 0:
                            hourly_points[h_idx]["metrics"]["cvr_ratio"] = ords / sess
                            hourly_points[h_idx]["metrics"]["cvr_percent"] = (ords / sess) * 100
            except Exception as e_sess:
                logger.warning(f"[{brand}] Failed to query hourly sessions: {e_sess}")
            
            metrics_data["hourly_trend"] = {
                "range": {"start": today_str, "end": today_str},
                "timezone": "IST",
                "points": hourly_points
                # "comparison" omitted for now as it requires fetching yesterday's data - simpler v1
            }
            
            # 8. ORDER SPLIT (COD vs Prepaid)
            # Simplistic check: financial_status or payment_gateway_names
            # "Cash on Delivery (COD)" is a common gateway name.
            # Querying counts
            q_split = text("""
                SELECT 
                    SUM(CASE WHEN payment_gateway_names LIKE '%Cash%' OR payment_gateway_names LIKE '%COD%' THEN 1 ELSE 0 END) as cod_orders,
                    SUM(CASE WHEN payment_gateway_names NOT LIKE '%Cash%' AND payment_gateway_names NOT LIKE '%COD%' THEN 1 ELSE 0 END) as prepaid_orders
                FROM shopify_orders_update
                WHERE DATE(created_at) = :d
            """)
            res_split = conn.execute(q_split, {"d": today_str}).first()
            cod_orders = int(res_split.cod_orders or 0) if res_split else 0
            prepaid_orders = total_orders - cod_orders # simplified fallback
            # (Better to use the query result directly if reliable, but valid logic is complex)
            # Re-reading query result safely
            if res_split and res_split.prepaid_orders is not None:
                prepaid_orders = int(res_split.prepaid_orders)
            
            metrics_data["order_split"] = {
                "metric": "ORDER_SPLIT",
                "range": {"start": today_str, "end": today_str},
                "cod_orders": cod_orders,
                "prepaid_orders": prepaid_orders,
                "total_orders_from_split": cod_orders + prepaid_orders,
                "cod_percent": (cod_orders / total_orders * 100) if total_orders else 0,
                "prepaid_percent": (prepaid_orders / total_orders * 100) if total_orders else 0
            }

            # 9. PAYMENT SALES SPLIT
            q_sales_split = text("""
                SELECT 
                    SUM(CASE WHEN payment_gateway_names LIKE '%Cash%' OR payment_gateway_names LIKE '%COD%' THEN total_price ELSE 0 END) as cod_sales,
                    SUM(CASE WHEN payment_gateway_names NOT LIKE '%Cash%' AND payment_gateway_names NOT LIKE '%COD%' THEN total_price ELSE 0 END) as prepaid_sales
                FROM shopify_orders_update
                WHERE DATE(created_at) = :d
            """)
            res_ssplit = conn.execute(q_sales_split, {"d": today_str}).first()
            cod_sales = float(res_ssplit.cod_sales or 0) if res_ssplit else 0.0
            prepaid_sales = float(res_ssplit.prepaid_sales or 0) if res_ssplit else 0.0
            total_split_sales = cod_sales + prepaid_sales
            
            metrics_data["payment_sales_split"] = {
                "metric": "PAYMENT_SPLIT_SALES",
                "range": {"start": today_str, "end": today_str},
                "cod_sales": cod_sales,
                "prepaid_sales": prepaid_sales,
                "total_sales_from_split": total_split_sales,
                "cod_percent": (cod_sales / total_split_sales * 100) if total_split_sales else 0,
                "prepaid_percent": (prepaid_sales / total_split_sales * 100) if total_split_sales else 0
            }
            
            # 10. TOP PRODUCTS
            # Assuming shopify_orders_update has `product_id` (from inspection it does seem to have `product_id`)
            # But `shopify_orders_update` usually contains line items if normalized, or we check if it is one row per line item.
            # Schema showed `line_item`, `product_id`.
            q_top = text("""
                SELECT product_id, line_item as title, COUNT(*) as count, SUM(total_price) as sales
                FROM shopify_orders_update 
                WHERE DATE(created_at) = :d AND product_id IS NOT NULL
                GROUP BY product_id, line_item
                ORDER BY count DESC
                LIMIT 50
            """)
            res_top = conn.execute(q_top, {"d": today_str})
            products_list = []
            rank = 1
            for row in res_top:
                products_list.append({
                    "rank": rank,
                    "product_id": row.product_id,
                    "landing_page_path": "", # Placeholder, strictly not in orders table usually
                    "sessions": 0, # Requires joining with sessions which is hard
                    "add_to_cart_rate": 0,
                    "sales_count": row.count
                })
                rank += 1
            
            metrics_data["top_products"] = {
                "brand_key": brand,
                "range": {"start": today_str, "end": today_str},
                "products": products_list
            }

            logger.info(f"[{brand}] Granular fetch complete.")
            
    except Exception as e:
        logger.error(f"[{brand}] Query error: {e}")
        metrics_data["error"] = str(e)
        logger.exception("Traceback:")

    return metrics_data


def atomic_cache_replace(key: str, value: dict, ex: int, preserve_seconds: int):
    """Replace primary cache key with `value` and preserve previous value under `{key}:old` with TTL.

    Steps:
    1. GET current value (if any)
    2. SET primary key to new value with expire `ex`
    3. If previous existed, SET `{key}:old` to previous with expire `preserve_seconds`
    """
    def _normalize(o):
        if o is None:
            return None
        if isinstance(o, Decimal):
            try:
                return float(o)
            except Exception:
                return str(o)
        if isinstance(o, (datetime.date, datetime.datetime)):
            try:
                return o.isoformat()
            except Exception:
                return str(o)
        if isinstance(o, bytes):
            try:
                return o.decode()
            except Exception:
                return str(o)
        if isinstance(o, dict):
            return {k: _normalize(v) for k, v in o.items()}
        if isinstance(o, (list, tuple, set)):
            return [_normalize(v) for v in o]
        return o

    payload = json.dumps(_normalize(value), indent=2)
    prev_val = None
    if redis_client:
        try:
            prev_val = redis_client.get(key)
        except Exception as e:
            logger.error(f"Redis GET failed: {e}")
    elif use_redis_rest:
        try:
            r = requests.get(UPSTASH_REDIS_REST_URL.rstrip("/") + f"/get/{key}", headers={"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"}, timeout=10)
            if r.status_code == 200:
                j = r.json()
                # Upstash REST /get returns { "result": <value> }
                prev_val = j.get("result")
        except Exception as e:
            logger.error(f"Upstash REST GET failed: {e}")

    # Set primary key to new payload
    if redis_client:
        try:
            redis_client.set(key, payload, ex=ex)
            logger.debug(f"Redis SET {key} success.")
        except Exception as e:
            logger.error(f"Redis SET failed: {e}")
            return False
    elif use_redis_rest:
        try:
            url = UPSTASH_REDIS_REST_URL.rstrip("/") + f"/set/{key}"
            headers = {"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}", "Content-Type": "application/json"}
            body = {"value": payload}
            if ex:
                body["ex"] = ex
            r = requests.post(url, headers=headers, json=body, timeout=10)
            if r.status_code not in (200, 201):
                logger.error(f"Upstash REST set failed: {r.status_code} {r.text}")
                return False
        except Exception as e:
            logger.error(f"Upstash REST request failed: {e}")
            return False
    else:
        logger.warning(f"No cache configured for key {key}")
        return False

    # Preserve previous under key:old with TTL so it gets deleted automatically
    if prev_val:
        old_key = f"{key}:old"
        if redis_client:
            try:
                redis_client.set(old_key, prev_val, ex=preserve_seconds)
                logger.debug(f"Redis SET {old_key} (preserve) success.")
            except Exception as e:
                logger.error(f"Redis set old failed: {e}")
        elif use_redis_rest:
            try:
                url = UPSTASH_REDIS_REST_URL.rstrip("/") + f"/set/{old_key}"
                headers = {"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}", "Content-Type": "application/json"}
                body = {"value": prev_val, "ex": preserve_seconds}
                r = requests.post(url, headers=headers, json=body, timeout=10)
                if r.status_code not in (200, 201):
                    print("Upstash REST set old failed:", r.status_code, r.text)
            except Exception as e:
                logger.error(f"Upstash REST request failed when setting old: {e}")

    return True


def fetch_and_cache_all() -> dict:
    """
    Main orchestrator:
    1. Determine rolling window (Today + past 4 days).
    2. Iterate over brands and dates.
    3. Conditionally skip cached historical dates.
    4. Fetch and Cache.
    """
    brands_list = BRANDS
    if not brands_list:
        brands_list = [f"brand_{i}" for i in range(1, 6)]
    
    # Determine Anchor Date
    env_target = os.getenv("TARGET_DATE")
    if env_target:
        try:
            anchor_date = datetime.datetime.strptime(env_target, "%Y-%m-%d")
            logger.info(f"Using TARGET_DATE as anchor: {env_target}")
        except:
            utc_now = datetime.datetime.utcnow()
            ist_now = utc_now + datetime.timedelta(hours=5, minutes=30)
            anchor_date = ist_now
    else:
        utc_now = datetime.datetime.utcnow()
        ist_now = utc_now + datetime.timedelta(hours=5, minutes=30)
        anchor_date = ist_now
    
    # Generate list of 5 dates (Anchor, Anchor-1, ... Anchor-4)
    dates_to_fetch = []
    for i in range(5):
        d = anchor_date - datetime.timedelta(days=i)
        dates_to_fetch.append(d.strftime("%Y-%m-%d"))
    
    logger.info(f"Rolling Window: {dates_to_fetch} for brands: {brands_list}")

    results = {}

    # Parallelize Brand + Date fetches
    # 4 workers is a safe start
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        future_to_key = {}
        
        for brand in brands_list:
            for date_str in dates_to_fetch:
                try:
                    is_today_anchor = (date_str == dates_to_fetch[0])
                    cache_key = f"metrics:{brand}:{date_str}"
                    
                    # Check Existence Logic
                    should_fetch = True
                    if not is_today_anchor and not BACKFILL_MODE:
                        exists = False
                        if redis_client:
                            exists = bool(redis_client.exists(cache_key))
                        elif use_redis_rest:
                            u = f"{UPSTASH_REDIS_REST_URL.rstrip('/')}/get/{cache_key}?_token={UPSTASH_REDIS_REST_TOKEN}"
                            try:
                                r = requests.get(u)
                                if r.status_code == 200:
                                    j = r.json()
                                    if j.get("result"):
                                        exists = True
                            except:
                                pass
                        
                        if exists:
                            logger.info(f"[{brand}] Skipping {date_str} (Already cached)")
                            should_fetch = False
                    
                    if should_fetch:
                        future = executor.submit(fetch_metrics_for_brand, brand, date_str)
                        future_to_key[future] = (brand, date_str, cache_key)
                    else:
                        if brand not in results: results[brand] = {}
                        results[brand][date_str] = "SKIPPED_EXISTS"
                        
                except Exception as e_sched:
                    logger.error(f"Failed to schedule {brand} {date_str}: {e_sched}")

        for future in concurrent.futures.as_completed(future_to_key):
            brand, date_str, cache_key = future_to_key[future]
            try:
                data = future.result()
                
                # Check internal error
                if "error" in data:
                     logger.error(f"[{brand}] Error in sub-fetch for {date_str}: {data['error']}")
                     if brand not in results: results[brand] = {}
                     results[brand][date_str] = f"Error: {data['error']}"
                     continue

                # Cache it
                if not BACKFILL_MODE:
                    ok = atomic_cache_replace(cache_key, data, METRICS_TTL, CACHE_PRESERVE_OLD_SECONDS)
                    if ok:
                        logger.info(f"[{brand}] Cached {date_str} to {cache_key}")
                    else:
                         logger.error(f"[{brand}] Cache write failed for {date_str}")
                else:
                    logger.info(f"[{brand}] BACKFILL_MODE enabled. Skipping cache update for {date_str}.")
                
                if brand not in results: results[brand] = {}
                results[brand][date_str] = "OK"

            except Exception as e:
                logger.error(f"[{brand}] Error processing {date_str}: {e}")
                if brand not in results: results[brand] = {}
                results[brand][date_str] = f"Error: {str(e)}"

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
                # Parse JSON
                data = json.loads(cached_val)
                
                # Robustness: Check if data is wrapped in {"ex": ..., "value": "..."}
                # This can happen if previously saved via Upstash-style body dict
                if isinstance(data, dict) and "value" in data and "ex" in data:
                    # Unwrap one level
                    try:
                        inner = data["value"]
                        # Inner might be a JSON string
                        if isinstance(inner, str):
                            data = json.loads(inner)
                        else:
                            data = inner
                    except:
                        pass # Keep original if unwrap fails
                
                return jsonify(data)
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
