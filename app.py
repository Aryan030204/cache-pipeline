from flask import Flask, request, jsonify
import os
import json
import base64
import datetime
import ssl
from decimal import Decimal
from dotenv import load_dotenv
import redis
import requests
import certifi
from Crypto.Cipher import AES
from Crypto.Util.Padding import unpad
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool
import concurrent.futures
import multiprocessing
import logging
import time

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
UPSTASH_REDIS_URL = os.getenv("UPSTASH_REDIS_URL")
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")
METRICS_TTL = int(os.getenv("CACHE_TTL_SECONDS", os.getenv("METRICS_TTL_SECONDS", "86400")))
CACHE_PRESERVE_OLD_SECONDS = int(os.getenv("CACHE_PRESERVE_OLD_SECONDS", os.getenv("CACHE_PRESERVE_OLD", "3600")))
QSTASH_TOKEN = os.getenv("QSTASH_TOKEN")
QSTASH_COMPLETION_TOKEN = os.getenv("QSTASH_COMPLETION_TOKEN")
BACKFILL_MODE = os.getenv("BACKFILL_MODE", "false").lower() == "true"

# Tenant/brand config service
GET_BRANDS_API = os.getenv("GET_BRANDS_API")
PIPELINE_AUTH_HEADER = os.getenv("PIPELINE_AUTH_HEADER")
PASSWORD_AES_KEY = os.getenv("PASSWORD_AES_KEY")
BRAND_CONFIG_CACHE_TTL_SECONDS = int(os.getenv("BRAND_CONFIG_CACHE_TTL_SECONDS", "300"))

if not GET_BRANDS_API or not PIPELINE_AUTH_HEADER or not PASSWORD_AES_KEY:
    logger.warning("GET_BRANDS_API / PIPELINE_AUTH_HEADER / PASSWORD_AES_KEY not fully configured. Brand config will be unavailable.")


def decrypt_value(encrypted_str: str) -> str:
    """Decrypt an AES-256-CBC value formatted as iv_b64:ciphertext_b64."""
    if not encrypted_str or ":" not in encrypted_str:
        return encrypted_str
    iv_b64, cipher_b64 = encrypted_str.split(":", 1)
    iv = base64.b64decode(iv_b64)
    cipher = base64.b64decode(cipher_b64)
    key = PASSWORD_AES_KEY.encode("utf-8").ljust(32, b"\0")[:32]
    aes = AES.new(key, AES.MODE_CBC, iv)
    return unpad(aes.decrypt(cipher), AES.block_size).decode("utf-8")


_brand_config_cache = {"data": None, "fetched_at": 0.0}


def fetch_active_brands(force_refresh: bool = False) -> dict:
    """Return {brand_tag: brand_config_dict} for brands where is_active is True.

    TTL-cached (BRAND_CONFIG_CACHE_TTL_SECONDS) unless force_refresh=True.
    On fetch failure, falls back to the last known-good cached value.
    """
    now = time.time()
    if not force_refresh and _brand_config_cache["data"] is not None and \
            (now - _brand_config_cache["fetched_at"]) < BRAND_CONFIG_CACHE_TTL_SECONDS:
        return _brand_config_cache["data"]

    if not GET_BRANDS_API or not PIPELINE_AUTH_HEADER:
        logger.error("GET_BRANDS_API or PIPELINE_AUTH_HEADER missing. Cannot fetch brands.")
        return _brand_config_cache["data"] or {}

    headers = {"x-pipeline-key": PIPELINE_AUTH_HEADER}

    try:
        resp = requests.get(GET_BRANDS_API, headers=headers, timeout=15)
        resp.raise_for_status()
        brands_map = resp.json()  # {brand_id: db_database}
    except Exception as e:
        logger.error(f"Failed to fetch brands list from {GET_BRANDS_API}: {e}")
        return _brand_config_cache["data"] or {}

    configs = {}
    for brand_id in brands_map:
        try:
            detail_resp = requests.get(f"{GET_BRANDS_API.rstrip('/')}/{brand_id}", headers=headers, timeout=15)
            detail_resp.raise_for_status()
            detail = detail_resp.json()

            if detail.get("is_active") is not True:
                continue

            tag = detail.get("brand_tag")
            if not tag:
                logger.warning(f"Brand id {brand_id} has no brand_tag, skipping.")
                continue

            configs[tag] = {
                "brand_id": brand_id,
                "brand_tag": tag,
                "brand_name": detail.get("brand_name"),
                "db_host": detail["db_host"],
                "db_port": int(detail.get("port", 3306)),
                "db_user": detail["db_user"],
                "db_password": decrypt_value(detail["db_password"]),
                "db_database": detail["db_database"],
                "access_token": decrypt_value(detail.get("access_token", "")),
                "shop_name": detail.get("shop_name"),
            }
        except Exception as e:
            logger.error(f"Failed loading brand id {brand_id}: {e}")

    _brand_config_cache["data"] = configs
    _brand_config_cache["fetched_at"] = now
    logger.info(f"Loaded {len(configs)} active brands: {list(configs.keys())}")
    return configs


# Redis Configuration
# Prioritize standard REDIS_URL for official/native Redis
REDIS_URL = os.getenv("REDIS_URL") or os.getenv("UPSTASH_REDIS_URL")

# Legacy/Upstash REST specific (Fallback)
UPSTASH_REDIS_REST_URL = os.getenv("UPSTASH_REDIS_REST_URL")
UPSTASH_REDIS_REST_TOKEN = os.getenv("UPSTASH_REDIS_REST_TOKEN")

redis_client = None
use_redis_rest = False

# Database Engine Cache (prevent connection leak)
ENGINES = {}


def _resolve_ca_bundle_path():
    """Resolve the CA bundle path to use for DB SSL verification, per DB_TLS_CA_MODE."""
    mode = os.getenv("DB_TLS_CA_MODE", "certifi").strip().lower()
    if mode == "none":
        return None
    if mode == "rds":
        write_path = os.getenv("RDS_CA_WRITE_PATH", "/tmp/rds-ca.pem")
        ca_url = os.getenv("RDS_CA_URL", "https://truststore.pki.rds.amazonaws.com/global/global-bundle.pem")
        try:
            if os.path.exists(write_path) and os.path.getsize(write_path) > 0:
                return write_path
            os.makedirs(os.path.dirname(write_path) or ".", exist_ok=True)
            resp = requests.get(ca_url, timeout=30)
            resp.raise_for_status()
            if b"BEGIN CERTIFICATE" not in resp.content:
                raise RuntimeError("Downloaded CA bundle does not look like a PEM certificate file.")
            with open(write_path, "wb") as f:
                f.write(resp.content)
            return write_path
        except Exception as e:
            logger.error(f"Failed to obtain RDS CA bundle, falling back to certifi: {e}")
            return certifi.where()
    # default: certifi
    return certifi.where()


def build_engine(brand_tag: str, cfg: dict):
    """Get existing engine or create new one for a brand's DB, with SSL config."""
    conn_str = (
        f"mysql+pymysql://{cfg['db_user']}:{cfg['db_password']}@"
        f"{cfg['db_host']}:{cfg['db_port']}/{cfg['db_database']}?charset=utf8mb4"
    )
    if conn_str in ENGINES:
        return ENGINES[conn_str]

    try:
        connect_args = {}
        ssl_verify = os.getenv("DB_SSL_VERIFY", "true").strip().lower() == "true"
        if ssl_verify:
            ca_path = _resolve_ca_bundle_path()
            ctx = ssl.create_default_context(cafile=ca_path) if ca_path else ssl.create_default_context()

            verify_cert = os.getenv("DB_SSL_VERIFY_CERT", "true").strip().lower() == "true"
            verify_identity = os.getenv("DB_SSL_VERIFY_IDENTITY", "false").strip().lower() == "true"

            host = cfg["db_host"]
            if "elb.amazonaws.com" in host or ("amazonaws.com" in host and "rds.amazonaws.com" not in host):
                # NLB/ELB hostnames won't match the RDS cert's CN/SAN - never verify identity against them.
                verify_identity = False

            ctx.check_hostname = verify_identity
            ctx.verify_mode = ssl.CERT_REQUIRED if verify_cert else ssl.CERT_NONE
            connect_args = {"ssl": ctx}

        ENGINES[conn_str] = create_engine(
            conn_str,
            poolclass=NullPool,
            connect_args=connect_args
        )
        logger.info(f"[{brand_tag}] Created new database engine (NullPool) with SSL verify={ssl_verify}.")
        return ENGINES[conn_str]
    except Exception as e:
        logger.error(f"[{brand_tag}] Failed to create engine: {e}")
        raise

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

def fetch_metrics_for_brand(brand: str, cfg: dict, target_date_str: str) -> dict:
    """Fetch metrics from overall_summary table for a specific date."""
    logger.info(f"[{brand}] Fetching metrics for {target_date_str}...")

    metrics_data = {}

    try:
        # Use helper to get engine with SSL config
        t0 = time.time()
        engine = build_engine(brand, cfg)

        with engine.connect() as conn:
            t1 = time.time()
            logger.info(f"[{brand}] DB Connect took {t1 - t0:.2f}s")
            
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
            t2 = time.time()
            logger.info(f"[{brand}] Query Execution took {t2 - t1:.2f}s")
            
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


def fetch_hourly_metrics_for_brand(brand: str, cfg: dict, target_date_str: str) -> list:
    """Fetch hourly data from hour_wise_sales table for a specific date."""
    logger.info(f"[{brand}] Fetching HOURLY metrics for {target_date_str}...")

    hourly_data = []

    try:
        # Use helper to get engine with SSL config
        t0 = time.time()
        engine = build_engine(brand, cfg)
        with engine.connect() as conn:
            t1 = time.time()
            logger.info(f"[{brand}] (Hourly) DB Connect took {t1 - t0:.2f}s")
            
            q = text("""
                SELECT 
                    hour, 
                    number_of_orders, 
                    total_sales, 
                    number_of_sessions, 
                    number_of_atc_sessions
                FROM hour_wise_sales
                WHERE date = :d
                ORDER BY hour ASC
            """)
            
            rows = conn.execute(q, {"d": target_date_str}).fetchall()
            t2 = time.time()
            logger.info(f"[{brand}] (Hourly) Query Execution took {t2 - t1:.2f}s")
            
            for r in rows:
                hourly_data.append({
                    "hour": int(r.hour),
                    "number_of_orders": float(r.number_of_orders or 0),
                    "total_sales": float(r.total_sales or 0),
                    "number_of_sessions": int(r.number_of_sessions or 0),
                    "number_of_atc_sessions": int(r.number_of_atc_sessions or 0)
                })
                
            logger.info(f"[{brand}] Fetched {len(hourly_data)} hours for {target_date_str}")

    except Exception as e:
        logger.error(f"[{brand}] Hourly Query error: {e}")
        # Return empty list on error to avoid breaking the pipeline, or could raise
        logger.exception("Traceback:")

    return hourly_data


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
            # Always overwrite existing key to ensure latest values are cached
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
    brand_configs = fetch_active_brands(force_refresh=True)
    if not brand_configs:
        logger.warning("No active brands found. Skipping pipeline run.")
        return {}

    brands_list = list(brand_configs.keys())
    logger.info(f"Pipeline started. Brands to process: {len(brands_list)}")

    
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

    # --- Hourly Dates (Today + Yesterday) ---
    hourly_dates = [
        anchor_date.strftime("%Y-%m-%d"),
        (anchor_date - datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    ]
    hourly_delete_date = (anchor_date - datetime.timedelta(days=2)).strftime("%Y-%m-%d")
    logger.info(f"Hourly Caching: {hourly_dates}")
    logger.info(f"Hourly Deleting: {hourly_delete_date}")

    results = {}

    # --- 3. Parallel Execution ---
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        future_to_item = {}
        
        # Schedule Fetches
        for brand in brands_list:
            cfg = brand_configs[brand]

            # Task: Fetch & Cache for 'dates_to_cache'
            for date_str in dates_to_cache:
                logger.info(f"[{brand}] Triggering fetch for {date_str}...")
                future = executor.submit(fetch_metrics_for_brand, brand, cfg, date_str)
                future_to_item[future] = (brand, date_str, "CACHE")

            # Task: Delete 'date_to_delete'
            # We can just do this synchronously or via simple helper, but needs to happen per brand
            # Let's just do it directly here to ensure it runs
            del_key = f"metrics:{brand}:{date_to_delete}"
            delete_cache_key(del_key)

            # Task: Fetch & Cache HOURLY (Today + Yesterday)
            for h_date in hourly_dates:
                logger.info(f"[{brand}] Triggering HOURLY fetch for {h_date}...")
                future_h = executor.submit(fetch_hourly_metrics_for_brand, brand, cfg, h_date)
                future_to_item[future_h] = (brand, h_date, "HOURLY_CACHE")

            # Task: Delete old hourly
            del_h_key = f"hourly_metrics:{brand}:{hourly_delete_date}"
            delete_cache_key(del_h_key)

        # Process Results
        for future in concurrent.futures.as_completed(future_to_item):
            brand, date_str, action = future_to_item[future]
            try:
                data = future.result()
                
                if action == "HOURLY_CACHE":
                    # Hourly Data Handling
                    cache_key = f"hourly_metrics:{brand}:{date_str}"
                    # Data is a list of dicts or empty list
                    success = atomic_cache_replace(cache_key, data, METRICS_TTL, CACHE_PRESERVE_OLD_SECONDS)
                    status = "OK_HOURLY" if success else "CACHE_FAIL_HOURLY"
                    if brand not in results: results[brand] = {}
                    results[brand][f"{date_str}_hourly"] = status
                    logger.info(f"[{brand}] Cached HOURLY for {date_str}. Status: {status}")
                    continue

                if "error" in data:
                     logger.error(f"[{brand}] Failed to fetch metrics for {date_str}: {data['error']}")
                     if brand not in results: results[brand] = {}
                     results[brand][date_str] = f"Error: {data['error']}"
                     continue
                
                logger.info(f"[{brand}] Fetched metrics for {date_str}. Now caching...")

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
                
                if success:
                    logger.info(f"[{brand}] Successfully cached data for {date_str}. Key: {cache_key}")
                else:
                    logger.error(f"[{brand}] Failed to write to cache for {date_str}.")
                
                if brand not in results: results[brand] = {}
                results[brand][date_str] = status
                
            except Exception as e:
                logger.error(f"[{brand}] Exception {date_str}: {e}")
                if brand not in results: results[brand] = {}
                results[brand][date_str] = str(e)

    return results


@app.route("/trigger-pipeline", methods=["GET"])
def manual_trigger():
    logger.info("Received manual pipeline trigger request.")
    results = fetch_and_cache_all()
    logger.info(f"Manual pipeline run completed. Results: {json.dumps(results)}")
    return jsonify({"status": "ok", "results": results})


@app.route("/qstash", methods=["POST"])
def qstash_hook():
    # Optional token-based verification for QStash — set `QSTASH_COMPLETION_TOKEN` in .env and make QStash
    # send `Authorization: Bearer <token>` header. If not set, the endpoint accepts requests without verification.
    if QSTASH_COMPLETION_TOKEN:
        auth = request.headers.get("Authorization", "")
        logger.info(f"Received QStash webhook. Auth header present: {bool(auth)}")
        if auth != f"Bearer {QSTASH_COMPLETION_TOKEN}":
            logger.warning("Unauthorized QStash request.")
            return ("unauthorized", 401)
        else:
            logger.info("QStash request authorized.")
    else:
        logger.info("Received QStash webhook (No Token Verification enabled).")

    logger.info("Starting pipeline execution via QStash trigger...")
    results = fetch_and_cache_all()
    logger.info(f"QStash pipeline run completed. Results summary: {json.dumps(results)}")
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

    if brand not in fetch_active_brands():
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
    return jsonify({"ok": True, "brands": list(fetch_active_brands().keys())})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
