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

if not BRANDS and TOTAL_CONFIG_COUNT > 0:
    # derive brand tags from BRAND_TAG_i or SHOP_NAME_i
    derived = []
    for i in range(TOTAL_CONFIG_COUNT):
        tag = os.getenv(f"BRAND_TAG_{i}") or os.getenv(f"X_BRAND_NAME_{i}") or os.getenv(f"SHOP_NAME_{i}")
        if tag:
            derived.append(tag)
    BRANDS = derived

if not BRANDS:
    print("Warning: No brands configured. Set BRANDS or TOTAL_CONFIG_COUNT with BRAND_TAG_<i> in your .env.")

if not (UPSTASH_REDIS_URL or UPSTASH_REDIS_REST_URL):
    print("Warning: No Upstash Redis URL configured; caching will be disabled.")

redis_client = None
use_redis_rest = False
if UPSTASH_REDIS_URL:
    try:
        redis_client = redis.from_url(UPSTASH_REDIS_URL, decode_responses=True)
    except Exception as e:
        print("Failed to create redis client:", e)
elif UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN:
    use_redis_rest = True


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


def fetch_metrics_for_brand(brand: str) -> dict:
    """Fetch metrics for a single brand using SQLAlchemy (supports MySQL postgres if connection string provided).

    If `<BRAND>_METRICS_QUERY` (uppercased) exists, it will be executed and the rows returned as `custom_rows`.
    Otherwise simple default COUNT(*) queries are attempted on `products`, `orders`, and `customers`.
    """
    conn_str = get_conn_str_for_brand(brand)
    metrics = {"brand": brand, "timestamp": datetime.datetime.utcnow().isoformat() + "Z"}

    custom_query = os.getenv(f"{brand.upper()}_METRICS_QUERY") or os.getenv(f"BRAND_{brand.upper()}_METRICS_QUERY")

    if not conn_str:
        metrics["error"] = "missing_connection_string"
        return metrics

    try:
        engine = create_engine(conn_str)
        with engine.connect() as conn:
            # Prefer explicit overall_summary latest-row read
            try:
                q = text("SELECT * FROM overall_summary ORDER BY date DESC LIMIT 1")
                res = conn.execute(q).first()
                if res:
                    # convert SQLAlchemy Row to plain dict and serialize dates
                    row = dict(res._mapping)
                    for k, v in list(row.items()):
                        if hasattr(v, "isoformat"):
                            try:
                                row[k] = v.isoformat()
                            except Exception:
                                row[k] = str(v)
                    metrics["overall_summary"] = row
                else:
                    # fallback: if empty, allow custom query or generic counts
                    if custom_query:
                        res2 = conn.execute(text(custom_query))
                        rows = [dict(r._mapping) for r in res2]
                        metrics["custom_rows"] = rows
                    else:
                        counts = {}
                        for name, q2 in [("products", "SELECT COUNT(*) AS c FROM products"), ("orders", "SELECT COUNT(*) AS c FROM orders"), ("customers", "SELECT COUNT(*) AS c FROM customers")]:
                            try:
                                r2 = conn.execute(text(q2)).first()
                                counts[name] = int(r2.c) if r2 and hasattr(r2, 'c') else (r2[0] if r2 else None)
                            except Exception:
                                counts[name] = None
                        metrics["counts"] = counts
            except Exception as sub_e:
                # If overall_summary read fails, try custom query or counts
                if custom_query:
                    try:
                        res2 = conn.execute(text(custom_query))
                        rows = [dict(r._mapping) for r in res2]
                        metrics["custom_rows"] = rows
                    except Exception as e2:
                        metrics["error"] = str(e2)
                else:
                    counts = {}
                    for name, q2 in [("products", "SELECT COUNT(*) AS c FROM products"), ("orders", "SELECT COUNT(*) AS c FROM orders"), ("customers", "SELECT COUNT(*) AS c FROM customers")]:
                        try:
                            r2 = conn.execute(text(q2)).first()
                            counts[name] = int(r2.c) if r2 and hasattr(r2, 'c') else (r2[0] if r2 else None)
                        except Exception:
                            counts[name] = None
                    metrics["counts"] = counts
    except Exception as e:
        metrics["error"] = str(e)

    return metrics


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

    payload = json.dumps(_normalize(value))
    prev_val = None
    if redis_client:
        try:
            prev_val = redis_client.get(key)
        except Exception as e:
            print("Redis GET failed:", e)
    elif use_redis_rest:
        try:
            r = requests.get(UPSTASH_REDIS_REST_URL.rstrip("/") + f"/get/{key}", headers={"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}"}, timeout=10)
            if r.status_code == 200:
                j = r.json()
                # Upstash REST /get returns { "result": <value> }
                prev_val = j.get("result")
        except Exception as e:
            print("Upstash REST GET failed:", e)

    # Set primary key to new payload
    if redis_client:
        try:
            redis_client.set(key, payload, ex=ex)
        except Exception as e:
            print("Redis SET failed:", e)
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
                print("Upstash REST set failed:", r.status_code, r.text)
                return False
        except Exception as e:
            print("Upstash REST request failed:", e)
            return False
    else:
        print("No cache configured for key", key)
        return False

    # Preserve previous under key:old with TTL so it gets deleted automatically
    if prev_val:
        old_key = f"{key}:old"
        if redis_client:
            try:
                redis_client.set(old_key, prev_val, ex=preserve_seconds)
            except Exception as e:
                print("Redis set old failed:", e)
        elif use_redis_rest:
            try:
                url = UPSTASH_REDIS_REST_URL.rstrip("/") + f"/set/{old_key}"
                headers = {"Authorization": f"Bearer {UPSTASH_REDIS_REST_TOKEN}", "Content-Type": "application/json"}
                body = {"value": prev_val, "ex": preserve_seconds}
                r = requests.post(url, headers=headers, json=body, timeout=10)
                if r.status_code not in (200, 201):
                    print("Upstash REST set old failed:", r.status_code, r.text)
            except Exception as e:
                print("Upstash REST request failed when setting old:", e)

    return True


def fetch_and_cache_all() -> dict:
    results = {}

    def compute_max_workers():
        # Determine a sensible max worker count based on CPU cores and number of brands.
        try:
            cpu = multiprocessing.cpu_count() or 1
        except Exception:
            cpu = 1
        # For I/O-bound work (DB + network), allow up to 2 * CPUs
        suggested = max(1, cpu * 2)
        return min(len(BRANDS) or 1, suggested)

    def process_brand(brand: str):
        r = fetch_metrics_for_brand(brand)
        try:
            ok = atomic_cache_replace(f"metrics:{brand}", r, METRICS_TTL, CACHE_PRESERVE_OLD_SECONDS)
            if not ok:
                r.setdefault("cache_error", "failed_to_cache")
        except Exception as e:
            r.setdefault("cache_error", str(e))
        return brand, r

    max_workers = compute_max_workers()
    if len(BRANDS) <= 1:
        # simple sequential path
        for brand in BRANDS:
            b, r = process_brand(brand)
            results[b] = r
        return results

    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(process_brand, b): b for b in BRANDS}
        for fut in concurrent.futures.as_completed(futures):
            brand = futures[fut]
            try:
                b, r = fut.result()
                results[b] = r
            except Exception as e:
                results[brand] = {"brand": brand, "error": str(e)}

    return results


@app.route("/qstash", methods=["POST"])
def qstash_hook():
    # Optional token-based verification for QStash — set `QSTASH_TOKEN` in .env and make QStash send
    # `Authorization: Bearer <token>` header. If not set, the endpoint accepts requests without verification.
    if QSTASH_TOKEN:
        auth = request.headers.get("Authorization", "")
        if auth != f"Bearer {QSTASH_TOKEN}":
            return ("unauthorized", 401)

    results = fetch_and_cache_all()
    return jsonify({"status": "ok", "results": results})


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"ok": True, "brands": BRANDS})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", "8080")))
