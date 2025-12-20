# Pipeline B — QStash-triggered metrics fetch & Upstash cache

This small service listens for completion events (published to QStash) and, when triggered, fetches the latest metrics from each brand's database and caches a JSON object into an Upstash Redis key for fast reads.

Files added
- `app.py` — Flask app with `/qstash` webhook and `/health` endpoint.
- `fetch_metrics.py` — CLI runner to fetch & cache once (useful for manual runs or cron jobs).
- `requirements.txt` — Python dependencies.
- `.env.example` — example env variables and conventions.

Env variables and conventions
- `BRANDS` — comma-separated list of brand identifiers (e.g. `brand1,brand2,brand3,brand4`).
- For each brand in `BRANDS` you must provide a database URL. The service checks these env var names (in order):
  - `<BRAND_UPPER>_DATABASE_URL`  (e.g. `BRAND1_DATABASE_URL`)
  - `BRAND_<BRAND_UPPER>_DATABASE_URL` (e.g. `BRAND_BRAND1_DATABASE_URL`)
  - `<brand>_DATABASE_URL` (fallback)

- Optional per-brand custom query: `<BRAND_UPPER>_METRICS_QUERY` or `BRAND_<BRAND_UPPER>_METRICS_QUERY`.
  - If provided that SQL is executed and results saved as `custom_rows`.
  - If not provided, the service will attempt default table counts: `products`, `orders`, `customers`.

- `UPSTASH_REDIS_URL` — Upstash Redis URL (use the full URL `redis://` or `rediss://` form supported by redis-py).
- `METRICS_TTL_SECONDS` — TTL for cached metrics (default `86400` seconds = 24h).
- `QSTASH_AUTH_TOKEN` — optional. If set, the endpoint requires header `Authorization: Bearer <token>`.

Run locally (development)
1. Create a `.env` file (see `.env.example`) with your variables.
2. Create and activate a Python virtualenv and install dependencies:

```powershell
python -m venv .venv; .\.venv\Scripts\Activate.ps1
python -m pip install -r requirements.txt
```

3. Start the app:

```powershell
python app.py
```

4. The service exposes `/qstash` on port 8080 by default and `/health`.

Testing manually
- Run the fetcher once locally:

```powershell
python fetch_metrics.py
```

- Simulate QStash by POSTing to `/qstash` (include `Authorization` header if you set `QSTASH_AUTH_TOKEN`).

Deployment
- Deploy behind a HTTPS endpoint reachable by QStash. Configure QStash to POST your custom completion event to `https://<your-host>/qstash` and, if you set `QSTASH_AUTH_TOKEN`, include the token as `Authorization: Bearer <token>`.

Notes
- The default queries are conservative. If your schema differs provide `BRAND_METRICS_QUERY` per brand to extract exactly what you need.
- This implementation uses PostgreSQL via `psycopg2`. If your DBs are different, adapt the connection/query code in `app.py`.
