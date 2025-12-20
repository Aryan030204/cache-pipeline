from app import fetch_and_cache_all
import json
from decimal import Decimal
import datetime


def default(o):
    if isinstance(o, Decimal):
        try:
            return float(o)
        except Exception:
            return str(o)
    if isinstance(o, (datetime.date, datetime.datetime)):
        return o.isoformat()
    return str(o)


if __name__ == "__main__":
    results = fetch_and_cache_all()
    print(json.dumps(results, indent=2, default=default))
