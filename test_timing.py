import time
import logging
import sys
# Mock the app context or just import the relevant parts if possible, 
# but easier to just import app
import app

# Setup logging to console
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
logger = logging.getLogger("test_timing")

def test_single_brand():
    # Pick a brand that was potentially slow
    brand = "bbb"
    date_str = "2025-12-28"

    brand_configs = app.fetch_active_brands(force_refresh=True)
    cfg = brand_configs.get(brand)
    if not cfg:
        print(f"Brand '{brand}' not found or not active.")
        return

    print(f"Testing metrics fetch for {brand} on {date_str}...")
    t0 = time.time()
    res = app.fetch_metrics_for_brand(brand, cfg, date_str)
    t1 = time.time()
    print(f"Total time: {t1 - t0:.2f}s")
    print(f"Result keys: {list(res.keys())}")
    
    if "error" in res:
        print(f"Error encountered: {res['error']}")

if __name__ == "__main__":
    test_single_brand()
