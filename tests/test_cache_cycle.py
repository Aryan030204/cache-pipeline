import os
import sys
import threading
import time
import unittest
from unittest import mock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import app as cache_app  # noqa: E402


BRANDS = {f"B{i}": {"tag": f"B{i}"} for i in range(7)}


class _Tracker:
    """Counts how many distinct brands have fetches in flight at the same time."""

    def __init__(self, fail_for=None):
        self.lock = threading.Lock()
        self.active = {}
        self.max_brands = 0
        self.calls = 0
        self.fail_for = fail_for or set()

    def fetch(self, brand, cfg, date_str):
        with self.lock:
            self.calls += 1
            self.active[brand] = self.active.get(brand, 0) + 1
            self.max_brands = max(self.max_brands, len(self.active))
        time.sleep(0.01)
        with self.lock:
            self.active[brand] -= 1
            if not self.active[brand]:
                del self.active[brand]
        if brand in self.fail_for:
            raise RuntimeError("boom")
        return {"ok": True}


def _patches(tracker):
    return [
        mock.patch.object(cache_app, "fetch_active_brands", return_value=BRANDS),
        mock.patch.object(cache_app, "fetch_metrics_for_brand", side_effect=tracker.fetch),
        mock.patch.object(cache_app, "fetch_hourly_metrics_for_brand", side_effect=tracker.fetch),
        mock.patch.object(
            cache_app, "fetch_date_preset_metrics_for_brand",
            side_effect=lambda brand, cfg, anchor: tracker.fetch(brand, cfg, "presets"),
        ),
        mock.patch.object(cache_app, "atomic_cache_replace", return_value=True),
        mock.patch.object(cache_app, "delete_cache_key"),
    ]


class BoundedConcurrencyTests(unittest.TestCase):
    def _run(self, tracker, env):
        patches = _patches(tracker)
        for p in patches:
            p.start()
        self.addCleanup(lambda: [p.stop() for p in patches])
        with mock.patch.dict(os.environ, env):
            return cache_app.fetch_and_cache_all()

    def test_only_one_batch_of_brands_is_in_flight_at_a_time(self):
        tracker = _Tracker()
        results = self._run(tracker, {"PIPELINE_BRAND_BATCH": "2", "PIPELINE_MAX_WORKERS": "8"})

        self.assertLessEqual(tracker.max_brands, 2)
        self.assertEqual(set(results), set(BRANDS))
        # 5 daily + 2 hourly + 1 presets per brand
        self.assertEqual(tracker.calls, len(BRANDS) * 8)

    def test_every_date_of_every_brand_is_still_cached(self):
        tracker = _Tracker()
        results = self._run(tracker, {"PIPELINE_BRAND_BATCH": "3"})

        for brand in BRANDS:
            self.assertEqual(len(results[brand]), 5 + 2 + 1, brand)
            self.assertTrue(all(v.startswith("OK") for v in results[brand].values()), results[brand])

    def test_a_failing_brand_does_not_stop_the_others(self):
        tracker = _Tracker(fail_for={"B3"})
        results = self._run(tracker, {"PIPELINE_BRAND_BATCH": "2"})

        self.assertEqual(set(results), set(BRANDS))
        self.assertTrue(all("boom" in v for v in results["B3"].values()))
        self.assertTrue(all(v.startswith("OK") for v in results["B0"].values()))


class OneCycleAtATimeTests(unittest.TestCase):
    def test_second_cycle_is_skipped_while_one_is_running(self):
        started, release = threading.Event(), threading.Event()

        def slow_cycle():
            started.set()
            release.wait(5)
            return {"done": True}

        with mock.patch.object(cache_app, "fetch_and_cache_all", side_effect=slow_cycle) as cycle:
            first = []
            t = threading.Thread(target=lambda: first.append(cache_app.run_cache_cycle_guarded("a")))
            t.start()
            self.assertTrue(started.wait(2))

            self.assertIsNone(cache_app.run_cache_cycle_guarded("b"))

            release.set()
            t.join(5)
            self.assertEqual(first, [{"done": True}])
            self.assertEqual(cycle.call_count, 1)

        # the lock is released afterwards, even after an error
        with mock.patch.object(cache_app, "fetch_and_cache_all", side_effect=RuntimeError("x")):
            with self.assertRaises(RuntimeError):
                cache_app.run_cache_cycle_guarded("c")
        with mock.patch.object(cache_app, "fetch_and_cache_all", return_value={}):
            self.assertEqual(cache_app.run_cache_cycle_guarded("d"), {})

    def test_trigger_endpoint_returns_immediately_and_runs_in_background(self):
        release = threading.Event()
        ran = threading.Event()

        def slow_cycle():
            ran.set()
            release.wait(5)
            return {}

        client = cache_app.app.test_client()
        with mock.patch.object(cache_app, "fetch_and_cache_all", side_effect=slow_cycle):
            begun = time.monotonic()
            resp = client.get("/trigger-pipeline")
            self.assertEqual(resp.status_code, 202)
            self.assertEqual(resp.get_json(), {"status": "started"})
            self.assertLess(time.monotonic() - begun, 1.0)
            self.assertTrue(ran.wait(2))

            again = client.get("/trigger-pipeline")
            self.assertEqual(again.status_code, 200)
            self.assertEqual(again.get_json(), {"status": "already_running"})

            release.set()
            for _ in range(100):
                if not cache_app._cycle_lock.locked():
                    break
                time.sleep(0.02)
            self.assertFalse(cache_app._cycle_lock.locked())

    def test_wait_mode_keeps_the_synchronous_response(self):
        client = cache_app.app.test_client()
        with mock.patch.object(cache_app, "fetch_and_cache_all", return_value={"B0": {"d": "OK"}}):
            resp = client.get("/trigger-pipeline?wait=1")
        self.assertEqual(resp.status_code, 200)
        self.assertEqual(resp.get_json(), {"status": "ok", "results": {"B0": {"d": "OK"}}})


class StartupTests(unittest.TestCase):
    def test_startup_run_is_opt_in_delayed_and_guarded(self):
        self.assertEqual(os.getenv("RUN_ON_STARTUP", "false").lower() == "true", False)
        with mock.patch.object(cache_app, "STARTUP_DELAY_SECONDS", 7), \
             mock.patch.object(cache_app.time, "sleep") as sleep, \
             mock.patch.object(cache_app, "run_cache_cycle_guarded") as guarded:
            cache_app._run_pipeline_on_startup()
        sleep.assert_called_once_with(7)
        guarded.assert_called_once_with("startup")


if __name__ == "__main__":
    unittest.main()
