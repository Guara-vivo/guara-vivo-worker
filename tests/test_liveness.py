import os
import queue
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import liveness


class FakeConnection:
    """Runs the callback inline, like an I/O loop that is responding."""

    def __init__(self, error: Exception | None = None):
        self.calls = 0
        self.error = error

    def add_callback_threadsafe(self, callback):
        if self.error is not None:
            raise self.error
        self.calls += 1
        callback()


class SilentConnection:
    """Accepts callbacks but never runs them, like an I/O loop that is stuck."""

    def add_callback_threadsafe(self, callback):
        pass


class IOLoopConnection:
    """Runs callbacks on its own thread, like pika's connection thread."""

    def __init__(self):
        self.tasks: queue.Queue = queue.Queue()
        self.thread = threading.Thread(target=self._loop, daemon=True)
        self.thread.start()

    def _loop(self):
        while True:
            callback = self.tasks.get()
            if callback is None:
                return
            callback()

    def add_callback_threadsafe(self, callback):
        self.tasks.put(callback)

    def close(self):
        self.tasks.put(None)
        self.thread.join(1)


class LivenessTests(unittest.TestCase):
    def setUp(self):
        self.dir = tempfile.TemporaryDirectory()
        self.addCleanup(self.dir.cleanup)
        self.path = os.path.join(self.dir.name, "worker-alive")

    def read_stamp(self) -> float:
        with open(self.path, encoding="utf-8") as handle:
            return float(handle.read())

    def test_pulse_then_alive(self):
        liveness.write_pulse(self.path)
        self.assertTrue(liveness.is_alive(self.path))

    def test_missing_file_is_not_alive(self):
        self.assertFalse(liveness.is_alive(self.path))

    def test_old_pulse_is_not_alive(self):
        liveness.write_pulse(self.path, now=1000)
        self.assertFalse(liveness.is_alive(self.path, max_age=90, now=1100))
        self.assertTrue(liveness.is_alive(self.path, max_age=90, now=1050))

    def test_future_timestamp_is_not_alive(self):
        # The container clock went backwards: a stamp far in the future must not look healthy.
        liveness.write_pulse(self.path, now=5000)
        self.assertFalse(liveness.is_alive(self.path, max_age=90, now=1000))

    def test_garbage_or_empty_file_is_not_alive(self):
        for content in (b"", b"abc"):
            with self.subTest(content=content):
                with open(self.path, "wb") as handle:
                    handle.write(content)
                self.assertFalse(liveness.is_alive(self.path))

    def test_write_pulse_never_raises(self):
        # The callback runs on the pika I/O thread: an exception there would stop consuming.
        missing_dir = os.path.join(self.dir.name, "does-not-exist", "worker-alive")
        with self.assertLogs("guara-vivo-worker", level="ERROR") as logs:
            liveness.write_pulse(missing_dir)
        self.assertIn("could not write liveness pulse", logs.output[0])

    def test_start_pulse_does_not_stamp_when_the_loop_is_stuck(self):
        # A connection that drops right after connecting must not look healthy on every reconnect:
        # the first stamp has to go through the I/O loop, not be written directly.
        stop = threading.Event()
        thread = liveness.start_pulse(SilentConnection(), stop, path=self.path, interval=60)
        self.addCleanup(thread.join, 1)
        self.addCleanup(stop.set)
        time.sleep(0.1)
        self.assertFalse(liveness.is_alive(self.path))

    def test_start_pulse_requests_first_pulse_without_waiting_an_interval(self):
        connection = FakeConnection()
        stop = threading.Event()
        thread = liveness.start_pulse(connection, stop, path=self.path, interval=60)
        self.addCleanup(thread.join, 1)
        self.addCleanup(stop.set)
        deadline = time.time() + 1
        while not liveness.is_alive(self.path) and time.time() < deadline:
            time.sleep(0.01)
        self.assertTrue(liveness.is_alive(self.path))
        self.assertEqual(connection.calls, 1)

    def test_start_pulse_calls_callback_and_stops(self):
        connection = FakeConnection()
        stop = threading.Event()
        thread = liveness.start_pulse(connection, stop, path=self.path, interval=0.01)
        time.sleep(0.15)
        self.assertGreater(connection.calls, 2)
        stop.set()
        thread.join(1)
        self.assertFalse(thread.is_alive())

    def test_start_pulse_ends_when_connection_closed(self):
        connection = FakeConnection(error=RuntimeError("connection closed"))
        stop = threading.Event()
        thread = liveness.start_pulse(connection, stop, path=self.path, interval=0.01)
        thread.join(1)
        self.assertFalse(thread.is_alive())

    def test_pulse_continues_while_another_thread_blocks(self):
        # A long record job runs in its own thread; the I/O loop must keep the pulse moving.
        connection = IOLoopConnection()
        self.addCleanup(connection.close)
        blocker = threading.Thread(target=time.sleep, args=(0.4,))
        stop = threading.Event()
        thread = liveness.start_pulse(connection, stop, path=self.path, interval=0.02)
        self.addCleanup(thread.join, 1)
        self.addCleanup(stop.set)
        blocker.start()
        time.sleep(0.1)
        first = self.read_stamp()
        time.sleep(0.15)
        second = self.read_stamp()
        blocker.join()
        self.assertGreater(second, first)

    def test_cli_exit_codes(self):
        env = {**os.environ, "WORKER_LIVENESS_FILE": self.path}
        run = lambda: subprocess.run(
            [sys.executable, "liveness.py"], cwd=ROOT, env=env, capture_output=True, text=True
        )
        missing = run()
        self.assertEqual(missing.returncode, 1)
        self.assertNotIn("Traceback", missing.stderr)
        liveness.write_pulse(self.path)
        fresh = run()
        self.assertEqual(fresh.returncode, 0)
        self.assertNotIn("Traceback", fresh.stderr)


class DockerfileTests(unittest.TestCase):
    """No Docker on the dev machine: guard the image definition by reading the Dockerfile."""

    @classmethod
    def setUpClass(cls):
        cls.lines = (ROOT / "Dockerfile").read_text(encoding="utf-8").splitlines()

    def test_image_ships_the_liveness_module(self):
        # main.py imports liveness: an image without it would crash on start.
        copies = [line for line in self.lines if line.strip().startswith("COPY ")]
        self.assertTrue(any("liveness.py" in line.split() for line in copies))

    def test_healthcheck_runs_liveness_with_the_planned_timings(self):
        checks = [line for line in self.lines if line.strip().startswith("HEALTHCHECK")]
        self.assertEqual(len(checks), 1)
        check = checks[0]
        for expected in ("--interval=30s", "--timeout=5s", "--start-period=60s", "--retries=3", "liveness.py"):
            self.assertIn(expected, check)


if __name__ == "__main__":
    unittest.main()
