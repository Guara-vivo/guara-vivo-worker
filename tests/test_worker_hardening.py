import os
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from unittest.mock import patch

import pika


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

os.environ.setdefault("DATABASE_URL", "postgresql://user:password@localhost:5432/db")
os.environ.setdefault("RABBITMQ_USER", "worker")
os.environ.setdefault("RABBITMQ_PASSWORD", "secret")

import main


class FakeResponse:
    def __init__(self, headers=None, chunks=None, status_code=200):
        self.headers = headers or {}
        self.chunks = chunks or []
        self.status_code = status_code

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError("bad status")

    def iter_content(self, chunk_size):
        yield from self.chunks


class WorkerHardeningTests(unittest.TestCase):
    def test_validate_image_url_requires_allowlist(self):
        with patch.object(main, "ALLOWED_IMAGE_HOSTS", set()):
            with self.assertRaisesRegex(ValueError, "ALLOWED_IMAGE_HOSTS"):
                main.validate_image_url("https://example.supabase.co/image.jpg")

    def test_download_rejects_redirects(self):
        response = FakeResponse(
            headers={"location": "https://example.supabase.co/other.jpg"},
            status_code=302,
        )

        with tempfile.TemporaryDirectory() as tmp_dir, \
            patch.object(main, "ALLOWED_IMAGE_HOSTS", {"example.supabase.co"}), \
            patch.object(main.socket, "getaddrinfo", return_value=[(None, None, None, None, ("8.8.8.8", 0))]), \
            patch.object(main.requests, "get", return_value=response):
            with self.assertRaisesRegex(ValueError, "redirect"):
                main.download_images(["https://example.supabase.co/image.jpg"], Path(tmp_dir))

    def test_download_rejects_untrusted_image_mime(self):
        response = FakeResponse(
            headers={"content-type": "image/svg+xml"},
            chunks=[b"<svg></svg>"],
        )

        with tempfile.TemporaryDirectory() as tmp_dir, \
            patch.object(main, "ALLOWED_IMAGE_HOSTS", {"example.supabase.co"}), \
            patch.object(main.socket, "getaddrinfo", return_value=[(None, None, None, None, ("8.8.8.8", 0))]), \
            patch.object(main.requests, "get", return_value=response):
            with self.assertRaisesRegex(ValueError, "MIME"):
                main.download_images(["https://example.supabase.co/image.svg"], Path(tmp_dir))

    def test_download_rejects_mismatched_magic_bytes(self):
        response = FakeResponse(
            headers={"content-type": "image/jpeg"},
            chunks=[b"not-a-jpeg"],
        )

        with tempfile.TemporaryDirectory() as tmp_dir, \
            patch.object(main, "ALLOWED_IMAGE_HOSTS", {"example.supabase.co"}), \
            patch.object(main.socket, "getaddrinfo", return_value=[(None, None, None, None, ("8.8.8.8", 0))]), \
            patch.object(main.requests, "get", return_value=response):
            with self.assertRaisesRegex(ValueError, "signature"):
                main.download_images(["https://example.supabase.co/image.jpg"], Path(tmp_dir))

    def test_error_payload_sanitizes_query_strings(self):
        payload = main.build_error_payload(
            {"record_id": 1, "image_url": "https://host/path.jpg?token=secret"},
            "failed https://host/path.jpg?token=secret",
        )

        encoded = str(payload)
        self.assertNotIn("token=secret", encoded)
        self.assertIn("https://host/path.jpg", encoded)

    def test_handle_message_does_not_ack_when_error_publish_fails(self):
        class Channel:
            def __init__(self):
                self.acked = False

            def basic_ack(self, delivery_tag):
                self.acked = True

        method = type("Method", (), {"delivery_tag": 1})()
        channel = Channel()

        with patch.object(main.logger, "exception"), \
            patch.object(main, "process_with_retries", side_effect=RuntimeError("boom")), \
            patch.object(main, "publish_error", side_effect=RuntimeError("publish failed")):
            with self.assertRaisesRegex(RuntimeError, "publish failed"):
                main.handle_message(channel, method, None, b'{"record_id": 1}')

        self.assertFalse(channel.acked)

    def test_locked_record_is_not_marked_failed(self):
        with patch.object(main.logger, "info"), \
            patch.object(main, "process_record", side_effect=main.RecordAlreadyProcessing("locked")), \
            patch.object(main, "update_record_status") as update_record_status:
            main.process_with_retries(1)

        update_record_status.assert_not_called()

    def test_save_analysis_deletes_ibis_before_analysis_images(self):
        statements = []

        class Cursor:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def execute(self, statement, params=None):
                statements.append(" ".join(statement.split()))

            def fetchone(self):
                return [10]

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def cursor(self):
                return Cursor()

        record = {
            "id": 1,
            "date_time": "2026-06-04T12:00:00Z",
            "images": [],
            "user_id": 1,
        }
        ia_result = {"quantidade_guaras": 0, "guaras": [], "imagens": []}

        with patch.object(main, "db_connection", return_value=Connection()):
            main.save_analysis(record, ia_result)

        ibis_delete = next(i for i, stmt in enumerate(statements) if stmt.startswith("DELETE FROM ibis"))
        images_delete = next(i for i, stmt in enumerate(statements) if stmt.startswith("DELETE FROM analysis_images"))
        self.assertLess(ibis_delete, images_delete)


class ThreadedHandlerTests(unittest.TestCase):
    def test_job_runs_in_a_thread_and_ack_goes_through_the_connection_thread(self):
        import threading

        main_thread = threading.get_ident()
        seen = {}
        done = threading.Event()

        class Channel:
            def __init__(self):
                self.acked_on = None

            def basic_ack(self, delivery_tag):
                self.acked_on = threading.get_ident()

        class Connection:
            def add_callback_threadsafe(self, callback):
                seen["callback"] = callback
                done.set()

        def fake_process(body):
            seen["job_thread"] = threading.get_ident()
            return {"record_id": 1}, None

        channel, jobs = Channel(), []
        method = type("Method", (), {"delivery_tag": 7})()

        with patch.object(main, "process_message_body", side_effect=fake_process):
            main.make_threaded_handler(Connection(), jobs)(channel, method, None, b"{}")
            self.assertTrue(done.wait(5))
            jobs[0].join(5)

        # The callback only returned right away: nothing was acked from the job thread.
        self.assertNotEqual(seen["job_thread"], main_thread)
        self.assertIsNone(channel.acked_on)
        seen["callback"]()  # what the connection thread does
        self.assertEqual(channel.acked_on, main_thread)

    def test_failed_job_publishes_error_then_acks(self):
        order = []

        class Channel:
            def basic_ack(self, delivery_tag):
                order.append("ack")

        method = type("Method", (), {"delivery_tag": 1})()
        with patch.object(main, "publish_error", side_effect=lambda *a: order.append("error")):
            main.finish_message(Channel(), method, {"record_id": 1}, RuntimeError("boom"))
        self.assertEqual(order, ["error", "ack"])

    def test_dead_connection_is_logged_not_raised(self):
        class Connection:
            def add_callback_threadsafe(self, callback):
                raise RuntimeError("connection closed")

        jobs = []
        method = type("Method", (), {"delivery_tag": 1})()
        with patch.object(main, "process_message_body", return_value=({"record_id": 1}, None)),             patch.object(main.logger, "exception") as log:
            main.make_threaded_handler(Connection(), jobs)(object(), method, None, b"{}")
            jobs[0].join(5)
        log.assert_called_once()


class ConnectionPoolTests(unittest.TestCase):
    def test_connections_are_reused_and_dead_ones_replaced(self):
        class Conn:
            def __init__(self, healthy):
                self.healthy, self.closed, self.commits = healthy, 0, 0

            def cursor(self):
                conn = self

                class Cur:
                    def __enter__(self):
                        return self

                    def __exit__(self, *a):
                        return False

                    def execute(self, sql):
                        if not conn.healthy:
                            raise RuntimeError("server closed the connection")

                return Cur()

            def rollback(self):
                pass

            def commit(self):
                self.commits += 1

        class Pool:
            def __init__(self, conns):
                self.conns, self.returned = list(conns), []

            def getconn(self):
                return self.conns.pop(0)

            def putconn(self, conn, close=False):
                self.returned.append((conn, close))

        dead, alive = Conn(False), Conn(True)
        pool = Pool([dead, alive])

        with patch.object(main, "get_db_pool", return_value=pool):
            with main.db_connection() as conn:
                self.assertIs(conn, alive)

        self.assertEqual(pool.returned, [(dead, True), (alive, False)])
        self.assertEqual(alive.commits, 1)


class RunLoopPulseTests(unittest.TestCase):
    """`run()` keeps one liveness pulse per RabbitMQ connection and stops it when the connection ends."""

    def run_with_errors(self, *errors):
        stops = []

        class FakeChannel:
            def __init__(self):
                self.remaining = list(errors)

            def basic_consume(self, **kwargs):
                pass

            def start_consuming(self):
                raise self.remaining.pop(0)

        channel = FakeChannel()

        def fake_start_pulse(connection, stop, *args, **kwargs):
            stops.append(stop)
            return threading.Thread()

        with patch.object(main, "connect_rabbitmq", return_value=object()), \
                patch.object(main, "setup_channel", return_value=channel), \
                patch.object(main.liveness, "start_pulse", side_effect=fake_start_pulse), \
                patch.object(main.time, "sleep"):
            try:
                main.run()
                error = None
            except Exception as exc:
                error = exc
        return stops, error

    def test_run_starts_pulse_for_each_connection_and_stops_it(self):
        stops, error = self.run_with_errors(pika.exceptions.AMQPConnectionError(), KeyboardInterrupt())
        self.assertIsNone(error)
        self.assertEqual(len(stops), 2)
        self.assertTrue(all(stop.is_set() for stop in stops))

    def test_run_stops_pulse_on_unexpected_error(self):
        stops, error = self.run_with_errors(RuntimeError("boom"))
        self.assertIsInstance(error, RuntimeError)
        self.assertEqual(len(stops), 1)
        self.assertTrue(stops[0].is_set())


if __name__ == "__main__":
    unittest.main()
