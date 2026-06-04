import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


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


if __name__ == "__main__":
    unittest.main()
