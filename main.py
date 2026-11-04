import ipaddress
import json
import logging
import os
import re
import shutil
import socket
import tempfile
import threading
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

import pika
import psycopg2
import psycopg2.extras
import psycopg2.pool
import requests
from dotenv import load_dotenv


load_dotenv()


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("guara-vivo-worker")


class RecordAlreadyProcessing(Exception):
    pass


DATABASE_URL = os.getenv("DATABASE_URL")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD")
QUEUE_NAME = os.getenv("QUEUE_NAME", "guara-vermelho-inference")
ERROR_QUEUE_NAME = os.getenv("ERROR_QUEUE_NAME", f"{QUEUE_NAME}-error")
IA_API_URL = os.getenv("IA_API_URL", "http://ia-api:8000/guara-vermelho/inference")
DEBUG_SAVE_IMAGES_DIR = os.getenv("DEBUG_SAVE_IMAGES_DIR", "").strip()

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RABBITMQ_RECONNECT_SECONDS = int(os.getenv("RABBITMQ_RECONNECT_SECONDS", "5"))
DOWNLOAD_TIMEOUT_SECONDS = int(os.getenv("DOWNLOAD_TIMEOUT_SECONDS", "30"))
IA_TIMEOUT_SECONDS = int(os.getenv("IA_TIMEOUT_SECONDS", "60"))
ALLOWED_IMAGE_HOSTS = {
    value.strip().lower()
    for value in os.getenv("ALLOWED_IMAGE_HOSTS", "").split(",")
    if value.strip()
}
MAX_IMAGE_BYTES = int(os.getenv("MAX_IMAGE_BYTES", str(10 * 1024 * 1024)))
RABBITMQ_HEARTBEAT_SECONDS = int(os.getenv("RABBITMQ_HEARTBEAT_SECONDS", "600"))
RABBITMQ_BLOCKED_CONNECTION_TIMEOUT_SECONDS = int(
    os.getenv("RABBITMQ_BLOCKED_CONNECTION_TIMEOUT_SECONDS", "300")
)
DEBUG_MAX_RUNS = int(os.getenv("DEBUG_MAX_RUNS", "20"))
DATABASE_POOL_MAX = int(os.getenv("DATABASE_POOL_MAX", "3"))
PROGRESS_NOTIFY_CHANNEL = os.getenv("PROGRESS_NOTIFY_CHANNEL", "record_progress")
ALLOWED_IMAGE_MIME_TYPES = {"image/jpeg", "image/png", "image/webp"}
URL_PATTERN = re.compile(r"https://[^\s'\"<>]+")


if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")

if not RABBITMQ_USER or not RABBITMQ_PASSWORD:
    raise RuntimeError("RABBITMQ_USER and RABBITMQ_PASSWORD are required")


def normalize_database_url(database_url: str) -> str:
    if database_url.startswith("postgres://"):
        return database_url.replace("postgres://", "postgresql://", 1)
    if database_url.startswith("postgresql+psycopg2://"):
        return database_url.replace("postgresql+psycopg2://", "postgresql://", 1)
    if database_url.startswith("postgresql://"):
        return database_url
    raise RuntimeError("DATABASE_URL must be a PostgreSQL connection string")


DATABASE_URL = normalize_database_url(DATABASE_URL)


_db_pool: psycopg2.pool.ThreadedConnectionPool | None = None
_db_pool_lock = threading.Lock()


def get_db_pool() -> psycopg2.pool.ThreadedConnectionPool:
    global _db_pool
    with _db_pool_lock:
        if _db_pool is None:
            _db_pool = psycopg2.pool.ThreadedConnectionPool(1, DATABASE_POOL_MAX, DATABASE_URL)
        return _db_pool


def _checkout_connection():
    """Borrow a live connection; connections the server dropped while idle are replaced."""
    pool = get_db_pool()
    for _ in range(DATABASE_POOL_MAX + 1):
        conn = pool.getconn()
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
            conn.rollback()
            return pool, conn
        except Exception:
            pool.putconn(conn, close=True)
    raise RuntimeError("could not obtain a working database connection")


@contextmanager
def db_connection():
    pool, conn = _checkout_connection()
    try:
        yield conn
        conn.commit()
    except Exception:
        if not conn.closed:
            conn.rollback()
        raise
    finally:
        pool.putconn(conn, close=bool(conn.closed))


@contextmanager
def record_processing_lock(record_id: int):
    conn = psycopg2.connect(DATABASE_URL)
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s)", (record_id,))
            locked = cursor.fetchone()[0]
            if not locked:
                raise RecordAlreadyProcessing(f"record {record_id} is already being processed")
        yield
    finally:
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT pg_advisory_unlock(%s)", (record_id,))
            conn.commit()
        finally:
            conn.close()


def connect_rabbitmq() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=RABBITMQ_HEARTBEAT_SECONDS,
        blocked_connection_timeout=RABBITMQ_BLOCKED_CONNECTION_TIMEOUT_SECONDS,
    )
    return pika.BlockingConnection(parameters)


def setup_channel(connection: pika.BlockingConnection) -> pika.channel.Channel:
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_declare(queue=ERROR_QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)
    return channel


def notify_record_progress(cursor, record_id: int, user_id: int, status: str, progress: int) -> None:
    payload = {
        "record_id": record_id,
        "user_id": user_id,
        "status": status,
        "analysis_progress": progress,
    }
    cursor.execute(
        "SELECT pg_notify(%s, %s)",
        (PROGRESS_NOTIFY_CHANNEL, json.dumps(payload, ensure_ascii=False)),
    )


def set_record_progress(record_id: int, status: str, progress: int) -> None:
    normalized_progress = max(0, min(progress, 100))

    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                UPDATE records
                SET status = %s,
                    analysis_progress = %s
                WHERE id = %s
                RETURNING user_id
                """,
                (status, normalized_progress, record_id),
            )
            row = cursor.fetchone()

            if row is not None:
                notify_record_progress(cursor, record_id, row[0], status, normalized_progress)


def sanitize_url(value: str) -> str:
    parsed = urlparse(value)
    if parsed.scheme == "https" and parsed.netloc:
        return parsed._replace(query="", fragment="").geturl()
    return value


def sanitize_text(value: str) -> str:
    return URL_PATTERN.sub(lambda match: sanitize_url(match.group(0)), value)


def sanitize_payload(value: Any) -> Any:
    if isinstance(value, str):
        return sanitize_text(value)
    if isinstance(value, dict):
        return {key: sanitize_payload(item) for key, item in value.items()}
    if isinstance(value, list):
        return [sanitize_payload(item) for item in value]
    return value


def build_error_payload(message: dict[str, Any], error: str) -> dict[str, Any]:
    return {
        "message": sanitize_payload(message),
        "error": sanitize_text(error),
        "failed_at": datetime.now(timezone.utc).isoformat(),
    }


def publish_error(channel: pika.channel.Channel, message: dict[str, Any], error: str) -> None:
    payload = build_error_payload(message, error)
    channel.basic_publish(
        exchange="",
        routing_key=ERROR_QUEUE_NAME,
        body=json.dumps(payload).encode("utf-8"),
        properties=pika.BasicProperties(
            delivery_mode=pika.DeliveryMode.Persistent,
            content_type="application/json",
        ),
    )


def parse_message(body: bytes) -> dict[str, Any]:
    try:
        message = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError as exc:
        raise ValueError("message body must be valid JSON") from exc

    record_id = message.get("record_id")
    if not isinstance(record_id, int):
        raise ValueError("message must contain integer record_id")

    return message


def fetch_record(record_id: int) -> dict[str, Any]:
    with db_connection() as conn:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
            cursor.execute(
                """
                UPDATE records
                SET status = 'processing',
                    analysis_progress = 5
                WHERE id = %s
                  AND status IN ('pending', 'failed', 'processing')
                RETURNING id, images, latitude_camera, longitude_camera, date_time, user_id, status, analysis_progress
                """,
                (record_id,),
            )
            record = cursor.fetchone()

            if record is not None:
                notify_record_progress(
                    cursor,
                    record["id"],
                    record["user_id"],
                    record["status"],
                    record["analysis_progress"],
                )

    if record is None:
        raise ValueError(f"record {record_id} not found or not available for processing")
    if not record["images"]:
        raise ValueError(f"record {record_id} has no images")

    return dict(record)


def validate_image_url(image_url: str) -> None:
    if not ALLOWED_IMAGE_HOSTS:
        raise ValueError("ALLOWED_IMAGE_HOSTS must be configured")

    parsed = urlparse(image_url)

    if parsed.scheme != "https":
        raise ValueError(f"image URL must use https: {sanitize_url(image_url)}")

    if not parsed.hostname:
        raise ValueError(f"image URL must contain hostname: {sanitize_url(image_url)}")

    hostname = parsed.hostname.lower()

    if ALLOWED_IMAGE_HOSTS and hostname not in ALLOWED_IMAGE_HOSTS:
        raise ValueError(f"image host is not allowed: {hostname}")

    for result in socket.getaddrinfo(hostname, None):
        ip = ipaddress.ip_address(result[4][0])
        if ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved:
            raise ValueError(f"image URL resolves to unsafe IP: {hostname}")


def normalize_content_type(content_type: str) -> str:
    return content_type.lower().split(";", 1)[0].strip()


def detect_image_mime(file_bytes: bytes) -> str | None:
    if file_bytes.startswith(b"\xff\xd8\xff"):
        return "image/jpeg"
    if file_bytes.startswith(b"\x89PNG\r\n\x1a\n"):
        return "image/png"
    if len(file_bytes) >= 12 and file_bytes.startswith(b"RIFF") and file_bytes[8:12] == b"WEBP":
        return "image/webp"
    return None


def validate_downloaded_image(image_path: Path, expected_mime: str) -> None:
    with image_path.open("rb") as file_obj:
        header = file_obj.read(16)

    actual_mime = detect_image_mime(header)
    if actual_mime is None:
        raise ValueError(f"image signature is not allowed: {image_path.name}")
    if actual_mime != expected_mime:
        raise ValueError(f"image signature does not match MIME type: {image_path.name}")


def update_record_status(record_id: int, status: str) -> None:
    progress = 100 if status in {"completed", "failed"} else 0
    set_record_progress(record_id, status, progress)


def guess_image_extension(content_type: str) -> str:
    content_type = normalize_content_type(content_type)
    return {
        "image/jpeg": ".jpg",
        "image/png": ".png",
        "image/webp": ".webp",
    }.get(content_type, ".img")


def mime_type_for_image(image_path: Path) -> str:
    return {
        ".jpg": "image/jpeg",
        ".jpeg": "image/jpeg",
        ".png": "image/png",
        ".webp": "image/webp",
    }.get(image_path.suffix.lower(), "application/octet-stream")


def download_images(image_urls: list[str], directory: Path) -> list[Path]:
    image_paths = []

    for index, image_url in enumerate(image_urls, start=1):
        validate_image_url(image_url)

        try:
            with requests.get(
                image_url,
                timeout=DOWNLOAD_TIMEOUT_SECONDS,
                stream=True,
                allow_redirects=False,
            ) as response:
                if 300 <= response.status_code < 400:
                    raise ValueError(f"image redirect is not allowed: {sanitize_url(image_url)}")

                response.raise_for_status()

                content_type = normalize_content_type(response.headers.get("content-type", ""))
                if content_type not in ALLOWED_IMAGE_MIME_TYPES:
                    raise ValueError(f"image MIME type is not allowed: {content_type or 'missing'}")

                image_path = directory / f"image_{index}{guess_image_extension(content_type)}"
                total_bytes = 0

                with image_path.open("wb") as file_obj:
                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        if not chunk:
                            continue

                        total_bytes += len(chunk)
                        if total_bytes > MAX_IMAGE_BYTES:
                            raise ValueError(f"image exceeds max size: {sanitize_url(image_url)}")

                        file_obj.write(chunk)
        except requests.RequestException:
            raise ValueError(f"could not download image: {sanitize_url(image_url)}") from None

        if image_path.stat().st_size == 0:
            raise ValueError(f"downloaded empty image: {sanitize_url(image_url)}")

        validate_downloaded_image(image_path, content_type)

        image_paths.append(image_path)

    return image_paths


def cleanup_debug_dir() -> None:
    if not DEBUG_SAVE_IMAGES_DIR:
        return

    base_dir = Path(DEBUG_SAVE_IMAGES_DIR)
    if not base_dir.exists():
        return

    debug_dirs = sorted(
        [path for path in base_dir.iterdir() if path.is_dir()],
        key=lambda path: path.stat().st_mtime,
        reverse=True,
    )

    for old_dir in debug_dirs[DEBUG_MAX_RUNS:]:
        shutil.rmtree(old_dir, ignore_errors=True)


def save_debug_images(record_id: int, image_paths: list[Path], image_urls: list[str]) -> Path | None:
    if not DEBUG_SAVE_IMAGES_DIR:
        return None

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    debug_dir = Path(DEBUG_SAVE_IMAGES_DIR) / f"record_{record_id}_{timestamp}"

    try:
        debug_dir.mkdir(parents=True, exist_ok=True)
        metadata = []
        for index, image_path in enumerate(image_paths):
            debug_image_path = debug_dir / image_path.name
            shutil.copy2(image_path, debug_image_path)
            metadata.append(
                {
                    "source_url": sanitize_url(image_urls[index]) if index < len(image_urls) else None,
                    "saved_file": debug_image_path.name,
                    "size_bytes": debug_image_path.stat().st_size,
                }
            )

        (debug_dir / "metadata.json").write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("saved %s debug image(s) for record %s to %s", len(image_paths), record_id, debug_dir)
        cleanup_debug_dir()
        return debug_dir
    except Exception:
        logger.exception("could not save debug images for record %s", record_id)
        return None


def call_ia_api(
    image_paths: list[Path],
    debug_dir: Path | None = None,
    progress_callback: Callable[[int], None] | None = None,
) -> dict[str, Any]:
    image_results = []
    all_guaras = []
    total_guaras = 0

    total_images = len(image_paths)
    for image_index, image_path in enumerate(image_paths, start=1):
        with image_path.open("rb") as file_obj:
            response = requests.post(
                IA_API_URL,
                files={"image": (image_path.name, file_obj, mime_type_for_image(image_path))},
                timeout=IA_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

        image_result = response.json()
        image_results.append(image_result)
        if debug_dir is not None:
            (debug_dir / f"ia_result_{len(image_results)}.json").write_text(
                json.dumps(image_result, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        guaras = image_result.get("guaras")
        if isinstance(guaras, list):
            all_guaras.extend(item for item in guaras if isinstance(item, dict))

        quantidade_guaras = image_result.get("quantidade_guaras")
        if isinstance(quantidade_guaras, int):
            total_guaras += quantidade_guaras
        elif isinstance(guaras, list):
            total_guaras += len([item for item in guaras if isinstance(item, dict)])

        if progress_callback is not None and total_images > 0:
            progress = 25 + round((image_index / total_images) * 60)
            progress_callback(min(progress, 85))

    result = {
        "quantidade_guaras": total_guaras,
        "guaras": all_guaras,
        "imagens": image_results,
    }
    if debug_dir is not None:
        (debug_dir / "ia_result_aggregated.json").write_text(
            json.dumps(result, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
    return result


def extract_ibis_items(ia_result: dict[str, Any]) -> list[dict[str, Any]]:
    value = ia_result.get("guaras")
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    return []


def extract_ibis_quantity(ia_result: dict[str, Any], ibis_items: list[dict[str, Any]]) -> int:
    value = ia_result.get("quantidade_guaras")
    if isinstance(value, int):
        return value
    return len(ibis_items)


def save_analysis(record: dict[str, Any], ia_result: dict[str, Any]) -> None:
    ibis_items = extract_ibis_items(ia_result)
    ibis_quantity = extract_ibis_quantity(ia_result, ibis_items)

    with db_connection() as conn:
        with conn.cursor() as cursor:
            # Create or update aggregate analysis
            cursor.execute(
                """
                INSERT INTO analyses (ibis_quantity, datetime, recorder_id)
                VALUES (%s, %s, %s)
                ON CONFLICT (recorder_id) DO UPDATE SET
                    ibis_quantity = EXCLUDED.ibis_quantity,
                    datetime = EXCLUDED.datetime
                RETURNING id
                """,
                (
                    ibis_quantity,
                    record["date_time"],
                    record["id"],
                ),
            )
            analysis_id = cursor.fetchone()[0]

            # Delete existing analysis_images and associated ibis to ensure idempotency
            cursor.execute("DELETE FROM ibis WHERE analysis_id = %s", (analysis_id,))
            cursor.execute("DELETE FROM analysis_images WHERE analysis_id = %s", (analysis_id,))

            # Insert aggregate ibis items for backward compatibility
            for ibis_item in ibis_items:
                cursor.execute(
                    """
                    INSERT INTO ibis (color, age_group, analysis_id)
                    VALUES (%s, %s, %s)
                    """,
                    (
                        ibis_item.get("cor"),
                        ibis_item.get("fase_vida"),
                        analysis_id,
                    ),
                )

            # Process per-image analysis
            image_results = ia_result.get("imagens", [])
            for image_index, image_result in enumerate(image_results):
                image_url = record["images"][image_index] if image_index < len(record["images"]) else ""
                image_ibis_items = [item for item in image_result.get("guaras", []) if isinstance(item, dict)]
                image_ibis_quantity = image_result.get("quantidade_guaras", len(image_ibis_items))
                raw_result = json.dumps(
                    {
                        "image_index": image_index,
                        "image_url": image_url,
                        "ibis_quantity": image_ibis_quantity,
                        "result": image_result,
                    },
                    ensure_ascii=False,
                )

                # Insert analysis_image
                cursor.execute(
                    """
                    INSERT INTO analysis_images 
                    (analysis_id, record_id, image_index, image_url, ibis_quantity, raw_result, created_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    RETURNING id
                    """,
                    (
                        analysis_id,
                        record["id"],
                        image_index,
                        image_url,
                        image_ibis_quantity,
                        raw_result,
                        datetime.now(timezone.utc),
                    ),
                )
                analysis_image_id = cursor.fetchone()[0]

                # Insert per-image ibis detections
                for ibis_item in image_ibis_items:
                    cursor.execute(
                        """
                        INSERT INTO ibis (color, age_group, analysis_id, analysis_image_id, raw_detection)
                        VALUES (%s, %s, %s, %s, %s)
                        """,
                        (
                            ibis_item.get("cor"),
                            ibis_item.get("fase_vida"),
                            analysis_id,
                            analysis_image_id,
                            json.dumps(ibis_item, ensure_ascii=False),
                        ),
                    )

            cursor.execute(
                """
                UPDATE records
                SET status = 'completed',
                    analysis_progress = 100
                WHERE id = %s
                """,
                (record["id"],),
            )
            notify_record_progress(cursor, record["id"], record["user_id"], "completed", 100)


def process_record(record_id: int) -> None:
    with record_processing_lock(record_id):
        work_dir = Path(tempfile.mkdtemp(prefix=f"guara_record_{record_id}_"))
        try:
            record = fetch_record(record_id)
            image_paths = download_images(record["images"], work_dir)
            set_record_progress(record_id, "processing", 20)
            debug_dir = save_debug_images(record_id, image_paths, record["images"])
            ia_result = call_ia_api(
                image_paths,
                debug_dir,
                lambda progress: set_record_progress(record_id, "processing", progress),
            )
            set_record_progress(record_id, "processing", 90)
            save_analysis(record, ia_result)
            logger.info("record %s processed successfully", record_id)
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)


def process_with_retries(record_id: int) -> None:
    last_error = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            process_record(record_id)
            return
        except RecordAlreadyProcessing:
            logger.info("record %s is already being processed", record_id)
            return
        except Exception as exc:
            last_error = exc
            logger.exception("record %s failed on attempt %s/%s", record_id, attempt, MAX_RETRIES)
            if attempt < MAX_RETRIES:
                time.sleep(2 ** (attempt - 1))

    update_record_status(record_id, "failed")
    raise RuntimeError(f"record {record_id} failed after {MAX_RETRIES} attempts") from last_error


def process_message_body(body: bytes) -> tuple[dict[str, Any] | None, Exception | None]:
    """Run the job. Touches neither the channel nor the connection, so it can run in a thread."""
    message: dict[str, Any] | None = None
    try:
        message = parse_message(body)
        process_with_retries(message["record_id"])
        return message, None
    except Exception as exc:
        logger.exception("message processing failed")
        return message, exc


def finish_message(channel, method, message: dict[str, Any] | None, error: Exception | None) -> None:
    """Answer the broker. Must run on the connection's thread (pika is not thread-safe).

    If the error cannot be published the message is left unacked, so the broker redelivers it.
    """
    if error is not None and message is not None:
        publish_error(channel, message, str(error))
    channel.basic_ack(delivery_tag=method.delivery_tag)


def handle_message(channel, method, properties, body) -> None:
    message, error = process_message_body(body)
    finish_message(channel, method, message, error)


def make_threaded_handler(connection: pika.BlockingConnection, active_jobs: list[threading.Thread]):
    """Consumer callback that runs each job in a thread.

    Running a long job inside the callback would stop pika from answering broker heartbeats
    (a record with many images can take minutes), and the broker would drop the connection.
    The connection thread keeps serving I/O while the job runs; the ack goes back through
    `add_callback_threadsafe`. With prefetch_count=1 there is at most one job at a time.
    """

    def on_message(channel, method, properties, body) -> None:
        def job() -> None:
            message, error = process_message_body(body)
            try:
                connection.add_callback_threadsafe(
                    lambda: finish_message(channel, method, message, error)
                )
            except Exception:
                # Connection is gone: the broker redelivers; the advisory lock prevents double work.
                logger.exception("could not answer the broker for record message")

        thread = threading.Thread(target=job, name="record-job", daemon=False)
        active_jobs[:] = [item for item in active_jobs if item.is_alive()]
        active_jobs.append(thread)
        thread.start()

    return on_message


def run() -> None:
    logger.info("starting worker. queue=%s ia_api=%s", QUEUE_NAME, IA_API_URL)
    # Shared across reconnects: a job started on a dropped connection may still be running.
    active_jobs: list[threading.Thread] = []
    while True:
        try:
            connection = connect_rabbitmq()
            channel = setup_channel(connection)
            channel.basic_consume(
                queue=QUEUE_NAME,
                on_message_callback=make_threaded_handler(connection, active_jobs),
            )
            logger.info("waiting for messages")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            logger.exception("rabbitmq connection failed, retrying in %s seconds", RABBITMQ_RECONNECT_SECONDS)
            time.sleep(RABBITMQ_RECONNECT_SECONDS)
        except KeyboardInterrupt:
            logger.info("worker stopped")
            break

    # Let a running job finish its DB writes before the process exits.
    for job in active_jobs:
        job.join(timeout=30)


if __name__ == "__main__":
    run()
