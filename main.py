import json
import logging
import os
import shutil
import tempfile
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pika
import psycopg2
import psycopg2.extras
import requests
from dotenv import load_dotenv


load_dotenv()


LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("guara-vivo-worker")


DATABASE_URL = os.getenv("DATABASE_URL")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "rabbitmq")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", "5672"))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASSWORD = os.getenv("RABBITMQ_PASSWORD", "guest")
QUEUE_NAME = os.getenv("QUEUE_NAME", "guara-vermelho-inference")
ERROR_QUEUE_NAME = os.getenv("ERROR_QUEUE_NAME", f"{QUEUE_NAME}-error")
IA_API_URL = os.getenv("IA_API_URL", "http://ia-api:8000/guara-vermelho/inference")
DEBUG_SAVE_IMAGES_DIR = os.getenv("DEBUG_SAVE_IMAGES_DIR", "").strip()

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RABBITMQ_RECONNECT_SECONDS = int(os.getenv("RABBITMQ_RECONNECT_SECONDS", "5"))
DOWNLOAD_TIMEOUT_SECONDS = int(os.getenv("DOWNLOAD_TIMEOUT_SECONDS", "30"))
IA_TIMEOUT_SECONDS = int(os.getenv("IA_TIMEOUT_SECONDS", "60"))


if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL environment variable is required")


def normalize_database_url(database_url: str) -> str:
    if database_url.startswith("postgres://"):
        return database_url.replace("postgres://", "postgresql://", 1)
    if database_url.startswith("postgresql+psycopg2://"):
        return database_url.replace("postgresql+psycopg2://", "postgresql://", 1)
    if database_url.startswith("postgresql://"):
        return database_url
    raise RuntimeError("DATABASE_URL must be a PostgreSQL connection string")


DATABASE_URL = normalize_database_url(DATABASE_URL)


@contextmanager
def db_connection():
    conn = psycopg2.connect(DATABASE_URL)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def connect_rabbitmq() -> pika.BlockingConnection:
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASSWORD)
    parameters = pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=60,
        blocked_connection_timeout=300,
    )
    return pika.BlockingConnection(parameters)


def setup_channel(connection: pika.BlockingConnection) -> pika.channel.Channel:
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_declare(queue=ERROR_QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)
    return channel


def publish_error(channel: pika.channel.Channel, message: dict[str, Any], error: str) -> None:
    payload = {
        "message": message,
        "error": error,
        "failed_at": datetime.utcnow().isoformat(),
    }
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
                SET status = 'processing'
                WHERE id = %s
                RETURNING id, images, latitude_camera, longitude_camera, date_time
                """,
                (record_id,),
            )
            record = cursor.fetchone()

    if record is None:
        raise ValueError(f"record {record_id} not found")
    if not record["images"]:
        raise ValueError(f"record {record_id} has no images")

    return dict(record)


def update_record_status(record_id: int, status: str) -> None:
    with db_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                "UPDATE records SET status = %s WHERE id = %s",
                (status, record_id),
            )


def download_images(image_urls: list[str], directory: Path) -> list[Path]:
    image_paths = []
    for index, image_url in enumerate(image_urls, start=1):
        response = requests.get(image_url, timeout=DOWNLOAD_TIMEOUT_SECONDS)
        response.raise_for_status()

        content_type = response.headers.get("content-type", "")
        if content_type and not content_type.lower().startswith("image/"):
            raise ValueError(f"URL is not an image: {image_url}")

        image_path = directory / f"image_{index}.jpg"
        image_path.write_bytes(response.content)
        if image_path.stat().st_size == 0:
            raise ValueError(f"downloaded empty image: {image_url}")
        image_paths.append(image_path)

    return image_paths


def save_debug_images(record_id: int, image_paths: list[Path], image_urls: list[str]) -> None:
    if not DEBUG_SAVE_IMAGES_DIR:
        return

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
                    "source_url": image_urls[index] if index < len(image_urls) else None,
                    "saved_file": debug_image_path.name,
                    "size_bytes": debug_image_path.stat().st_size,
                }
            )

        (debug_dir / "metadata.json").write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        logger.info("saved %s debug image(s) for record %s to %s", len(image_paths), record_id, debug_dir)
    except Exception:
        logger.exception("could not save debug images for record %s", record_id)


def call_ia_api(image_paths: list[Path]) -> dict[str, Any]:
    image_results = []
    all_guaras = []
    total_guaras = 0

    for image_path in image_paths:
        with image_path.open("rb") as file_obj:
            response = requests.post(
                IA_API_URL,
                files={"image": (image_path.name, file_obj, "image/jpeg")},
                timeout=IA_TIMEOUT_SECONDS,
            )
            response.raise_for_status()

        image_result = response.json()
        image_results.append(image_result)

        guaras = image_result.get("guaras")
        if isinstance(guaras, list):
            all_guaras.extend(item for item in guaras if isinstance(item, dict))

        quantidade_guaras = image_result.get("quantidade_guaras")
        if isinstance(quantidade_guaras, int):
            total_guaras += quantidade_guaras
        elif isinstance(guaras, list):
            total_guaras += len([item for item in guaras if isinstance(item, dict)])

    return {
        "quantidade_guaras": total_guaras,
        "guaras": all_guaras,
        "imagens": image_results,
    }


def extract_ibis_items(ia_result: dict[str, Any]) -> list[dict[str, Any]]:
    for key in ("guaras"):
        value = ia_result.get(key)
        if isinstance(value, list):
            return [item for item in value if isinstance(item, dict)]
    return []


def extract_ibis_quantity(ia_result: dict[str, Any], ibis_items: list[dict[str, Any]]) -> int:
    for key in ("quantidade_guaras"):
        value = ia_result.get(key)
        if isinstance(value, int):
            return value
    return len(ibis_items)


def save_analysis(record: dict[str, Any], ia_result: dict[str, Any]) -> None:
    ibis_items = extract_ibis_items(ia_result)
    ibis_quantity = extract_ibis_quantity(ia_result, ibis_items)

    with db_connection() as conn:
        with conn.cursor() as cursor:
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

            cursor.execute("DELETE FROM ibis WHERE analysis_id = %s", (analysis_id,))
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

            cursor.execute(
                "UPDATE records SET status = 'completed' WHERE id = %s",
                (record["id"],),
            )


def process_record(record_id: int) -> None:
    work_dir = Path(tempfile.mkdtemp(prefix=f"guara_record_{record_id}_"))
    try:
        record = fetch_record(record_id)
        image_paths = download_images(record["images"], work_dir)
        save_debug_images(record_id, image_paths, record["images"])
        ia_result = call_ia_api(image_paths)
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
        except Exception as exc:
            last_error = exc
            logger.exception("record %s failed on attempt %s/%s", record_id, attempt, MAX_RETRIES)
            if attempt < MAX_RETRIES:
                time.sleep(2 ** (attempt - 1))

    update_record_status(record_id, "failed")
    raise RuntimeError(f"record {record_id} failed after {MAX_RETRIES} attempts") from last_error


def handle_message(channel, method, properties, body) -> None:
    message: dict[str, Any] | None = None
    try:
        message = parse_message(body)
        process_with_retries(message["record_id"])
        channel.basic_ack(delivery_tag=method.delivery_tag)
    except Exception as exc:
        logger.exception("message processing failed")
        if message is not None:
            publish_error(channel, message, str(exc))
        channel.basic_ack(delivery_tag=method.delivery_tag)


def run() -> None:
    logger.info("starting worker. queue=%s ia_api=%s", QUEUE_NAME, IA_API_URL)
    while True:
        try:
            connection = connect_rabbitmq()
            channel = setup_channel(connection)
            channel.basic_consume(queue=QUEUE_NAME, on_message_callback=handle_message)
            logger.info("waiting for messages")
            channel.start_consuming()
        except pika.exceptions.AMQPConnectionError:
            logger.exception("rabbitmq connection failed, retrying in %s seconds", RABBITMQ_RECONNECT_SECONDS)
            time.sleep(RABBITMQ_RECONNECT_SECONDS)
        except KeyboardInterrupt:
            logger.info("worker stopped")
            break


if __name__ == "__main__":
    run()
