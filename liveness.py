"""Worker liveness: a pulse file kept fresh by the RabbitMQ connection, read by the Docker HEALTHCHECK.

Standard library only: the healthcheck runs `python liveness.py`, which must not import `main`
(it needs environment variables and heavy dependencies).
"""
import logging
import os
import sys
import threading
import time

LIVENESS_FILE = os.getenv("WORKER_LIVENESS_FILE", "/tmp/worker-alive")
PULSE_SECONDS = 15.0
MAX_AGE_SECONDS = 90.0

logger = logging.getLogger("guara-vivo-worker")


def write_pulse(path: str = LIVENESS_FILE, now: float | None = None) -> None:
    """Write the current time to `path`. Never raises: it may run on the pika I/O thread."""
    try:
        stamp = time.time() if now is None else now
        with open(path, "w", encoding="utf-8") as handle:
            handle.write(str(stamp))
    except OSError:
        logger.exception("could not write liveness pulse to %s", path)


def is_alive(path: str = LIVENESS_FILE, max_age: float = MAX_AGE_SECONDS, now: float | None = None) -> bool:
    """True only if `path` holds a valid timestamp no more than `max_age` seconds away from `now`."""
    try:
        with open(path, encoding="utf-8") as handle:
            stamp = float(handle.read())
    except (OSError, ValueError):
        return False
    current = time.time() if now is None else now
    return abs(current - stamp) <= max_age


def start_pulse(
    connection,
    stop: threading.Event,
    path: str = LIVENESS_FILE,
    interval: float = PULSE_SECONDS,
) -> threading.Thread:
    """Keep the pulse file fresh through the connection's I/O loop until `stop` or the connection closes.

    The callback runs on the connection thread, so a stuck loop or a dropped connection stops the
    pulse and the file ages. A long record job runs in its own thread and does not stop it.
    Even the first stamp goes through the loop: writing it directly would keep a worker that
    reconnects and drops every few seconds looking healthy.
    """

    def pulse() -> None:
        while True:
            try:
                connection.add_callback_threadsafe(lambda: write_pulse(path))
            except Exception:
                logger.info("connection closed; liveness pulse stopped")
                return
            if stop.wait(interval):
                return

    thread = threading.Thread(target=pulse, name="liveness-pulse", daemon=True)
    thread.start()
    return thread


if __name__ == "__main__":
    sys.exit(0 if is_alive() else 1)
