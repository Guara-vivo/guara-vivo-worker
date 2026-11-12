FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY main.py liveness.py ./

RUN useradd --create-home --shell /bin/bash appuser \
    && chown -R appuser:appuser /app
USER appuser

# Healthy while the RabbitMQ connection keeps /tmp/worker-alive fresh (see liveness.py).
HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 CMD ["python", "liveness.py"]

CMD ["python", "main.py"]
