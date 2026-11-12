## Worker Notes

- Worker consumes RabbitMQ messages with `record_id`, downloads `records.images`, calls the identifier API, and writes aggregate `analyses`, per-image `analysis_images`, and per-detection `ibis` rows.
- Identifier API expects multipart field `image`, one image per request.
- Worker aggregates multiple image responses by summing `quantidade_guaras` and concatenating `guaras`.
- Worker stores each source image response in `analysis_images.raw_result` with `image_index`, `image_url`, and `ibis_quantity`.
- Worker links per-image detections through `ibis.analysis_image_id` and stores the full identifier bird JSON in `ibis.raw_detection`.
- Preserve identifier fields used by the frontend modal: `cor`, `fase_vida`, `acuracia.deteccao_yolo`, `acuracia.classificacao_guara`, `acuracia.classificacao_cor`, and `acuracia.classificacao_fase_vida`.
- After worker changes in Docker, rebuild and recreate containers with `docker compose --env-file .env.docker-compose up -d --build --force-recreate` to avoid stale worker code.
- `DEBUG_SAVE_IMAGES_DIR` saves downloaded images, `metadata.json`, `ia_result_*.json`, and `ia_result_aggregated.json` for debugging.
- Keep `debug-images/` out of Git.

- Each RabbitMQ message runs in its own thread (`make_threaded_handler`), so pika keeps answering broker heartbeats during long jobs. The ack and the error-queue publish go back through `connection.add_callback_threadsafe` (pika is not thread-safe). `prefetch_count=1` keeps it to one job at a time. If the error cannot be published the message stays unacked and is redelivered.
- Postgres access uses a small `ThreadedConnectionPool` (`DATABASE_POOL_MAX`, default 3) with a `SELECT 1` check on checkout, instead of one new connection per call. The per-record advisory lock still uses its own dedicated connection and needs a direct (session-mode) Postgres connection, not Supabase's transaction pooler.
- RabbitMQ's `consumer_timeout` (default 30 min) still bounds one job: many images x `IA_TIMEOUT_SECONDS` must stay below it.
- Healthcheck (`liveness.py`, stdlib only): a thread per RabbitMQ connection asks the connection's I/O loop (`add_callback_threadsafe`) to write the time to `/tmp/worker-alive` every 15 s; the Docker `HEALTHCHECK` runs `python liveness.py` and fails if the file is missing or older than 90 s. The container goes `unhealthy` when the loop stops answering or the connection drops, not while a long job runs (jobs run in their own threads). Keep `liveness.py` standard-library only and never make it import `main` (the healthcheck runs it without the worker's environment variables).
