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

## Response Style

- Minimal output.
- No motivational text.
- No explanations unless requested.
- No step-by-step reasoning unless requested.
- Prioritize action over commentary.

## Tool Usage

- Call tools immediately when needed.
- Avoid asking confirmation for obvious actions.
- Return concise summaries after execution.
- Stop after completing requested task.

## Git Commits

- Use short, concise commit messages.
- Start commit messages with one of these prefixes according to the change: `feature:`, `hotfix:`, or `refactor:`.
- Before committing, inspect `git status --short`, `git diff`, and `git log --oneline -10`.
- Stage only files related to the intended change.
- Never commit `.env`, credentials, secrets, downloaded debug images, or generated cache files.
