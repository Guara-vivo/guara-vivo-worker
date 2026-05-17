## Worker Notes

- Worker consumes RabbitMQ messages with `record_id`, downloads `records.images`, calls the identifier API, and writes `analyses` and `ibis` rows.
- Identifier API expects multipart field `image`, one image per request.
- Worker aggregates multiple image responses by summing `quantidade_guaras` and concatenating `guaras`.
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
