# Guara Vivo Worker

Worker responsável por consumir jobs da fila RabbitMQ `guara-vermelho-inference`, baixar as imagens do registro, chamar a IA API e salvar o resultado no PostgreSQL/Supabase.

## Fluxo

1. Consome mensagem RabbitMQ: `{"record_id": 10}`.
2. Atualiza `records.status` para `processing`.
3. Busca as URLs em `records.images`.
4. Baixa as imagens temporariamente para `/tmp`.
5. Envia as imagens para `IA_API_URL` via `multipart/form-data`.
6. Faz upsert em `analyses` usando `recorder_id = records.id`.
7. Substitui os registros em `ibis` ligados a essa análise.
8. Atualiza `records.status` para `completed` ou `failed`.
9. Em caso de falha final, publica a mensagem em `guara-vermelho-inference-error`.

## Variáveis de ambiente

Copie `.env.example` para `.env` e ajuste os valores.

```env
DATABASE_URL=postgresql://user:password@host:5432/database
RABBITMQ_HOST=rabbitmq
RABBITMQ_PORT=5672
RABBITMQ_USER=guest
RABBITMQ_PASSWORD=guest
QUEUE_NAME=guara-vermelho-inference
ERROR_QUEUE_NAME=guara-vermelho-inference-error
IA_API_URL=http://ia-api:8000/guara-vermelho/inference
```

## Rodando localmente

```bash
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
python main.py
```

## Docker

```bash
docker build -t guara-vivo-worker .
docker run --env-file .env guara-vivo-worker
```

## Contrato esperado da IA API

O Worker aceita respostas flexíveis, por exemplo:

```json
{
  "ibis_quantity": 2,
  "flock_size": "small",
  "ibis": [
    {"color": "red", "age_group": "adult"},
    {"color": "red", "age_group": "juvenile"}
  ]
}
```

Também tenta reconhecer campos alternativos como `guara_count`, `birds`, `detections`, `feather_color` e `age`.
