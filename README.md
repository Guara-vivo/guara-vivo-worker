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
RABBITMQ_USER=guara_worker
RABBITMQ_PASSWORD=secure_password
QUEUE_NAME=guara-vermelho-inference
ERROR_QUEUE_NAME=guara-vermelho-inference-error
IA_API_URL=http://ia-api:8000/guara-vermelho/inference

# Timeouts e retries
MAX_RETRIES=3
DOWNLOAD_TIMEOUT_SECONDS=30
IA_TIMEOUT_SECONDS=60
LOG_LEVEL=INFO

# Segurança
ALLOWED_IMAGE_HOSTS=example.supabase.co
MAX_IMAGE_BYTES=10485760

# RabbitMQ
RABBITMQ_HEARTBEAT_SECONDS=600
RABBITMQ_BLOCKED_CONNECTION_TIMEOUT_SECONDS=300

# Debug (local apenas)
DEBUG_SAVE_IMAGES_DIR=debug-images
DEBUG_MAX_RUNS=20
```

**Notas importantes:**
- `RABBITMQ_USER` e `RABBITMQ_PASSWORD` são obrigatórios (não use defaults `guest/guest` em produção).
- `ALLOWED_IMAGE_HOSTS`: lista de domínios permitidos separados por vírgula (ex: `example.supabase.co,cdn.example.com`). Se vazio, qualquer host HTTPS é aceito.
- `MAX_IMAGE_BYTES`: limite de bytes por imagem. Padrão 10 MB.
- URLs sempre usam HTTPS; HTTP é rejeitado.
- `DEBUG_SAVE_IMAGES_DIR`: se vazio, debug desativado. Limpar manualmente `debug-images/` periodicamente.

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

O Worker aceita respostas da IA com a seguinte estrutura:

```json
{
  "quantidade_guaras": 2,
  "guaras": [
    {
      "cor": "vermelho",
      "fase_vida": "adulto",
      "acuracia": {
        "deteccao_yolo": 0.98,
        "classificacao_guara": 0.97,
        "classificacao_cor": 0.95,
        "classificacao_fase_vida": 0.93
      }
    },
    {
      "cor": "vermelho",
      "fase_vida": "juvenil",
      "acuracia": {
        "deteccao_yolo": 0.95,
        "classificacao_guara": 0.94,
        "classificacao_cor": 0.92,
        "classificacao_fase_vida": 0.90
      }
    }
  ]
}
```

**Campos importantes:**
- `quantidade_guaras`: número inteiro de guarás detectados
- `guaras`: lista de objetos de detecção
  - `cor`: cor do pássaro (ex: "vermelho")
  - `fase_vida`: fase de vida (ex: "adulto", "juvenil")
  - `acuracia`: objeto com métricas de confiança de cada etapa do modelo
