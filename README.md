# Guará Vivo Worker

Processo em segundo plano do Guará Vivo. Ele consome a fila RabbitMQ `guara-vermelho-inference`, e para cada registro novo: baixa as fotos, manda cada uma para o [identifier](https://github.com/Guara-vivo/guara-vivo-identifier) (a IA) e salva o resultado no PostgreSQL.

```
API ──publica {"record_id": 10}──> RabbitMQ ──> worker ──fotos──> identifier
                                                   │
                                                   └──resultado──> PostgreSQL (tabelas analyses, analysis_images, ibis)
```

Em resumo, para cada mensagem: marca o registro como `processing`, baixa as imagens, chama a IA, grava a análise e marca como `completed` ou `failed`. Se falhar de vez, a mensagem vai para a fila `guara-vermelho-inference-error`.

**Stack:** Python 3.12, pika (RabbitMQ), psycopg2 (PostgreSQL), requests.

## O que você precisa antes

O worker não faz nada sozinho: ele depende de três serviços no ar.

| Serviço | Para quê |
| --- | --- |
| PostgreSQL | O mesmo banco da API, com as migrations já aplicadas (rode `alembic upgrade head` no repositório da API) |
| RabbitMQ | A fila de onde ele lê os registros |
| Identifier | A API de IA, normalmente em `http://localhost:8000` |

> O jeito mais fácil de ter tudo isso é subir a stack completa pelo Docker Compose do repositório [`guara-vivo-api`](https://github.com/Guara-vivo/guara-vivo-api). Use este README quando quiser rodar **só o worker** em Python, por exemplo para depurar.

## Rodando localmente

```powershell
python -m venv venv
.\venv\Scripts\activate          # Linux/macOS: source venv/bin/activate
pip install -r requirements.txt

copy .env.example .env           # Linux/macOS: cp
```

Edite o `.env`. Variáveis principais (a lista completa está em [`docs/REFERENCIA.md`](docs/REFERENCIA.md)):

| Variável | Valor para rodar fora do Docker |
| --- | --- |
| `DATABASE_URL` | Conexão PostgreSQL **direta** (não use o pooler do Supabase na porta 6543: o worker usa lock por registro, que não funciona nele) |
| `RABBITMQ_HOST` / `RABBITMQ_PORT` | `localhost` / `5672` |
| `RABBITMQ_USER` / `RABBITMQ_PASSWORD` | Obrigatórios, sem padrão. Use os mesmos do seu RabbitMQ |
| `IA_API_URL` | `http://localhost:8000/guara-vermelho/inference` |
| `ALLOWED_IMAGE_HOSTS` | Domínio do Storage de onde vêm as fotos (ex.: `seu-projeto.supabase.co`). Só HTTPS é aceito |
| `DEBUG_SAVE_IMAGES_DIR` | Opcional. Se preenchido, salva as imagens e os JSONs da IA nessa pasta, para depuração (deixe vazio em produção) |

Suba o worker:

```powershell
python main.py
```

Ele fica rodando e esperando mensagens. Para testar de ponta a ponta, crie um registro com fotos pela API ou pelo app e acompanhe o log do worker.

### Com Docker

```bash
docker build -t guara-vivo-worker .
docker run --env-file .env guara-vivo-worker
```

Dentro do Docker, `localhost` aponta para o próprio container. Ajuste `RABBITMQ_HOST` e `IA_API_URL` para os nomes dos serviços da sua rede Docker.

## Testes

Não precisam de banco, RabbitMQ nem IA.

```powershell
pip install pytest
python -m pytest -q
```

## Estrutura

```
main.py    todo o worker (consumo da fila, download, chamada da IA, gravação)
tests/     testes (pytest)
docs/      documentação de referência
```

## Mais detalhes

O contrato esperado da resposta da IA, o fluxo completo e todas as variáveis de ambiente estão em [`docs/REFERENCIA.md`](docs/REFERENCIA.md).
