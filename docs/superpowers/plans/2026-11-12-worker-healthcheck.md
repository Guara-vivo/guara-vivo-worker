# Healthcheck do worker e alerta de contêiner parado: plano de implementação

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fazer o worker se declarar `unhealthy` quando o laço do RabbitMQ para de responder, e alertar quando qualquer contêiner está parado ou reiniciando em ciclo.

**Architecture:** Uma thread de "pulso" por conexão pede ao laço do pika, via `add_callback_threadsafe`, que grave a hora num arquivo; o `HEALTHCHECK` do Docker lê a idade desse arquivo. Um módulo novo e só com biblioteca padrão (`liveness.py`) guarda a lógica, para o healthcheck não importar o `main.py` (que exige variáveis de ambiente). Um alerta próprio no Netdata cobre contêiner parado ou em ciclo de quedas.

**Tech Stack:** Python 3.12, pika (`BlockingConnection`), `unittest`, Docker `HEALTHCHECK`, Netdata `health.d`.

**Spec:** `docs/superpowers/specs/2026-11-12-worker-healthcheck-design.md`

## Global Constraints

- Estes repositórios têm `main` protegido: tudo vai por PR; só abrir PR com o aval do usuário.
- O worker roda como `appuser` sem privilégios; o arquivo de pulso fica em `/tmp/worker-alive`.
- O pulso a cada 15 s; o healthcheck considera saudável com o arquivo de até 90 s; `HEALTHCHECK`: intervalo 30 s, timeout 5 s, `start-period` 60 s, 3 tentativas.
- `liveness.py` importa só a biblioteca padrão (nada de `pika`, `dotenv` nem `main`).
- Sem dependências novas, sem permissões novas no contêiner.
- Testes: `python -m unittest discover -s tests` (linha de base: 23 testes passando).
- Não incluir segredos, e-mails nem dados pessoais em arquivos, commits ou PRs.

## Review Focus

- **Escrita do pulso falha** (disco cheio, permissão): o callback roda na thread de E/S do pika; uma exceção ali derrubaria o consumo. Esperado: registrar o erro, não propagar, deixar o arquivo envelhecer (vira `unhealthy`). Teste na Task 1.
- **Arquivo ausente, vazio, ilegível ou com lixo**: o healthcheck responde "não saudável" (saída 1) sem traceback. Teste na Task 1.
- **Relógio do contêiner volta para trás** (carimbo no futuro): idade negativa grande não pode contar como saudável. Teste na Task 1.
- **Conexão cai e reconecta**: a thread de pulso antiga para e só uma nova roda (sem acumular threads). Teste na Task 2.
- **Job longo**: o pulso continua enquanto a thread do job está ocupada (o laço de E/S segue livre). Teste na Task 1 (callbacks executados em outra thread enquanto outra bloqueia).

---

### Task 1: Módulo `liveness` (pulso e leitura de saúde)

**Files:**
- Create: `liveness.py`
- Test: `tests/test_liveness.py`

**Interfaces:**
- Produces:
  - Constantes: `LIVENESS_FILE: str` (env `WORKER_LIVENESS_FILE`, padrão `/tmp/worker-alive`), `PULSE_SECONDS: float = 15`, `MAX_AGE_SECONDS: float = 90`.
  - `write_pulse(path: str = LIVENESS_FILE, now: float | None = None) -> None`: grava o carimbo (`time.time()` ou `now`) no arquivo; **nunca levanta** (loga o erro em `logging.getLogger("guara-vivo-worker")`).
  - `is_alive(path: str = LIVENESS_FILE, max_age: float = MAX_AGE_SECONDS, now: float | None = None) -> bool`: `True` só se o arquivo existe, tem um número válido e `abs(now - carimbo) <= max_age`.
  - `start_pulse(connection, stop: threading.Event, path: str = LIVENESS_FILE, interval: float = PULSE_SECONDS) -> threading.Thread`: thread daemon que, a cada `interval`, chama `connection.add_callback_threadsafe(lambda: write_pulse(path))`; termina quando `stop` está setado ou quando `add_callback_threadsafe` levanta exceção. Grava um pulso inicial ao começar.
  - CLI: `python liveness.py` sai com 0 se `is_alive()`, senão 1, sem traceback.

- [ ] **Step 1: Escrever os testes que falham** em `tests/test_liveness.py` (mesmo estilo de `unittest` do repositório, usando `tempfile` para o arquivo):
  - `test_pulse_then_alive`: após `write_pulse(path)`, `is_alive(path)` é `True`.
  - `test_missing_file_is_not_alive`.
  - `test_old_pulse_is_not_alive`: `write_pulse(path, now=1000)`; `is_alive(path, max_age=90, now=1100)` é `False`; com `now=1050` é `True`.
  - `test_future_timestamp_is_not_alive`: `write_pulse(path, now=5000)`; `is_alive(path, max_age=90, now=1000)` é `False` (relógio voltou).
  - `test_garbage_or_empty_file_is_not_alive`: conteúdo `b""` e `b"abc"` dão `False` sem exceção.
  - `test_write_pulse_never_raises`: com `path` num diretório inexistente, `write_pulse` não levanta.
  - `test_start_pulse_calls_callback_and_stops`: conexão falsa cujo `add_callback_threadsafe` executa o callback e conta chamadas; com `interval=0.01`, após ~0,1 s há várias chamadas; ao setar `stop` a thread termina (`join(1)`).
  - `test_start_pulse_ends_when_connection_closed`: `add_callback_threadsafe` levanta `RuntimeError`; a thread termina sozinha.
  - `test_pulse_continues_while_another_thread_blocks`: o callback é executado por uma thread separada enquanto outra thread dorme; o carimbo do arquivo continua avançando.
  - `test_cli_exit_codes`: `subprocess.run([sys.executable, "liveness.py"], env={"WORKER_LIVENESS_FILE": path, ...})` retorna 1 sem arquivo e 0 depois de `write_pulse(path)`; `stderr` não contém "Traceback".

- [ ] **Step 2: Rodar e ver falhar.** Run: `python -m unittest tests.test_liveness -v`. Expected: erro de importação (`No module named 'liveness'`).

- [ ] **Step 3: Implementar `liveness.py`** com as assinaturas acima. Ler `WORKER_LIVENESS_FILE` do ambiente na importação; `is_alive` captura `OSError` e `ValueError` e devolve `False`; `write_pulse` captura `OSError` e loga.

- [ ] **Step 4: Rodar e ver passar.** Run: `python -m unittest discover -s tests`. Expected: todos passam (23 antigos + os novos).

- [ ] **Step 5: Commit** — `git add liveness.py tests/test_liveness.py && git commit -m "feat(worker): módulo liveness (pulso e leitura de saúde)"` (com o `Co-Authored-By` padrão).

---

### Task 2: Ligar o pulso ao laço do RabbitMQ

**Files:**
- Modify: `main.py` (`run()`, em torno das linhas 778-801; e o `import` no topo)
- Test: `tests/test_worker_hardening.py` (ou `tests/test_liveness.py`; seguir onde já houver teste de `run`/reconexão)

**Interfaces:**
- Consumes: `liveness.start_pulse(connection, stop, path, interval) -> threading.Thread` e `liveness.LIVENESS_FILE` da Task 1.
- Produces: `run()` inicia um pulso por conexão e o para (`stop.set()`) quando a conexão acaba, seja por `AMQPConnectionError`, por outro erro ou por `KeyboardInterrupt`.

- [ ] **Step 1: Escrever o teste que falha**, com `patch.object(main, ...)`: 
  - `test_run_starts_pulse_for_each_connection_and_stops_it`: `connect_rabbitmq` devolve uma conexão falsa; `setup_channel` devolve um canal falso cujo `start_consuming` levanta `pika.exceptions.AMQPConnectionError` na primeira vez e `KeyboardInterrupt` na segunda; `time.sleep` é neutralizado. Esperado: `liveness.start_pulse` chamado **duas vezes** (uma por conexão) e o `stop` de cada chamada fica setado ao fim (uma thread por conexão, nenhuma acumulada).
  - `test_run_stops_pulse_on_unexpected_error`: `start_consuming` levanta `RuntimeError`; o erro continua propagando (comportamento atual preservado) e o `stop` da conexão fica setado.

- [ ] **Step 2: Rodar e ver falhar.** Run: `python -m unittest discover -s tests -v`. Expected: os dois novos falham (`start_pulse` nunca é chamado).

- [ ] **Step 3: Alterar `run()`** para, depois de `connect_rabbitmq()`, criar um `threading.Event` e chamar `liveness.start_pulse(connection, stop)`; envolver o trecho de consumo em `try/finally` que faz `stop.set()`. Não mudar mais nada do comportamento de reconexão, e `import liveness` no topo de `main.py`.

- [ ] **Step 4: Rodar e ver passar.** Run: `python -m unittest discover -s tests`. Expected: todos passam.

- [ ] **Step 5: Commit** — `git add main.py tests && git commit -m "feat(worker): pulso de vida ligado à conexão com o RabbitMQ"`.

---

### Task 3: `HEALTHCHECK` no Dockerfile e verificação na EC2

**Files:**
- Modify: `Dockerfile`
- Modify: `AGENTS.md` (uma linha em "Worker Notes" sobre o healthcheck)

**Interfaces:**
- Consumes: `python liveness.py` da Task 1 (exit 0 saudável, 1 não).

- [ ] **Step 1: Alterar o `Dockerfile`:** `COPY liveness.py .` junto com o `COPY main.py .`, e, depois de `USER appuser`, `HEALTHCHECK --interval=30s --timeout=5s --start-period=60s --retries=3 CMD ["python", "liveness.py"]`.

- [ ] **Step 2: Rodar os testes.** Run: `python -m unittest discover -s tests`. Expected: passam (não há Docker local para construir a imagem).

- [ ] **Step 3: Documentar** no `AGENTS.md` (uma linha): o healthcheck lê `/tmp/worker-alive`, atualizado a cada 15 s pela conexão; `unhealthy` quando o laço do RabbitMQ para ou a conexão cai.

- [ ] **Step 4: Commit** — `git add Dockerfile AGENTS.md && git commit -m "feat(worker): HEALTHCHECK baseado no pulso de vida"`.

- [ ] **Step 5: Abrir o PR** em `Guara-vivo/guara-vivo-worker` (só com o aval do usuário), citando `Guara-vivo/documents#71`. Depois do merge, o deploy é disparado pelo workflow `trigger-api-deploy.yml`.

- [ ] **Step 6: Verificar na EC2 (leitura)** após o deploy: `docker ps` mostra o worker `(healthy)` (pode levar até ~60 s); no Netdata, o gráfico `docker.container_health_status` do worker passa de `no_healthcheck` para `healthy`. Se aparecer `unhealthy`, olhar `docker inspect --format '{{json .State.Health}}' guara-vivo-api-worker-1`.

---

### Task 4: Alerta de contêiner parado ou reiniciando (Netdata) e documentação

**Files:**
- Create (na EC2, no volume `netdataconfig`): `/etc/netdata/health.d/container-parado.conf`
- Modify: `guara-vivo-api/docs/MONITORAMENTO.md` (em outro repositório: branch e PR próprios)

**Interfaces:**
- Consumes: o contexto `docker.container_state` do Netdata (dimensões `exited` e `restarting`, por contêiner).

- [ ] **Step 1: Escrever o alerta** como `template` sobre `docker.container_state`: `lookup: average -3m unaligned of exited,restarting`, `warn: $this > 0.5`, `every: 30s`, `to: sysadmin`, `summary` com o nome do contêiner. Esses valores são ponto de partida; ajustar no teste do passo 3.

- [ ] **Step 2: Instalar** o arquivo na EC2 e recarregar com `docker exec netdata netdatacli reload-health`. Confirmar que o alerta aparece em `/api/v1/alarms?all` em estado `CLEAR`.

- [ ] **Step 3: Testar com um contêiner descartável** (por exemplo `docker run -d --name zz-teste-alerta busybox sleep 1 && ...` que fique `exited`, e outro com `--restart=always` que quebre em ciclo): o alerta deve ir a `WARNING` e o e-mail chegar. Depois, apagar os contêineres de teste e confirmar que o alerta volta a `CLEAR`. Confirmar também que um deploy normal dos contêineres da API não dispara o alerta (ajustar janela ou limite se disparar).

- [ ] **Step 4: Documentar** em `guara-vivo-api/docs/MONITORAMENTO.md`: o conteúdo do arquivo, o que ele cobre, e remover o item "Worker" da lista do que não está coberto. PR no `guara-vivo-api` com aval do usuário.

---

### Task 5: Teste real e fechamento da #71

- [ ] **Step 1:** Combinar um horário com o usuário. Parar o worker de propósito (`docker stop guara-vivo-api-worker-1`) e conferir que o alerta de estado chega por e-mail; subir de novo (`docker start`).
- [ ] **Step 2:** (Opcional, se o usuário quiser) simular worker travado sem pará-lo e conferir `unhealthy`.
- [ ] **Step 3:** Conferir que, com o worker processando um registro, não há alarme falso.
- [ ] **Step 4:** Comentar na #71 e na #61 com o resultado, marcar os critérios de aceite e mover a #71 para Done (com o aval do usuário).
