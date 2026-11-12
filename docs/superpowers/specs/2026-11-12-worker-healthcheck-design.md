# Healthcheck do worker e alerta de contêiner parado

Issue: [Guara-vivo/documents#71](https://github.com/Guara-vivo/documents/issues/71). Relacionada: #61 (monitoramento). Desenho aprovado em 2026-11-12.

## Intenção

Ser avisado quando o worker parar, travar ou perder a conexão com o RabbitMQ. Hoje nenhum alerta cobre isso: o worker não tem *healthcheck*, então o alerta padrão do Netdata (`docker_container_unhealthy`) nunca dispara para ele. Enquanto o worker está fora, os registros ficam `pending` e a API só os reenfileira depois de `REQUEUE_AFTER_SECONDS` (600 s), sem avisar ninguém.

## Como o worker funciona hoje (conferido no código)

- A thread principal fica em `channel.start_consuming()`; cada mensagem roda numa thread própria (`make_threaded_handler`), com `prefetch_count=1`.
- `run()` só trata `AMQPConnectionError` (espera `RABBITMQ_RECONNECT_SECONDS` e reconecta). Qualquer outro erro derruba o processo e o `restart: unless-stopped` o levanta de novo.
- Uma conexão pendurada sem erro pode passar despercebida até o heartbeat do broker (`RABBITMQ_HEARTBEAT_SECONDS`, 600 s).
- O que o Netdata enxerga do contêiner: estado (`running`, `exited`, `restarting`, ...) e saúde (`healthy`, `unhealthy`, `starting`, `not_running_unhealthy`, `no_healthcheck`). O alerta padrão olha só `unhealthy`; um contêiner que para vira `not_running_unhealthy`, que ele não conta.

## Objetivos

- Marcar o worker como `unhealthy` quando o laço do RabbitMQ não estiver respondendo ou a conexão tiver caído, para o alerta padrão do Netdata valer.
- Avisar quando um contêiner estiver parado ou reiniciando em ciclo (o que o healthcheck não pega).
- Não gerar alarme falso durante inferências longas.

## Fora do escopo

- Reiniciar o worker automaticamente quando ficar `unhealthy`.
- Alerta de fila de mensagens crescendo.
- Mudar o comportamento de reconexão ou de processamento do worker.

## Anti-objetivos

- Um healthcheck que marque `unhealthy` só porque uma inferência está demorando, ou que cause reinício por engano.
- Mais permissões ou dependências novas no contêiner do worker.

## Abordagens consideradas

1. **Só um alerta de estado no Netdata**, sem mexer no worker. Rápido, mas não percebe worker vivo e travado.
2. **Healthcheck no worker** (escolhida, junto com a 3). Percebe travado e sem conexão; reaproveita o alerta padrão.
3. **Alerta de estado do contêiner** no Netdata. Percebe parado e reiniciando em ciclo, em todos os contêineres.

Escolhido: 2 + 3, porque são complementares. O healthcheck cobre "vivo mas travado"; o alerta de estado cobre "parado ou em ciclo de quedas".

## Desenho

### Parte A: healthcheck do worker (PR em `guara-vivo-worker`)

- A cada conexão com o RabbitMQ, uma thread de "pulso" pede ao laço do pika, a cada ~15 s (`connection.add_callback_threadsafe`), que grave a hora atual em um arquivo (por exemplo `/tmp/worker-alive`). O pulso só avança se o laço de E/S da conexão estiver processando callbacks; se ele travar ou a conexão cair, o arquivo envelhece.
- A thread de pulso termina quando a conexão fecha (o `add_callback_threadsafe` falha) e uma nova começa na reconexão.
- Durante uma inferência longa o laço continua respondendo (o job roda em outra thread), então o arquivo continua sendo atualizado e o worker não é marcado `unhealthy` por demora de job.
- `HEALTHCHECK` no `Dockerfile`: um comando Python curto que sai com erro se o arquivo não existir ou tiver mais de ~90 s. Intervalo ~30 s, timeout ~5 s, `start-period` ~60 s, 3 tentativas.
- Testes automatizados em `tests/` para a lógica de gravar o pulso e de decidir "saudável ou não" pela idade do arquivo.

### Parte B: alerta de contêiner parado ou reiniciando (Netdata na EC2)

- Um alerta próprio em `health.d` (volume `netdataconfig`) sobre o contexto `docker.container_state`, olhando as dimensões `exited` e `restarting`, para todos os contêineres.
- O limite deve ser ajustado para não gritar a cada deploy (quando os contêineres são recriados). Testar com um contêiner descartável e confirmar que um deploy normal não dispara.
- O conteúdo do arquivo é documentado em `guara-vivo-api/docs/MONITORAMENTO.md`, porque a cópia na EC2 vive só no volume.

## Restrições

- O `main` dos repositórios é protegido: mudanças por PR.
- O `HEALTHCHECK` roda dentro do contêiner do worker, que roda como usuário sem privilégios (`appuser`); o arquivo de pulso deve ficar num caminho gravável por ele (`/tmp`).
- Mudanças no worker em Docker pedem recriar o contêiner no deploy (ver `AGENTS.md`).

## Deixado para quem implementa

- Valores exatos de intervalo do pulso e do limite de idade (partir de ~15 s e ~90 s).
- Forma exata do arquivo de pulso e do comando do healthcheck.
- Limite e janela do alerta B, definidos nos testes.

## Teste real (critérios de aceite da issue)

- Parar ou travar o worker de propósito gera alerta por e-mail em poucos minutos, em horário combinado.
- O worker em funcionamento normal, inclusive durante inferências, não gera alerta falso.
