# Postio Tasks

Este arquivo acompanha as proximas tarefas praticas do Postio. Sempre revisar junto com o [PLAN-PIPELINES.md](./PLAN-PIPELINES.md) antes de implementar novas fases.

## Como Usar

- Manter este checklist como a fonte curta do que esta em andamento.
- Atualizar o status conforme cada item for validado.
- Antes de abrir nova implementacao, confirmar se ela esta alinhada com o plano.
- Quando um item gerar decisao de arquitetura, refletir tambem no `PLAN-PIPELINES.md`.

## Status

- `[ ]` pendente
- `[~]` em andamento
- `[x]` concluido
- `[!]` bloqueado

## Ultima Atualizacao

- Data: 2026-04-28.
- Deploy validado no cluster EKS `eks-autob-k8s`.
- Imagem validada no pod: `ghcr.io/cloudvibedev/postio:39d532f4b0b3be67dc2076fe8dee137ef09f33fd`.
- Commit validado pelo Argo CD em `k8s-sandbox`: `41224a0f02fc893e3cb1b94586749e85939c3278`.
- Endpoints testados:
  - `http://gateway.sdx.autob/postio/pipeline/sqs`.
  - `http://gateway.sdx.autob/postio/pipeline/error`.

## Validacao Do Deploy Atual

- `[x]` Confirmar rollout no cluster.
- `[x]` Verificar se o Argo CD sincronizou o commit `f8102a2` do `k8s-sandbox`.
- `[x]` Confirmar se o pod `postio` esta usando a imagem `ghcr.io/cloudvibedev/postio:b023975057cc5f9e2981bc5fa7e663ee8731e260`.
- `[x]` Verificar logs de startup do pod para confirmar que o bloco `pipeline:` carregou sem erro.
- `[x]` Confirmar que a rota `POST /postio/pipeline/sqs` esta ativa no Gateway.

## Testes Funcionais Em Sandbox

- `[x]` Testar `POST /postio/pipeline/sqs`.
- `[x]` Confirmar resposta `202 Accepted` no endpoint novo.
- `[x]` Ler a fila `postio-sqs-input` e validar que o payload do endpoint novo chegou corretamente.
- `[x]` Testar regressao da rota v0 `POST /postio/sqs`.
- `[x]` Testar regressao da rota v0 `POST /postio/sns`.
- `[x]` Testar regressao da rota v0 `POST /postio/s3`.
- `[x]` Testar regressao da rota v0 `POST /postio/s3/multipart-test`.

## Evidencias Da Rodada Atual

- `POST /postio/pipeline/sqs` retornou `202 Accepted` e gravou o payload `pipeline-1777340608` na fila `postio-sqs-input`.
- `POST /postio/sqs` retornou `202 Accepted` e gravou o payload `v0-sqs-1777340626` na fila `postio-sqs-input`.
- `POST /postio/sns` retornou `202 Accepted`; a subscription SNS -> SQS entregou o payload `v0-sns-1777340643` na fila `postio-sns-capture`.
- `POST /postio/s3` retornou `201 Created` e o objeto salvo no bucket continha o payload `v0-s3-json-1777340667`.
- `POST /postio/s3/multipart-test` retornou `201 Created` e o objeto salvo no bucket continha o arquivo `v0-s3-multipart-1777340682.txt`.

## Observabilidade

- `[x]` Validar traces no Grafana/Tempo para o endpoint `POST /postio/pipeline/sqs`.
- `[x]` Confirmar span `postio.pipeline.http.request`.
- `[x]` Confirmar span `postio.pipeline.decode`.
- `[x]` Confirmar span `postio.pipeline.validate`.
- `[x]` Confirmar span `postio.pipeline.transform.request`.
- `[x]` Confirmar span `postio.pipeline.target.send`.
- `[x]` Confirmar span `postio.pipeline.complete`.
- `[x]` Confirmar que os traces mostram tempo por etapa.
- `[x]` Confirmar que erros de target aparecem no trace com contexto suficiente.

Notas:

- Grafana `http://grafana.o11y.sdx.autob` respondeu com sucesso e permitiu consultar o datasource Tempo pelo proxy.
- Validado em Grafana/Tempo que os spans internos ficam no mesmo trace apos atravessar channels/tasks Tokio.
- Validado que `traceparent` recebido no HTTP e respeitado como parent do trace da pipeline.
- Validado que falha de target registra `result.status=failed`, `error.kind=target_send_failed` e evento `pipeline target failed` no span `postio.pipeline.target.send`.

Evidencia do caminho feliz:

- Endpoint: `POST /postio/pipeline/sqs`.
- Trace ID validado: `39d532f4b0b3be67dc2076fe8dee137e`.
- Request ID: `11b74d1e-1c2f-423f-be6e-1046e59a5866`.
- Spans no mesmo trace: `http.request`, `http.submit`, `decode`, `validate`, `transform.request`, `target.send`, `complete`.

Evidencia do caminho de erro:

- Endpoint: `POST /postio/pipeline/error`.
- Trace ID validado: `41224a0f02fc893e3cb1b94586749e85`.
- Request ID: `4dfe9e0e-442b-4493-b3c3-74fb457fee0d`.
- Resposta HTTP: `502`.
- Span `postio.pipeline.target.send`: `target.type=http`, `result.status=failed`, `error.kind=target_send_failed`.

## Proxima Fase Planejada

- `[x]` Revisar no `PLAN-PIPELINES.md` o escopo da primeira transformacao.
- `[x]` Definir se a proxima implementacao sera `transform.engine: template`.
- `[ ]` Definir schema minimo do `transform.engine: template`.
- `[ ]` Definir `transform.output.body`.
- `[ ]` Definir `transform.output.headers`.
- `[ ]` Definir `transform.output.query`.
- `[ ]` Definir `transform.output.method`.
- `[ ]` Definir `transform.output.url`.
- `[ ]` Definir `transform.output.delaySeconds`.
- `[ ]` Definir `transform.output.attributes`.
- `[ ]` Implementar `HTTP -> SQS` com `transform.output.body`.
- `[ ]` Implementar acesso no template a `body`, `headers`, `params`, `query` e `context`.
- `[ ]` Manter fallback noop quando `transform` nao existir.
- `[ ]` Criar teste `HTTP -> SQS` transformando payload.
- `[ ]` Criar teste `HTTP -> HTTP` transformando body/header.
- `[ ]` Criar teste garantindo que pipelines sem `transform` continuam funcionando.
- `[ ]` Criar exemplos de `HTTP -> SQS` com transform template.
- `[ ]` Criar exemplos de `SQS -> HTTP` com transform template.

## Rotas Temporarias De Validacao

- `[x]` Criar rota sandbox `POST /postio/pipeline/error` para validar traces de erro.
- `[x]` Confirmar que a rota retorna `502` quando o target HTTP falha.
- `[x]` Confirmar que a rota aparece no Tempo com `result.status=failed`.
- `[ ]` Decidir se a rota `POST /postio/pipeline/error` deve permanecer no sandbox para validacoes futuras ou ser removida.

## Bloqueios Atuais

- `[x]` Esta sessao local nao acessa o endpoint privado do EKS sem VPN/rota para a VPC.
- `[x]` Esta sessao local nao resolve `gateway.sdx.autob` sem DNS da Client VPN.
- `[!]` Argo CD esta `Synced`, mas com health global `Degraded` por `RepeatedResourceWarning` do namespace `cloudvibe`; nao parece ser causado pelo Postio.
- `[x]` O hostname direto do Tempo nao respondeu durante a validacao; os traces foram validados via proxy de datasource do Grafana.
- `[!]` A role IAM `postio-ingestion-api` nao permite `sqs:ReceiveMessage`; validacoes de leitura direta da fila precisam de permissao separada ou outro mecanismo de teste.
