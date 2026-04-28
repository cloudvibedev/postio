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
- Imagem mais recente publicada: `ghcr.io/cloudvibedev/postio:45d93635cbf7f8862ad8ba841c7d2925bc063b99`.
- Commit mais recente validado pelo Argo CD em `k8s-sandbox`: `987c6df46d38fd8e5ab0c8cba77f39b15febc1d9`.
- Endpoints testados:
  - `http://gateway.sdx.autob/postio/pipeline/sqs`.
  - `http://gateway.sdx.autob/postio/pipeline/error`.
  - `http://gateway.sdx.autob/postio/pipeline/template/sqs/acme`.

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
- `POST /postio/pipeline/template/sqs/acme?source=checkout` retornou `202 Accepted` com `messageId=ced2bf5b-9ed6-4806-b4ae-63450007a847`.

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
- `[x]` Definir schema minimo do `transform.engine: template`.
- `[x]` Definir `transform.output.body`.
- `[x]` Definir `transform.output.headers`.
- `[x]` Definir `transform.output.query`.
- `[x]` Definir `transform.output.method`.
- `[x]` Definir `transform.output.url`.
- `[x]` Definir `transform.output.delaySeconds`.
- `[x]` Definir `transform.output.attributes`.
- `[x]` Implementar `HTTP -> SQS` com `transform.output.body`.
- `[x]` Implementar acesso no template a `body`, `headers`, `params`, `query` e `context`.
- `[x]` Manter fallback noop quando `transform` nao existir.
- `[x]` Criar teste `HTTP -> SQS` transformando payload.
- `[x]` Criar teste `HTTP -> HTTP` transformando body/header.
- `[x]` Criar teste garantindo que pipelines sem `transform` continuam funcionando.
- `[x]` Criar exemplos de `HTTP -> SQS` com transform template.
- `[x]` Criar exemplos de `SQS -> HTTP` com transform template.
- `[x]` Criar teste `SQS -> HTTP` transformando body/header.
- `[x]` Implementar `transform.output.query` para target HTTP.
- `[x]` Criar teste `HTTP -> HTTP` transformando query string.

Notas da primeira entrega:

- `transform.output.body`, `headers`, `method`, `url`, `query`, `attributes` e `delaySeconds` ja sao aplicados no runtime.
- Exemplo `SQS -> HTTP` com `transform.engine: template` documentado no README e no plano.

## Proximos Passos

### Fechar Transform Template

- `[x]` Implementar `transform.output.attributes` para target SQS.
- `[x]` Criar teste `HTTP -> SQS` validando atributos SQS dinamicos.
- `[x]` Criar teste `SQS -> SQS` validando atributos SQS dinamicos.
- `[x]` Atualizar README com exemplo de `transform.output.attributes`.
- `[x]` Atualizar `PLAN-PIPELINES.md` marcando `attributes` como suportado para SQS.
- `[x]` Publicar imagem com suporte a attributes e acompanhar GitHub Actions.
- `[x]` Atualizar sandbox para a nova imagem.
- `[!]` Validar `transform.output.attributes` no sandbox.

Notas:

- A policy ACK/IAM foi ajustada para permitir `sqs:ReceiveMessage` na fila `postio-sqs-input`.
- A rota sandbox `POST /postio/pipeline/template/sqs/{tenant}` foi criada e retornou `202 Accepted` gravando no SQS.
- A leitura direta dos atributos SQS no sandbox ficou bloqueada nesta rodada porque:
  - o token SSO usado pelo `kubectl` expirou e nao renovou automaticamente; e
  - em seguida os hostnames `gateway.sdx.autob` e `grafana.o11y.sdx.autob` pararam de resolver na sessao local.
- Assim que o SSO/DNS da VPN estiver OK, validar a mensagem `ced2bf5b-9ed6-4806-b4ae-63450007a847` ou reenviar novo payload e ler com `message-attribute-names=All`.

### Rotas De Validacao Em Sandbox

- `[ ]` Decidir se a rota `POST /postio/pipeline/error` deve permanecer como rota operacional de teste.
- `[ ]` Se permanecer, documentar que ela e sandbox-only e existe para validar traces de falha.
- `[ ]` Se for removida, remover manifests do `k8s-sandbox` e validar que os demais testes continuam passando.
- `[ ]` Decidir se a rota `POST /postio/pipeline/template/http/{tenant}` deve permanecer como rota operacional de teste para transform template.
- `[ ]` Se permanecer, documentar que ela e sandbox-only e existe para validar `body`, `headers` e `query`.

### Proxima Capacidade Estrutural

- `[ ]` Definir schema inicial de `validate.engine: jsonschema`.
- `[ ]` Implementar etapa `validate` real mantendo fallback noop quando `validate` nao existir.
- `[ ]` Criar teste de payload valido seguindo para o target.
- `[ ]` Criar teste de payload invalido bloqueando o target.
- `[ ]` Garantir span `postio.pipeline.validate` com `result.status=accepted` ou `result.status=rejected`.
- `[ ]` Documentar exemplos simples de validacao no README.

### Transformacao Avancada

- `[ ]` Revisar no plano o escopo minimo do `transform.engine: rhai`.
- `[ ]` Definir contrato de entrada e saida do Rhai antes de implementar.
- `[ ]` Definir limites de seguranca do Rhai: timeout, funcoes permitidas e acesso a contexto.
- `[ ]` Implementar Rhai somente depois de fechar `template` e `jsonschema`.

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
- `[x]` A role IAM `postio-ingestion-api` agora declara `sqs:ReceiveMessage` para a fila `postio-sqs-input`.
- `[!]` O token SSO local do AWS/kubectl expirou durante a validacao de attributes.
- `[!]` A resolucao DNS local para `gateway.sdx.autob` e `grafana.o11y.sdx.autob` falhou depois do teste `POST /postio/pipeline/template/sqs/acme`.
