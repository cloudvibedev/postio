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
- Imagem validada no pod: `ghcr.io/cloudvibedev/postio:b023975057cc5f9e2981bc5fa7e663ee8731e260`.
- Commit validado pelo Argo CD em `k8s-sandbox`: `f8102a2699c83acf562d3bcb703a6d5edb0a7408`.
- Endpoint testado: `http://gateway.sdx.autob/postio/pipeline/sqs`.

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
- `[!]` Confirmar que erros de target aparecem no trace com contexto suficiente.

Notas:

- Grafana `http://grafana.o11y.sdx.autob` respondeu com sucesso e permitiu consultar o datasource Tempo pelo proxy.
- Os spans da pipeline existem no Tempo, mas aparecem divididos em traces diferentes quando atravessam channels/tasks Tokio.
- Correcao local implementada: carregar o contexto de trace dentro da `PipelineMessage` e reanexar o parent span em cada etapa.
- Correcao local implementada: falha de target registra `result.status`, `error.kind` e evento de erro no span `postio.pipeline.target.send`.
- Ainda falta validar em Grafana/Tempo, apos novo deploy, que os spans aparecem no mesmo trace e que o erro de target aparece com contexto suficiente.

## Proxima Fase Planejada

- `[x]` Revisar no `PLAN-PIPELINES.md` o escopo da primeira transformacao.
- `[x]` Definir se a proxima implementacao sera `transform.engine: template`.
- `[ ]` Definir schema minimo do transform template para `body`, `headers` e metadata.
- `[ ]` Criar exemplos de `HTTP -> SQS` com transform template.
- `[ ]` Criar exemplos de `SQS -> HTTP` com transform template.
- `[ ]` Planejar testes antes da implementacao.

## Bloqueios Atuais

- `[x]` Esta sessao local nao acessa o endpoint privado do EKS sem VPN/rota para a VPC.
- `[x]` Esta sessao local nao resolve `gateway.sdx.autob` sem DNS da Client VPN.
- `[!]` Argo CD esta `Synced`, mas com health global `Degraded` por `RepeatedResourceWarning` do namespace `cloudvibe`; nao parece ser causado pelo Postio.
- `[!]` O hostname direto do Tempo nao respondeu durante a validacao; os traces foram validados via proxy de datasource do Grafana.
