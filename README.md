# postio

Postio e uma API de injestao de dados configuravel. Ela recebe chamadas HTTP `POST` e encaminha o payload para destinos de entrada como AWS SNS, AWS SQS e AWS S3.

O objetivo da v0 e ser simples de operar: uma aplicacao, um arquivo YAML/JSON, varias rotas, varios destinos.

A evolucao v1 adiciona um modo de pipeline unica por processo. Nessa primeira implementacao, a pipeline suporta entradas e saidas `http` e `sqs`, com etapas internas de decode, validate, transform, target e completion.

## Sumario

- [Visao geral](#visao-geral)
- [Recursos da v0](#recursos-da-v0)
- [Pipeline v1 experimental](#pipeline-v1-experimental)
- [Resources](#resources)
- [Instalacao](#instalacao)
- [Executando localmente](#executando-localmente)
- [Configuracao da aplicacao](#configuracao-da-aplicacao)
- [Arquivo de rotas](#arquivo-de-rotas)
- [Templates](#templates)
- [Recurso SNS](#recurso-sns)
- [Recurso SQS](#recurso-sqs)
- [Recurso S3](#recurso-s3)
- [Exemplos simples](#exemplos-simples)
- [Exemplo complexo](#exemplo-complexo)
- [Chamando a API](#chamando-a-api)
- [Rotas sandbox](#rotas-sandbox)
- [Observabilidade](#observabilidade)
- [Operacao](#operacao)
- [Testes e desenvolvimento](#testes-e-desenvolvimento)
- [Arquitetura](#arquitetura)
- [Limitacoes da v0](#limitacoes-da-v0)
- [Troubleshooting](#troubleshooting)

## Visao geral

Postio funciona como uma ponte entre HTTP e servicos de injestao. Em vez de criar uma API especifica para cada topico, fila ou arquivo, voce declara as rotas em um arquivo de configuracao:

```yaml
routes:
  - id: topic-input
    path: /events/{topic}
    sink:
      type: sns
      topic: "{{ params.topic }}"

  - id: queue-input
    path: /queue
    sink:
      type: sqs
      queueUrl: https://sqs.us-east-1.amazonaws.com/123456789012/my-queue

  - id: file-input
    path: /file/{bucket}/{filename}
    sink:
      type: s3
      bucket: "{{ params.bucket }}"
      key: "{{ params.filename }}"
```

Cada rota recebe o request, monta um contexto com `params`, `query`, `headers`, `body` e `context`, renderiza templates e envia o dado para o destino configurado.

## Recursos da v0

- Rotas dinamicas `POST` configuradas por YAML ou JSON.
- Destinos AWS SNS, AWS SQS e AWS S3.
- Templates em propriedades do destino usando path params, query string, headers, body JSON e metadados do request.
- Resposta padronizada com `route_id`, `sink`, `status` e identificadores do destino quando disponiveis.
- Tracing OpenTelemetry com spans por etapa da injestao e por chamada AWS.
- `/health` para liveness.
- `/docs` com Swagger UI e `/openapi.json`.
- `/echo` para debug de requests.

## Pipeline v1 experimental

O bloco `pipeline` executa uma unica pipeline por processo/deployment Postio. Ele pode coexistir no arquivo com `routes[]`, mas a recomendacao para a v1 e configurar um deployment por pipeline para manter isolamento operacional, escala e troubleshooting simples.

Fluxo interno atual:

```text
source -> decode -> validate(noop/jsonschema/http) -> transform(noop/template/http) -> target -> completion
```

Escopo implementado nesta versao:

- Source `http`: registra uma rota `POST` e envia o payload para a pipeline.
- Source `sqs`: faz polling da fila, envia cada mensagem para a pipeline e finaliza conforme `source.completion`.
- Target `http`: envia o payload para um endpoint externo.
- Target `sqs`: envia o payload para uma fila SQS.
- Validate: retorna valido quando ausente; com `engine: jsonschema`, valida o payload localmente; com `engine: http`, chama um endpoint validador e aceita somente status `200 OK`.
- Transform: retorna o payload original quando ausente; com `engine: template`, monta body, headers, query, atributos e outros overrides de saida; com `engine: http`, envia o payload como string para um endpoint e usa a resposta como novo payload.
- Completion: pode ser configurado em `source.completion`. HTTP customiza status/body da resposta; SQS decide `ack`, `retry`, `drop` ou `deadLetter`.

### Schema raiz

```yaml
pipeline:
  id: http-to-sqs-orders
  enabled: true
  source:
    type: http
    method: POST
    path: /orders
    completion:
      onFailure:
        response:
          status: 502
  validate:
    engine: jsonschema
    schema:
      type: object
      required:
        - id
      properties:
        id:
          type: string
  transform:
    engine: template
    output:
      body:
        id: "{{ body.id }}"
        tenant: "{{ params.tenant }}"
  target:
    type: sqs
    queue: orders-output
```

| Propriedade | Obrigatoria | Padrao | Descricao |
| --- | --- | --- | --- |
| `id` | sim | - | Identificador da pipeline. Aparece em logs, spans e respostas. |
| `enabled` | nao | `true` | Liga ou desliga a pipeline. |
| `source` | sim | - | Entrada da pipeline. Nesta fase pode ser `http` ou `sqs`. |
| `source.completion` | nao | politica default | Politica de finalizacao do source. |
| `validate` | nao | noop | Validacao opcional antes do transform. Suporta `engine: jsonschema` e `engine: http`. |
| `transform` | nao | noop | Transformacao opcional antes do target. Suporta `engine: template` e `engine: http`. |
| `target` | sim | - | Saida da pipeline. Nesta fase pode ser `http` ou `sqs`. |

`source.completion` e opcional. Quando ausente, a politica default continua ativa por source.

## Resources

Cada resource documenta os papeis que suporta dentro da pipeline:

- `source`: como o dado entra na pipeline.
- `target`: como a pipeline envia dados para fora.
- `source.completion`: como a pipeline finaliza a conversa com o source original.

O completion pertence ao `source`, porque e ele que decide como responder, confirmar ou liberar retry para a origem. Ja o retry de envio pertence ao `target`, como `target.retry`, porque controla tentativas de entrega para o destino.

No codigo, os schemas de pipeline ficam em `src/pipeline/config/`, separados por familia: `source.rs`, `target.rs`, `transform.rs` e `validate.rs`. As structs continuam separadas por resource e papel, como `HttpSourceConfig`, `HttpSourceCompletionConfig`, `HttpTargetConfig`, `HttpValidateConfig`, `HttpTransformConfig`, `SqsSourceConfig`, `SqsSourceCompletionConfig`, `SqsDeadLetterConfig`, `SqsTargetConfig`, `TemplateTransformConfig` e `ValidateConfig`.

### HTTP

HTTP pode ser usado como `source` e como `target`.

#### HTTP Source

```yaml
pipeline:
  id: receive-orders
  source:
    type: http
    method: POST
    path: /tenants/{tenant}/orders
  target:
    type: sqs
    queue: orders-output
```

| Propriedade | Obrigatoria | Padrao | Descricao |
| --- | --- | --- | --- |
| `type` | sim | - | Deve ser `http`. |
| `method` | nao | `POST` | Metodo aceito. Nesta fase apenas `POST` e registrado. |
| `path` | sim | - | Rota HTTP da pipeline. Deve iniciar com `/` e pode usar params como `{tenant}`. |
| `completion` | nao | politica default | Politica de resposta HTTP do source. |

#### HTTP Completion

Completion default para HTTP source:

| Situacao | Acao atual | Resposta HTTP |
| --- | --- | --- |
| Target HTTP com sucesso | Responde ao cliente com `CompletionResponse`. | Status retornado pelo target HTTP, ou `200` se ausente. |
| Target SQS com sucesso | Responde ao cliente com `CompletionResponse`. | `202 Accepted`. |
| Target falha | Responde ao cliente com `CompletionResponse` de erro. | `502 Bad Gateway`. |

Schema:

```yaml
source:
  type: http
  method: POST
  path: /orders
  completion:
    onSuccess:
      response:
        status: 202
        body:
          ok: true
          requestId: "{{ context.requestId }}"
          messageId: "{{ context.messageId }}"
    onFailure:
      response:
        status: 502
        body:
          ok: false
          error: "{{ context.error }}"
    onValidationFailure:
      response:
        status: 422
        body:
          ok: false
          error: validation_failed
```

| Propriedade | Obrigatoria | Padrao | Descricao |
| --- | --- | --- | --- |
| `onSuccess.response.status` | nao | status do target ou `202` | Status HTTP para resposta customizada. |
| `onSuccess.response.body` | nao | `CompletionResponse` | Body para resposta customizada. |
| `onFailure.response.status` | nao | `502` | Status HTTP para resposta de falha. |
| `onFailure.response.body` | nao | `CompletionResponse` de erro | Body para resposta customizada de falha. |
| `onValidationFailure.response.status` | nao | `422` | Status HTTP quando a validacao rejeitar o payload. |
| `onValidationFailure.response.body` | nao | `CompletionResponse` de rejeicao | Body para resposta customizada de validacao. |

Templates de `response.body` podem usar `params`, `query`, `headers`, `body` e `context`. O `context` inclui `requestId`, `pipelineId`, `sourceType`, `status`, `targetType`, `targetStatusCode`, `messageId` e `error` quando estes valores existirem.

#### HTTP Target

```yaml
pipeline:
  id: forward-orders
  source:
    type: sqs
    queue: orders-input
  target:
    type: http
    method: POST
    url: https://api.example.com/orders
    timeoutMs: 5000
    headers:
      x-source: postio
    retry:
      maxAttempts: 3
      backoff:
        type: fixed
        delayMs: 500
```

| Propriedade | Obrigatoria | Padrao | Descricao |
| --- | --- | --- | --- |
| `type` | sim | - | Deve ser `http`. |
| `method` | nao | `POST` | Metodo usado na chamada para o target. |
| `url` | sim | - | Endpoint destino. |
| `headers` | nao | `connection: keep-alive` | Headers fixos enviados ao target. Quando `connection` nao e configurado, o Postio envia `connection: keep-alive`. |
| `timeoutMs` | nao | sem timeout por request | Timeout da chamada HTTP em milissegundos. |
| `retry` | nao | sem retry | Retry de envio ao target. |

#### Target Retry

`target.retry` pode ser usado em targets `http` e `sqs`. O runtime repete a tentativa quando o envio ao target falha. Para HTTP, respostas `5xx` tambem sao tratadas como falha transiente de target.

```yaml
target:
  type: http
  url: https://api.example.com/orders
  retry:
    maxAttempts: 3
    backoff:
      type: exponential
      initialMs: 200
      maxMs: 5000
```

| Propriedade | Obrigatoria | Padrao | Descricao |
| --- | --- | --- | --- |
| `maxAttempts` | sim | - | Quantidade maxima de tentativas. Deve ser maior que zero. |
| `backoff.type` | sim | - | Estrategia de espera. Suporta `fixed` e `exponential`. |
| `backoff.delayMs` | sim para `fixed` | - | Espera fixa em milissegundos. Deve ser maior que zero. |
| `backoff.initialMs` | sim para `exponential` | - | Espera inicial em milissegundos. Deve ser maior que zero. |
| `backoff.maxMs` | sim para `exponential` | - | Espera maxima em milissegundos. Deve ser maior ou igual a `initialMs`. |

### SQS

SQS pode ser usado como `source` e como `target`.

#### SQS Source

```yaml
pipeline:
  id: consume-orders
  source:
    type: sqs
    queue: orders-input
    batchSize: 5
    waitTimeSeconds: 10
    visibilityTimeoutSeconds: 30
  target:
    type: http
    method: POST
    url: https://api.example.com/orders
```

| Propriedade | Obrigatoria | Padrao | Descricao |
| --- | --- | --- | --- |
| `type` | sim | - | Deve ser `sqs`. |
| `queue` | condicional | - | Nome ou URL da fila. Obrigatorio quando `queueUrl` nao existe. |
| `queueUrl` | condicional | - | URL completa da fila. Se informada, e usada diretamente. |
| `batchSize` | nao | `1` | Quantidade maxima de mensagens por polling. |
| `waitTimeSeconds` | nao | `10` | Long polling em segundos. |
| `visibilityTimeoutSeconds` | nao | AWS default | Visibility timeout aplicado ao receive. |
| `completion` | nao | politica default | Politica de ack, retry, drop ou DLQ do source. |

#### SQS Completion

Completion default para SQS source:

| Situacao | Acao atual | Efeito no SQS |
| --- | --- | --- |
| Target com sucesso | `ack` | Executa `DeleteMessage` na mensagem original. |
| Target falha | `retry` | Nao deleta a mensagem; ela volta apos o visibility timeout. |

Schema:

```yaml
source:
  type: sqs
  queue: orders-input
  completion:
    onSuccess:
      action: ack
    onFailure:
      action: retry
    onValidationFailure:
      action: deadLetter
      deadLetter:
        queue: orders-invalid-dlq
```

| Propriedade | Obrigatoria | Padrao | Descricao |
| --- | --- | --- | --- |
| `onSuccess.action` | nao | `ack` | Acao quando o target conclui com sucesso. Para SQS, `ack` significa deletar a mensagem original. |
| `onFailure.action` | nao | `retry` | Acao quando o target falha. Para SQS, `retry` significa nao deletar a mensagem. |
| `onValidationFailure.action` | nao | `retry` | Acao quando a validacao rejeita a mensagem. |
| `action` | nao | depende da situacao | Valores suportados: `ack`, `retry`, `drop` e `deadLetter`. |
| `deadLetter.queue` | condicional | - | Nome da fila SQS de DLQ. Obrigatorio quando `deadLetter.queueUrl` nao existe. |
| `deadLetter.queueUrl` | condicional | - | URL completa da fila SQS de DLQ. Se informada, e usada diretamente. |
| `deadLetter.delaySeconds` | nao | AWS default | Delay aplicado ao envio para DLQ. |
| `deadLetter.attributes` | nao | - | Atributos SQS enviados junto com a mensagem de DLQ. |
| `action=drop` | nao | - | Descarta a mensagem, deletando-a mesmo quando a pipeline rejeitar ou falhar. |

Quando `action: deadLetter` e usado, o Postio envia o payload atual para a fila de DLQ e so depois executa `DeleteMessage` na mensagem original. Se o envio para DLQ falhar, a mensagem original nao e deletada.

#### SQS Target

```yaml
pipeline:
  id: publish-orders
  source:
    type: http
    path: /orders
  target:
    type: sqs
    queue: orders-output
    delaySeconds: 5
    retry:
      maxAttempts: 3
      backoff:
        type: fixed
        delayMs: 500
```

| Propriedade | Obrigatoria | Padrao | Descricao |
| --- | --- | --- | --- |
| `type` | sim | - | Deve ser `sqs`. |
| `queue` | condicional | - | Nome ou URL da fila. Obrigatorio quando `queueUrl` nao existe. |
| `queueUrl` | condicional | - | URL completa da fila. Se informada, e usada diretamente. |
| `delaySeconds` | nao | AWS default | Delay aplicado ao envio da mensagem. |
| `retry` | nao | sem retry | Retry de envio ao target SQS quando `SendMessage` falhar. |

### Validate JSON Schema

O validate `jsonschema` valida o payload decodificado antes do transform. Quando `validate` nao existe, a pipeline continua em modo noop e aceita o payload.

```yaml
pipeline:
  id: http-to-sqs-validated
  source:
    type: http
    method: POST
    path: /orders
  validate:
    engine: jsonschema
    schema:
      type: object
      required:
        - id
        - tenant
        - total
      properties:
        id:
          type: string
        tenant:
          type: string
        total:
          type: number
  target:
    type: sqs
    queue: orders-output
```

Campos de `validate`:

| Propriedade | Obrigatoria | Padrao | Descricao |
| --- | --- | --- | --- |
| `engine` | sim | - | Deve ser `jsonschema`. |
| `schema` | sim | - | JSON Schema inline usado para validar o payload decodificado. |

Comportamento atual:

| Source | Payload valido | Payload invalido |
| --- | --- | --- |
| HTTP | Segue para `transform` e `target`. | Retorna `422 Unprocessable Entity`, `status: rejected`, `error: validation failed` e nao chama o target. |
| SQS | Segue para `transform` e `target`; em sucesso, deleta a mensagem original. | Nao chama o target e nao deleta a mensagem original; ela volta apos o visibility timeout. |

Resposta HTTP em caso de rejeicao:

```json
{
  "pipelineId": "http-to-sqs-validated",
  "requestId": "7a8a0a41-8b9b-47b8-9288-29cbb9af5d7d",
  "status": "rejected",
  "error": "validation failed",
  "details": [
    {
      "path": "/total",
      "message": "\"invalid\" is not of type \"number\""
    }
  ]
}
```

### Validate HTTP

O validate `http` chama um endpoint externo antes do transform. O Postio envia o payload atual como string no body da requisicao. A validacao e aceita somente quando o endpoint responde `200 OK`; qualquer outro status ou erro de chamada rejeita o payload e impede o target.

```yaml
pipeline:
  id: http-to-sqs-http-validated
  source:
    type: http
    method: POST
    path: /orders
  validate:
    engine: http
    method: POST
    url: https://validator.example.com/orders
    timeoutMs: 2000
    headers:
      x-validator: orders
  target:
    type: sqs
    queue: orders-output
```

Campos de `validate.engine: http`:

| Propriedade | Obrigatoria | Padrao | Descricao |
| --- | --- | --- | --- |
| `engine` | sim | - | Deve ser `http`. |
| `method` | nao | `POST` | Metodo usado para chamar o validador. |
| `url` | sim | - | Endpoint externo de validacao. |
| `headers` | nao | `connection: keep-alive` | Headers fixos enviados ao endpoint validador. Quando `connection` nao e configurado, o Postio envia `connection: keep-alive`. |
| `timeoutMs` | nao | sem timeout por request | Timeout da chamada em milissegundos. |

Comportamento:

| Resposta do validador | Resultado |
| --- | --- |
| `200 OK` | O payload segue para `transform` e `target`. |
| Qualquer outro status | O payload e rejeitado com `status: rejected`. |
| Erro de rede ou timeout | O payload e rejeitado com `status: rejected`. |

### Transform Template

O transform `template` monta uma nova requisicao de saida antes do target. Quando `transform` nao existe, a pipeline continua em modo noop e envia o payload original.

```yaml
pipeline:
  id: http-to-sqs-template
  source:
    type: http
    method: POST
    path: /tenants/{tenant}/orders
  transform:
    engine: template
    output:
      body:
        event: order.received
        tenant: "{{ params.tenant }}"
        source: "{{ query.source }}"
        requestId: "{{ context.requestId }}"
        original: "{{ body }}"
      attributes:
        event: order.received
        tenant: "{{ params.tenant }}"
        source: "{{ query.source }}"
  target:
    type: sqs
    queue: orders-output
```

Campos de `transform`:

| Propriedade | Obrigatoria | Padrao | Descricao |
| --- | --- | --- | --- |
| `engine` | sim | - | Deve ser `template`. |
| `output` | sim | - | Objeto com os overrides produzidos pela transformacao. |

Campos de `transform.output`:

| Propriedade | Obrigatoria | Suporte atual | Descricao |
| --- | --- | --- | --- |
| `body` | nao | sim | Novo payload enviado ao target. Pode ser string, objeto, array, numero, booleano ou `null`. |
| `headers` | nao | sim para target HTTP | Headers dinamicos enviados ao target HTTP. Sobrescrevem headers fixos de mesmo nome. |
| `method` | nao | sim para target HTTP | Metodo dinamico do target HTTP. |
| `url` | nao | sim para target HTTP | URL dinamica do target HTTP. |
| `delaySeconds` | nao | sim para target SQS | Delay dinamico usado no envio SQS. |
| `query` | nao | sim para target HTTP | Query string dinamica enviada ao target HTTP. |
| `attributes` | nao | sim para target SQS | Atributos dinamicos enviados ao SQS como `String`. |

Contexto disponivel no template:

| Raiz | Descricao |
| --- | --- |
| `body` | Payload decodificado. JSON continua navegavel, por exemplo `{{ body.order.id }}`. |
| `headers` | Headers recebidos da fonte HTTP. |
| `params` | Params extraidos do path HTTP, como `{tenant}`. |
| `query` | Query string recebida pela fonte HTTP. |
| `context.requestId` | ID da mensagem na pipeline. |
| `context.pipelineId` | ID da pipeline. |
| `context.attempt` | Tentativa atual. |
| `context.sourceType` | Tipo da fonte, como `http` ou `sqs`. |

Regras de renderizacao:

- Quando a string inteira e um template, o tipo JSON e preservado quando possivel. Exemplo: `original: "{{ body }}"` vira o objeto original.
- Quando o template esta dentro de outra string, o resultado e string. Exemplo: `"order-{{ body.id }}"`.
- Se uma expressao nao existe, o valor vira `null` em template inteiro ou string vazia em template parcial.

Exemplo com target HTTP e header dinamico:

```yaml
pipeline:
  id: http-to-http-template
  source:
    type: http
    path: /events
  transform:
    engine: template
    output:
      headers:
        x-postio-event: "{{ body.event }}"
      query:
        event: "{{ body.event }}"
        source: "{{ headers.x-source }}"
      body:
        event: "{{ body.event }}"
        source: "{{ headers.x-source }}"
  target:
    type: http
    method: POST
    url: https://example.com/events
```

Exemplo com target SQS e atributos dinamicos:

```yaml
pipeline:
  id: http-to-sqs-template
  source:
    type: http
    path: /orders/{tenant}
  transform:
    engine: template
    output:
      body:
        event: "{{ body.event }}"
        tenant: "{{ params.tenant }}"
        original: "{{ body }}"
      attributes:
        event: "{{ body.event }}"
        tenant: "{{ params.tenant }}"
        priority: "{{ body.priority }}"
  target:
    type: sqs
    queue: orders-output
```

Exemplo lendo SQS e enviando para HTTP:

```yaml
pipeline:
  id: sqs-to-http-template
  source:
    type: sqs
    queue: orders-input
    batchSize: 5
    waitTimeSeconds: 10
    visibilityTimeoutSeconds: 30
  transform:
    engine: template
    output:
      headers:
        content-type: application/json
        x-postio-source: "{{ context.sourceType }}"
        x-postio-event: "{{ body.event }}"
      query:
        event: "{{ body.event }}"
        source: "{{ context.sourceType }}"
      body:
        event: "{{ body.event }}"
        orderId: "{{ body.order.id }}"
        total: "{{ body.order.total }}"
        requestId: "{{ context.requestId }}"
        sourceType: "{{ context.sourceType }}"
        original: "{{ body }}"
  target:
    type: http
    method: POST
    url: https://api.example.com/orders/events
    timeoutMs: 5000
```

Mensagem de entrada na fila:

```json
{
  "event": "order.created",
  "order": {
    "id": "ord-1",
    "total": 99.9
  }
}
```

Request enviado ao HTTP target:

```json
{
  "event": "order.created",
  "orderId": "ord-1",
  "total": 99.9,
  "requestId": "gerado-pelo-postio",
  "sourceType": "sqs",
  "original": {
    "event": "order.created",
    "order": {
      "id": "ord-1",
      "total": 99.9
    }
  }
}
```

### Transform HTTP

O transform `http` chama um endpoint externo para produzir o novo payload. O Postio envia o payload atual como string no body da requisicao e espera a resposta como string. Em sucesso `2xx`, o body da resposta vira o payload que sera enviado ao target. Em status nao `2xx` ou erro de chamada, a pipeline falha antes do target.

```yaml
pipeline:
  id: http-transform-to-sqs
  source:
    type: http
    method: POST
    path: /orders
  transform:
    engine: http
    method: POST
    url: https://transformer.example.com/orders
    timeoutMs: 3000
    headers:
      x-transformer: orders
  target:
    type: sqs
    queue: orders-output
```

Campos de `transform.engine: http`:

| Propriedade | Obrigatoria | Padrao | Descricao |
| --- | --- | --- | --- |
| `engine` | sim | - | Deve ser `http`. |
| `method` | nao | `POST` | Metodo usado para chamar o transformador. |
| `url` | sim | - | Endpoint externo de transformacao. |
| `headers` | nao | `connection: keep-alive` | Headers fixos enviados ao endpoint transformador. Quando `connection` nao e configurado, o Postio envia `connection: keep-alive`. |
| `timeoutMs` | nao | sem timeout por request | Timeout da chamada em milissegundos. |

Exemplo de entrada:

```json
{
  "id": "ord-1",
  "total": 99.9
}
```

Exemplo de resposta do transformador:

```json
{
  "event": "order.received",
  "id": "ord-1",
  "total": 99.9
}
```

Nesse caso, o target recebe exatamente o body retornado pelo transformador.

### Exemplos de pipeline

HTTP para SQS:

```yaml
pipeline:
  id: http-to-sqs-orders
  source:
    type: http
    method: POST
    path: /orders
  target:
    type: sqs
    queue: orders-output
```

Chamada:

```bash
curl -X POST http://127.0.0.1:8080/orders \
  -H 'content-type: application/json' \
  -d '{"id":"ord-1","total":99.9}'
```

SQS para HTTP:

```yaml
pipeline:
  id: sqs-to-http-orders
  source:
    type: sqs
    queue: orders-input
    batchSize: 10
    waitTimeSeconds: 10
    visibilityTimeoutSeconds: 30
  target:
    type: http
    method: POST
    url: https://api.example.com/orders
    timeoutMs: 5000
```

## Instalacao

### Pre-requisitos

- Rust stable.
- Cargo.
- Credenciais AWS validas para os destinos usados.
- Docker e Docker Compose, apenas se quiser subir Grafana/Jaeger localmente.

### Clonar e preparar

```bash
git clone <repo-url>
cd postio
cargo build
```

### Configurar ambiente

Copie o arquivo de exemplo se quiser iniciar com variaveis locais:

```bash
cp .env.example .env
```

As credenciais AWS seguem a cadeia padrao do AWS SDK. Exemplos comuns:

```bash
export AWS_REGION=us-east-1
export AWS_PROFILE=default
```

Ou, em ambientes automatizados:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_SESSION_TOKEN=...
export AWS_REGION=us-east-1
```

## Executando localmente

### Sem observabilidade local

```bash
export POSTIO_CONFIG=config/example.yaml
export OTEL_ENABLED=false
cargo run
```

Por padrao a API sobe em `127.0.0.1:8080`.

### Com Grafana e Jaeger

```bash
docker compose up -d jaeger grafana
export POSTIO_CONFIG=config/example.yaml
cargo run
```

Servicos locais:

- API: `http://127.0.0.1:8080`
- Health: `http://127.0.0.1:8080/health`
- Swagger UI: `http://127.0.0.1:8080/docs`
- OpenAPI: `http://127.0.0.1:8080/openapi.json`
- Grafana: `http://localhost:3001`
- Jaeger: `http://localhost:16686`

## Configuracao da aplicacao

| Variavel | Padrao | Descricao |
| --- | --- | --- |
| `APP_HOST` | `127.0.0.1` | Host onde o servidor HTTP escuta. Use `0.0.0.0` em container. |
| `APP_PORT` | `8080` | Porta HTTP. |
| `APP_CORS_ALLOW_ORIGINS` | permissivo | Lista separada por virgula ou `*`. |
| `APP_BODY_LIMIT_BYTES` | `1048576` | Limite maximo do corpo do request em bytes. |
| `POSTIO_CONFIG` | `config/example.yaml` | Caminho do arquivo YAML/JSON com as rotas. |
| `OTEL_ENABLED` | `true` | Habilita tracing OpenTelemetry. Use `false` para logs sem export OTEL. |
| `OTEL_SERVICE_NAME` | `postio` em `.env.example` | Nome do servico nos traces. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` em `.env.example` | Endpoint OTLP base. |
| `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` | `http://localhost:4318/v1/traces` em `.env.example` | Endpoint OTLP HTTP para traces. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | `http/protobuf` em `.env.example` | Protocolo OTLP. |
| `OTEL_TRACES_SAMPLER` | `always_on` em `.env.example` | Politica de amostragem dos traces. |
| `OTEL_RESOURCE_ATTRIBUTES` | `service.version=0.1.0,deployment.environment=local` em `.env.example` | Atributos fixos adicionados ao recurso OTEL. |

## Arquivo de rotas

O arquivo pode ser YAML ou JSON. A extensao `.json` ativa parser JSON; qualquer outra extensao usa YAML.

Estrutura raiz:

```yaml
routes:
  - id: route-id
    method: POST
    path: /path/{param}
    sink:
      type: sns
      topic: my-topic
```

### Propriedades de `routes[]`

| Propriedade | Obrigatoria | Padrao | Descricao |
| --- | --- | --- | --- |
| `id` | sim | - | Identificador unico da rota. Aparece em logs, spans e resposta. |
| `method` | nao | `POST` | Metodo HTTP. Na v0 apenas `POST` e registrado; outros metodos sao ignorados. |
| `path` | sim | - | Path Axum da rota. Deve iniciar com `/`. Params usam `{nome}`. |
| `sink` | sim | - | Destino que recebera o dado. Pode ser `sns`, `sqs` ou `s3`. |

Exemplo com path params:

```yaml
routes:
  - id: dynamic-topic
    path: /topic/{topicName}
    sink:
      type: sns
      topic: "{{ params.topicName }}"
```

## Templates

Templates usam `{{ ... }}` em strings e podem ler os seguintes objetos:

| Origem | Exemplo | Descricao |
| --- | --- | --- |
| `params` | `{{ params.topicName }}` | Parametros declarados no path da rota. |
| `query` | `{{ query.source }}` | Parametros da query string. |
| `headers` | `{{ headers.x-tenant-id }}` | Headers HTTP convertidos para string. Headers nao UTF-8 sao ignorados. |
| `form` | `{{ form.tenant }}` | Campos texto de requests `multipart/form-data`. |
| `file` | `{{ file.filename }}` | Metadados do primeiro arquivo de requests `multipart/form-data`. |
| `body` | `{{ body.customer.id }}` | Campos do body quando o request e JSON. |
| `context` | `{{ context.requestId }}` | Metadados gerados pelo Postio. |

Campos disponiveis em `file` para multipart:

| Campo | Descricao |
| --- | --- |
| `fieldName` | Nome do campo multipart que recebeu o arquivo. |
| `filename` | Nome original do arquivo enviado pelo cliente. |
| `contentType` | Content-Type do arquivo, quando informado pelo cliente. |
| `size` | Tamanho do arquivo em bytes. |

Campos disponiveis em `context`:

| Campo | Descricao |
| --- | --- |
| `requestId` | UUID gerado para o request. |
| `timestamp` | Timestamp UTC em RFC3339. |
| `method` | Metodo HTTP recebido. Na v0 sera `POST`. |
| `path` | Path real chamado pelo cliente. |
| `routeId` | `id` da rota configurada. |

Quando a string inteira e um template, o valor JSON original e preservado:

```yaml
message:
  customerId: "{{ body.customer.id }}"
  amount: "{{ body.amount }}"
```

Se `body.amount` for numero, ele continua numero no JSON renderizado antes de ser convertido para envio.

Quando o template esta embutido em outra string, o resultado final e string:

```yaml
key: "events/{{ context.requestId }}.json"
```

### Tratamento do body

- Body vazio vira `null`.
- Body JSON valido vira JSON.
- Body nao JSON, mas UTF-8, vira string.
- Body nao UTF-8 retorna `400 Bad Request`.
- Body `multipart/form-data` e parseado em `form` e `file`. Campos texto precisam ser UTF-8; arquivo pode ser binario.
- Para multipart, `body` vira um objeto com metadados: `{"form": {...}, "file": {...}}`. O conteudo binario do arquivo nao entra em `body`.

## Recurso SNS

O destino `sns` publica uma mensagem em um topico AWS SNS.

### Propriedades

| Propriedade | Obrigatoria | Template | Descricao |
| --- | --- | --- | --- |
| `type` | sim | nao | Deve ser `sns`. |
| `topicArn` | condicional | sim | ARN completo do topico. Se informado, e usado diretamente. |
| `topic` | condicional | sim | Nome do topico ou ARN. Obrigatorio quando `topicArn` nao existe. Nome e resolvido via `ListTopics` e cacheado em memoria. |
| `subject` | nao | sim | Subject da mensagem SNS. |
| `message` | nao | sim | Payload enviado ao SNS. Se ausente, usa o body completo do request. |
| `attributes` | nao | sim | Mapa de atributos SNS. Todos sao enviados como `String`. |

`topic` ou `topicArn` e obrigatorio.

### SNS com topico fixo

```yaml
routes:
  - id: orders-topic
    path: /orders
    sink:
      type: sns
      topicArn: arn:aws:sns:us-east-1:123456789012:orders-created
```

### SNS com topico dinamico

```yaml
routes:
  - id: dynamic-topic
    path: /events/{topic}
    sink:
      type: sns
      topic: "{{ params.topic }}"
      subject: "event from {{ query.source }}"
```

### SNS com mensagem e atributos

```yaml
routes:
  - id: payment-events
    path: /payments/{eventType}
    sink:
      type: sns
      topic: payment-events
      subject: "{{ params.eventType }}"
      message:
        eventType: "{{ params.eventType }}"
        paymentId: "{{ body.payment.id }}"
        amount: "{{ body.payment.amount }}"
        requestId: "{{ context.requestId }}"
      attributes:
        tenant: "{{ headers.x-tenant-id }}"
        source: "{{ query.source }}"
```

Resposta de sucesso SNS:

```json
{
  "route_id": "payment-events",
  "sink": "sns",
  "status": "accepted",
  "message_id": "..."
}
```

## Recurso SQS

O destino `sqs` envia uma mensagem para uma fila AWS SQS.

### Propriedades

| Propriedade | Obrigatoria | Template | Descricao |
| --- | --- | --- | --- |
| `type` | sim | nao | Deve ser `sqs`. |
| `queueUrl` | condicional | sim | URL completa da fila. Se informada, e usada diretamente. |
| `queue` | condicional | sim | Nome da fila ou URL. Obrigatorio quando `queueUrl` nao existe. Nome e resolvido via `GetQueueUrl` e cacheado em memoria. |
| `message` | nao | sim | Payload enviado ao SQS. Se ausente, usa o body completo do request. |
| `attributes` | nao | sim | Mapa de atributos SQS. Todos sao enviados como `String`. |

`queue` ou `queueUrl` e obrigatorio.

### SQS com URL fixa

```yaml
routes:
  - id: queue-input
    path: /my-queue
    sink:
      type: sqs
      queueUrl: https://sqs.us-east-1.amazonaws.com/123456789012/my-queue
```

### SQS com nome dinamico

```yaml
routes:
  - id: tenant-queue
    path: /tenants/{tenant}/queue
    sink:
      type: sqs
      queue: "{{ params.tenant }}-input"
      message:
        requestId: "{{ context.requestId }}"
        tenant: "{{ params.tenant }}"
        payload: "{{ body }}"
      attributes:
        tenant: "{{ params.tenant }}"
        source: "{{ query.source }}"
```

Resposta de sucesso SQS:

```json
{
  "route_id": "tenant-queue",
  "sink": "sqs",
  "status": "accepted",
  "message_id": "..."
}
```

## Recurso S3

O destino `s3` grava um objeto em um bucket AWS S3.

### Propriedades

| Propriedade | Obrigatoria | Template | Descricao |
| --- | --- | --- | --- |
| `type` | sim | nao | Deve ser `s3`. |
| `bucket` | sim | sim | Bucket onde o objeto sera criado. |
| `key` | sim | sim | Chave do objeto dentro do bucket. |
| `contentType` | nao | sim | Content-Type enviado ao S3. Se ausente em multipart com arquivo, usa o content type do arquivo. |
| `object` | nao | sim | Conteudo gravado. Se ausente, usa o body completo do request. Em multipart com arquivo, grava os bytes reais do arquivo. |
| `metadata` | nao | sim | Metadados do objeto. Todos sao convertidos para string. |

### S3 com upload multipart/form-data

Quando o request e `multipart/form-data` e existe um campo de arquivo, o S3 grava os bytes do primeiro arquivo encontrado. Os campos texto ficam disponiveis em `form` e os metadados do arquivo em `file`.

```yaml
routes:
  - id: upload-file
    path: /upload/{tenant}
    sink:
      type: s3
      bucket: my-ingestion-bucket
      key: "{{ params.tenant }}/{{ form.folder }}/{{ file.filename }}"
      metadata:
        tenant: "{{ params.tenant }}"
        folder: "{{ form.folder }}"
        originalFilename: "{{ file.filename }}"
```

Chamada:

```bash
curl -X POST http://127.0.0.1:8080/upload/acme \
  -F 'folder=invoices' \
  -F 'file=@./invoice.pdf;type=application/pdf'
```

Se `object` for configurado em uma rota S3, o Postio grava o objeto renderizado em vez dos bytes do arquivo multipart.

### S3 simples

```yaml
routes:
  - id: raw-file
    path: /myfile
    sink:
      type: s3
      bucket: my-ingestion-bucket
      key: "raw/{{ context.requestId }}.json"
      contentType: application/json
```

### S3 com bucket e arquivo dinamicos

```yaml
routes:
  - id: dynamic-file
    path: /file/{bucket}/{filename}
    sink:
      type: s3
      bucket: "{{ params.bucket }}"
      key: "{{ params.filename }}"
      contentType: application/json
```

### S3 com objeto transformado e metadata

```yaml
routes:
  - id: audit-file
    path: /audit/{tenant}/{entity}
    sink:
      type: s3
      bucket: audit-input
      key: "{{ params.tenant }}/{{ params.entity }}/{{ context.requestId }}.json"
      contentType: application/json
      object:
        tenant: "{{ params.tenant }}"
        entity: "{{ params.entity }}"
        source: "{{ query.source }}"
        receivedAt: "{{ context.timestamp }}"
        payload: "{{ body }}"
      metadata:
        tenant: "{{ params.tenant }}"
        routeId: "{{ context.routeId }}"
```

Resposta de sucesso S3:

```json
{
  "route_id": "audit-file",
  "sink": "s3",
  "status": "created",
  "bucket": "audit-input",
  "key": "tenant-a/order/....json",
  "etag": "\"...\""
}
```

## Exemplos simples

### Um endpoint para SNS

```yaml
routes:
  - id: anywhere-topic
    path: /anywhere
    sink:
      type: sns
      topic: my-topic
```

Chamada:

```bash
curl -X POST http://127.0.0.1:8080/anywhere \
  -H 'content-type: application/json' \
  -d '{"hello":"sns"}'
```

### Um endpoint para SQS

```yaml
routes:
  - id: my-queue
    path: /my-queue
    sink:
      type: sqs
      queue: my-queue
```

Chamada:

```bash
curl -X POST http://127.0.0.1:8080/my-queue \
  -H 'content-type: application/json' \
  -d '{"hello":"sqs"}'
```

### Um endpoint para S3

```yaml
routes:
  - id: my-file
    path: /myfile
    sink:
      type: s3
      bucket: my-ingestion-bucket
      key: "requests/{{ context.requestId }}.json"
      contentType: application/json
```

Chamada:

```bash
curl -X POST http://127.0.0.1:8080/myfile \
  -H 'content-type: application/json' \
  -d '{"hello":"s3"}'
```

## Exemplo complexo

Este exemplo usa varias rotas, varios destinos, parametros dinamicos, headers, query string e transformacao do payload.

```yaml
routes:
  - id: tenant-events-topic
    path: /tenants/{tenant}/events/{eventType}
    sink:
      type: sns
      topic: "{{ params.tenant }}-events"
      subject: "{{ params.eventType }}"
      message:
        eventType: "{{ params.eventType }}"
        tenant: "{{ params.tenant }}"
        source: "{{ query.source }}"
        correlationId: "{{ headers.x-correlation-id }}"
        payload: "{{ body }}"
        receivedAt: "{{ context.timestamp }}"
      attributes:
        tenant: "{{ params.tenant }}"
        eventType: "{{ params.eventType }}"
        requestId: "{{ context.requestId }}"

  - id: tenant-command-queue
    path: /tenants/{tenant}/commands/{commandName}
    sink:
      type: sqs
      queue: "{{ params.tenant }}-commands"
      message:
        command: "{{ params.commandName }}"
        tenant: "{{ params.tenant }}"
        payload: "{{ body }}"
        requestId: "{{ context.requestId }}"
      attributes:
        tenant: "{{ params.tenant }}"
        command: "{{ params.commandName }}"

  - id: tenant-raw-archive
    path: /tenants/{tenant}/archive/{entity}/{fileName}
    sink:
      type: s3
      bucket: central-ingestion-archive
      key: "{{ params.tenant }}/{{ params.entity }}/{{ params.fileName }}"
      contentType: application/json
      object:
        tenant: "{{ params.tenant }}"
        entity: "{{ params.entity }}"
        originalFileName: "{{ params.fileName }}"
        source: "{{ query.source }}"
        requestId: "{{ context.requestId }}"
        payload: "{{ body }}"
      metadata:
        tenant: "{{ params.tenant }}"
        entity: "{{ params.entity }}"
        source: "{{ query.source }}"
```

Chamadas:

```bash
curl -X POST 'http://127.0.0.1:8080/tenants/acme/events/order-created?source=checkout' \
  -H 'content-type: application/json' \
  -H 'x-correlation-id: corr-123' \
  -d '{"order":{"id":"ord-1","total":99.9}}'

curl -X POST 'http://127.0.0.1:8080/tenants/acme/commands/recalculate-score' \
  -H 'content-type: application/json' \
  -d '{"customerId":"cus-1"}'

curl -X POST 'http://127.0.0.1:8080/tenants/acme/archive/orders/order-1.json?source=batch' \
  -H 'content-type: application/json' \
  -d '{"order":{"id":"ord-1","total":99.9}}'
```

## Chamando a API

Rotas configuradas retornam:

- `202 Accepted` para SNS e SQS.
- `201 Created` para S3.
- `400 Bad Request` quando o body nao pode ser interpretado como UTF-8.
- Erros `5xx` quando a chamada ao destino falha.

Exemplo de health check:

```bash
curl http://127.0.0.1:8080/health
```

Exemplo de echo:

```bash
curl -X POST http://127.0.0.1:8080/echo \
  -H 'content-type: application/json' \
  -d '{"debug":true}'
```

## Rotas sandbox

As rotas abaixo existem no ambiente sandbox para validacao operacional do pipeline. Elas nao fazem parte da API de negocio e devem ser tratadas como endpoints sandbox-only.

### POST /postio/pipeline/error

Valida o comportamento de erro do pipeline.

Fluxo:

```text
HTTP input -> HTTP target invalido
```

O target aponta para uma porta local que nao deve responder. A rota deve retornar erro e registrar a falha nos traces.

Ela existe para validar:

- resposta `502`;
- span `postio.pipeline.target.send`;
- `result.status=failed`;
- `error.kind=target_send_failed`;
- propagacao de `traceparent`;
- visualizacao da falha no Tempo/Grafana.

### POST /postio/pipeline/template/http/{tenant}

Valida `transform.engine: template` montando um request HTTP de saida.

Fluxo:

```text
HTTP input -> transform template -> HTTP target de captura
```

Ela existe para validar:

- `body` transformado;
- headers dinamicos, como `x-postio-event`;
- query string dinamica;
- uso de `params.tenant`;
- uso de `query.source`;
- uso de headers recebidos da fonte HTTP.

Exemplo:

```bash
curl -X POST 'http://gateway.sdx.autob/postio/pipeline/template/http/acme?source=checkout' \
  -H 'content-type: application/json' \
  -H 'x-source: integration' \
  -d '{"event":"order.created","priority":3,"total":42}'
```

## Observabilidade

Postio foi pensado para permitir acompanhar a operacao de ponta a ponta via traces.

### Subindo stack local

```bash
docker compose up -d jaeger grafana
```

URLs:

- Grafana: `http://localhost:3001`
- Jaeger: `http://localhost:16686`
- OTLP HTTP: `http://localhost:4318`
- OTLP gRPC: `http://localhost:4317`

### Spans principais

| Span | Descricao |
| --- | --- |
| `postio.ingest.request` | Span principal do request de injestao. |
| `postio.ingest.parse_body` | Parse do body em JSON, texto ou null. |
| `postio.ingest.build_context` | Montagem do contexto de templates. |
| `postio.ingest.dispatch` | Envio ao destino configurado. |
| `postio.aws.sns` | Escopo da operacao SNS. |
| `postio.aws.sns.publish` | Chamada `Publish` no SNS. |
| `postio.aws.sns.list_topics` | Resolucao de nome de topico para ARN quando necessario. |
| `postio.aws.sqs` | Escopo da operacao SQS. |
| `postio.aws.sqs.send_message` | Chamada `SendMessage` no SQS. |
| `postio.aws.sqs.get_queue_url` | Resolucao de nome de fila para URL quando necessario. |
| `postio.aws.s3` | Escopo da operacao S3. |
| `postio.aws.s3.put_object` | Chamada `PutObject` no S3. |

### Atributos uteis nos traces

- `request_id`
- `route_id`
- `sink_type`
- `http.method`
- `http.route`
- `http.target`
- `request.body_bytes`
- `response.status_code`
- `sink.status`
- `aws.sns.message_id`
- `aws.sqs.message_id`
- `s3.bucket`
- `s3.key`
- `s3.etag`

Por seguranca, o body completo nao e registrado automaticamente como atributo de trace. O tamanho do body e registrado em `request.body_bytes`.

## Operacao

### Imagem Docker por artefato

`Dockerfile.artifact` espera um binario precompilado em `artifacts/<bin-name>/<arch>/` e aceita `BIN_NAME`.

```bash
docker build \
  -f Dockerfile.artifact \
  --build-arg TARGETARCH=amd64 \
  --build-arg BIN_NAME=postio \
  .
```

### Deploy

Para ambientes containerizados, configure pelo menos:

```bash
APP_HOST=0.0.0.0
APP_PORT=8080
POSTIO_CONFIG=/etc/postio/config.yaml
AWS_REGION=us-east-1
OTEL_SERVICE_NAME=postio
OTEL_EXPORTER_OTLP_ENDPOINT=http://otel-collector:4318
```

Monte o arquivo de rotas em `POSTIO_CONFIG` e garanta permissoes IAM para:

- SNS: `sns:Publish` e, se usar `topic` por nome, `sns:ListTopics`.
- SQS: `sqs:SendMessage` e, se usar `queue` por nome, `sqs:GetQueueUrl`.
- S3: `s3:PutObject`.

## Testes e desenvolvimento

Comandos principais:

```bash
cargo fmt -- --check
cargo build
OTEL_ENABLED=false cargo test
```

Testes de integracao:

```bash
OTEL_ENABLED=false cargo test --test integration
```

## Arquitetura

Fluxo principal:

```text
HTTP route -> parse body -> build template context -> render sink config -> AWS sink
```

Organizacao relevante:

```text
src/
  bridge/
    aws.rs          # dispatcher AWS para SNS, SQS e S3
    config.rs       # modelo e validacao do YAML/JSON
    dispatcher.rs   # contrato de dispatch
    template.rs     # renderizacao de templates
  pipeline/
    config/
      mod.rs        # schema raiz da pipeline
      source.rs     # schemas de source HTTP/SQS
      target.rs     # schemas de target HTTP/SQS
      transform.rs  # schemas de transform
      validate.rs   # schemas de validacao
    runtime.rs      # workers internos da pipeline
    validation/     # engines de validacao
  routes/
    ingest.rs       # rotas dinamicas de injestao
    system.rs       # health e echo
  config.rs         # variaveis de ambiente
  state.rs          # estado compartilhado da aplicacao
```

## Limitacoes da v0

- Apenas rotas `POST` sao registradas.
- Apenas destinos `sns`, `sqs` e `s3` estao implementados.
- Nao ha hot reload do arquivo de configuracao; reinicie a aplicacao apos alterar rotas.
- Atributos SNS/SQS e metadata S3 sao convertidos para string.
- Resolucao de `topic` por nome usa `ListTopics`; prefira `topicArn` quando quiser evitar essa chamada.
- Resolucao de `queue` por nome usa `GetQueueUrl`; prefira `queueUrl` quando quiser evitar essa chamada.
- Nao ha autenticacao/autorizacao HTTP embutida na v0.

## Troubleshooting

### A rota nao responde

Verifique se o arquivo apontado por `POSTIO_CONFIG` foi carregado e se a rota tem `method: POST` ou omite `method`.

### Erro ao resolver topico SNS

Se usar `topic` por nome, a aplicacao precisa de `sns:ListTopics`. Para simplificar, configure `topicArn` diretamente.

### Erro ao resolver fila SQS

Se usar `queue` por nome, a aplicacao precisa de `sqs:GetQueueUrl`. Para simplificar, configure `queueUrl` diretamente.

### Traces nao aparecem

Confirme:

```bash
docker compose ps
echo "$OTEL_ENABLED"
echo "$OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
```

Use `OTEL_ENABLED=false` apenas quando quiser desabilitar exportacao OTEL.

## Template Bootstrap

Este repositorio veio de um template Rust. Ao criar um novo repositorio a partir dele, rode:

```bash
./scripts/init-template.sh
```

Para sobrescrever o nome detectado:

```bash
./scripts/init-template.sh my-new-api
```

O script ajusta referencias principais como `Cargo.toml`, imports Rust, README, `.env.example` e `Dockerfile.artifact`, depois executa `cargo build` e `OTEL_ENABLED=false cargo test`.
