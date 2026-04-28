# Postio Pipeline Engine Plan

Este documento planeja a evolucao do Postio de uma API de entrada HTTP para um roteador de pipelines de dados.

O objetivo e permitir que qualquer fonte suportada envie dados para qualquer destino suportado, com transformacoes no meio.

Na v1 inicial, cada processo/deployment Postio executa uma unica pipeline. Para executar varias pipelines, a recomendacao e subir varios deployments Postio, um por pipeline.

```text
source -> decode -> validate -> transform request -> target -> transform response -> ack/reply
```

## Objetivo

Postio deve evoluir para uma aplicacao de integracao e injestao configuravel, onde cada pipeline declara:

- de onde o dado entra;
- como o dado e interpretado;
- como o dado e transformado;
- para onde o dado vai;
- como a resposta do destino e tratada;
- como a fonte original recebe ack, retry ou resposta.

Exemplos desejados:

- HTTP -> SQS
- HTTP -> SNS
- HTTP -> S3
- SQS -> HTTP
- SQS -> SQS
- SQS -> S3
- Futuro: AMQP -> HTTP, Kafka -> S3, etc.

## Principios

- Manter a v0 atual funcionando.
- Configuracao declarativa em YAML/JSON.
- Transformacao deterministica, observavel e limitada.
- Validacao deve ser uma etapa oficial antes da transformacao.
- Separar claramente fonte, transformacao e destino.
- Nao acoplar HTTP ao core de pipeline.
- Steps devem trocar uma mensagem canonica interna, nao tipos especificos de HTTP, SQS ou S3.
- SDK clients e recursos externos devem ser criados no startup e reaproveitados por todos os pipelines.
- Projetar engines de transformacao como plugaveis.
- Deve ser facil adicionar novos tipos de source, target e transform sem alterar o core do runtime.
- O core deve depender de traits/contratos, nao de implementacoes concretas.
- v1 inicial executa uma unica pipeline por processo.
- Multipipeline no mesmo processo fica como possibilidade futura, nao como requisito inicial.
- Comecar simples com Rhai antes de considerar Deno/JavaScript.

## Modelo Conceitual

```text
Pipeline
  Source
    recebe ou busca input
  Codec
    decodifica bytes em dados estruturados
  Validation
    valida contrato, formato e regras antes da transformacao
  Request Transform
    monta request para o target
  Target
    envia dados para destino
  Response Transform
    interpreta resposta do target
  Completion Policy
    decide ack, retry, response ou dead-letter
```

## Exemplo De Configuracao

```yaml
pipeline:
  id: http-to-sqs-orders
  source:
    type: http
    method: POST
    path: /orders

  input:
    contentType: application/json

  validate:
    engine: jsonschema
    schema:
      type: object
      required:
        - id
        - total
      properties:
        id:
          type: string
        total:
          type: number

  transform:
    engine: rhai
    script: |
      #{
        body: #{
          eventType: "order.received",
          payload: input.body,
          requestId: context.requestId
        },
        attributes: #{
          source: "http"
        }
      }

  target:
    type: sqs
    queue: orders-events

  responseTransform:
    engine: rhai
    script: |
      #{
        status: 202,
        body: #{
          ok: true,
          messageId: target.messageId
        }
      }
```

```yaml
pipeline:
  id: sqs-to-http-orders
  source:
    type: sqs
    queue: orders-input
    batchSize: 10
    visibilityTimeoutSeconds: 30

  input:
    contentType: application/json

  validation:
    steps:
      - engine: jsonschema
        schemaRef: ./schemas/order.schema.json
      - engine: rhai
        script: |
          if input.body.total <= 0 {
            throw "total must be greater than zero";
          }
          true

  transform:
    engine: rhai
    script: |
      let order = input.body;

      #{
        method: "POST",
        url: "https://api.example.com/orders",
        headers: #{
          "content-type": "application/json",
          "x-source": "postio"
        },
        body: #{
          id: order.id,
          total: order.total,
          receivedAt: context.timestamp
        }
      }

  target:
    type: http
    timeoutMs: 5000

  responseTransform:
    engine: rhai
    script: |
      if target.status >= 200 && target.status < 300 {
        #{ ack: true }
      } else {
        #{ ack: false, retry: true }
      }
```

## Compatibilidade Com A v0

A configuracao atual baseada em `routes` deve continuar funcionando.

Internamente, cada rota v0 pode ser convertida para um pipeline equivalente:

```yaml
routes:
  - id: postio-s3-api
    path: /postio/s3
    sink:
      type: s3
      bucket: my-bucket
      key: "requests/{{ context.requestId }}.json"
```

Equivale conceitualmente a:

```yaml
pipeline:
  id: postio-s3-api
  source:
    type: http
    method: POST
    path: /postio/s3
  target:
    type: s3
    bucket: my-bucket
    key: "requests/{{ context.requestId }}.json"
```

## Schemas De Configuracao

Esta secao define os contratos esperados para configuracao YAML/JSON e para o request canonico produzido por `transform.output`.

Regra central:

```text
target = defaults/config base
transform.output = request final ou parcial que pode sobrescrever defaults do target
```

Separar sempre:

- `TargetConfig`: configuracao base declarada em `target`.
- `TargetRequest`: request canonico final usado pelo target, normalmente produzido por `transform.output` e mergeado com `TargetConfig`.

### Pipeline

```yaml
apiVersion: postio.dev/v1alpha1
kind: Pipeline
pipeline:
  id: string
  enabled: boolean
  source: SourceConfig
  input: InputConfig
  validation: ValidationConfig
  validate: ValidationStep
  transform: TransformConfig
  target: TargetConfig
  responseTransform: TransformConfig
  # source.completion vive dentro do source
  # target.retry vive dentro do target
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `id` | sim | Identificador unico do pipeline |
| `enabled` | nao | Liga/desliga pipeline; padrao `true` |
| `source` | sim | Entrada do pipeline |
| `input` | nao | Dicas de decode/content type |
| `validation` | nao | Lista explicita de validacoes |
| `validate` | nao | Atalho para uma validacao unica |
| `transform` | nao | Transformacao antes do target; padrao `noop` |
| `target` | sim | Destino do pipeline |
| `responseTransform` | nao | Transformacao da resposta do target |
| `source.completion` | nao | Politica de resposta/ack/retry/drop/deadLetter do source |

### InputConfig

```yaml
input:
  contentType: string
  decodeAs: json | text | binary | multipart | auto
  maxBodyBytes: number
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `contentType` | nao | Content type esperado ou assumido |
| `decodeAs` | nao | Estrategia de decode; padrao `auto` |
| `maxBodyBytes` | nao | Limite por pipeline |

### SourceConfig: HTTP

```yaml
source:
  type: http
  method: POST
  path: /orders/{id}
  auth:
    type: none
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `type` | sim | `http` |
| `method` | nao | Metodo HTTP; inicialmente `POST` |
| `path` | sim | Path exposto pela API |
| `auth` | nao | Futuro: auth do endpoint |

Source HTTP produz metadata:

```yaml
params: map<string,string>
query: map<string,string>
headers: map<string,string>
method: string
path: string
```

### SourceConfig: SQS

```yaml
source:
  type: sqs
  queue: orders-input
  queueUrl: https://sqs.us-east-1.amazonaws.com/123/orders-input
  batchSize: 10
  waitTimeSeconds: 10
  visibilityTimeoutSeconds: 60
  maxConcurrency: 4
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `type` | sim | `sqs` |
| `queue` | condicional | Nome da fila; usado quando `queueUrl` ausente |
| `queueUrl` | condicional | URL da fila |
| `batchSize` | nao | Mensagens por poll |
| `waitTimeSeconds` | nao | Long polling |
| `visibilityTimeoutSeconds` | nao | Visibility timeout durante processamento |
| `maxConcurrency` | nao | Concorrencia do source |

Source SQS produz metadata:

```yaml
messageId: string
receiptHandle: string
attributes: map<string,string>
messageAttributes: map<string,string>
queueUrl: string
```

### TargetConfig: HTTP

```yaml
target:
  type: http
  method: POST
  url: https://api.example.com/events
  headers:
    content-type: application/json
  timeoutMs: 5000
  retry:
    maxAttempts: 3
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `type` | sim | `http` |
| `method` | nao | Metodo default |
| `url` | condicional | URL default; obrigatoria se transform nao produzir `url` |
| `headers` | nao | Headers default |
| `timeoutMs` | nao | Timeout da chamada |
| `retry` | nao | Retry do target |

### TargetRequest: HTTP

Produzido por `transform.output`:

```yaml
transform:
  engine: template
  output:
    method: POST
    url: "https://api.example.com/orders/{{ body.orderId }}"
    headers:
      content-type: application/json
      x-request-id: "{{ context.requestId }}"
    query:
      source: postio
    body:
      orderId: "{{ body.orderId }}"
      total: "{{ body.total }}"
    timeoutMs: 5000
```

Schema:

```yaml
method: string
url: string
headers: map<string,string>
query: map<string,string|number|boolean>
body: any
timeoutMs: number
```

Merge:

- `transform.output.method` sobrescreve `target.method`.
- `transform.output.url` sobrescreve `target.url`.
- `headers` sao mesclados, com output vencendo conflitos.
- `query` vem do output.
- `body` vem do output ou do payload original no noop.
- `timeoutMs` do output sobrescreve `target.timeoutMs`.

### TargetConfig: SQS

```yaml
target:
  type: sqs
  queue: orders-events
  queueUrl: https://sqs.us-east-1.amazonaws.com/123/orders-events
  delaySeconds: 0
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `type` | sim | `sqs` |
| `queue` | condicional | Nome da fila; usado quando `queueUrl` ausente |
| `queueUrl` | condicional | URL da fila |
| `delaySeconds` | nao | Delay default |

### TargetRequest: SQS

```yaml
transform:
  engine: template
  output:
    body:
      eventType: order.created
      payload: "{{ body }}"
    attributes:
      tenant: "{{ headers.x-tenant-id }}"
      source: postio
    delaySeconds: 0
    messageGroupId: "{{ body.customerId }}"
    messageDeduplicationId: "{{ context.requestId }}"
```

Schema:

```yaml
body: any
attributes: map<string,string|number|boolean>
delaySeconds: number
messageGroupId: string
messageDeduplicationId: string
```

Regras:

- `body` objeto/array deve ser serializado como JSON string.
- `body` string deve ser enviado como string.
- `attributes` devem ser convertidos para atributos SQS `String`.
- `messageGroupId` e `messageDeduplicationId` sao usados para filas FIFO.

### TargetConfig: SNS

```yaml
target:
  type: sns
  topic: orders-topic
  topicArn: arn:aws:sns:us-east-1:123:orders-topic
  subject: orders
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `type` | sim | `sns` |
| `topic` | condicional | Nome ou ARN do topico |
| `topicArn` | condicional | ARN direto do topico |
| `subject` | nao | Subject default |

### TargetRequest: SNS

```yaml
transform:
  engine: template
  output:
    message:
      eventType: order.created
      payload: "{{ body }}"
    subject: "order {{ body.orderId }}"
    attributes:
      tenant: "{{ headers.x-tenant-id }}"
      source: postio
```

Schema:

```yaml
message: any
subject: string
attributes: map<string,string|number|boolean>
```

Regras:

- `message` objeto/array deve ser serializado como JSON string.
- `message` string deve ser enviado como string.
- `attributes` devem ser convertidos para atributos SNS `String`.
- `subject` do output sobrescreve `target.subject`.

### TargetConfig: S3

```yaml
target:
  type: s3
  bucket: my-bucket
  key: "requests/{{ context.requestId }}.json"
  contentType: application/json
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `type` | sim | `s3` |
| `bucket` | sim | Bucket default |
| `key` | sim | Key default |
| `contentType` | nao | Content-Type default |

### TargetRequest: S3

```yaml
transform:
  engine: template
  output:
    bucket: my-bucket
    key: "orders/{{ body.orderId }}.json"
    contentType: application/json
    object:
      orderId: "{{ body.orderId }}"
      payload: "{{ body }}"
    metadata:
      tenant: "{{ headers.x-tenant-id }}"
      source: postio
```

Schema:

```yaml
bucket: string
key: string
contentType: string
object: any
metadata: map<string,string|number|boolean>
```

Regras:

- `bucket`, `key` e `contentType` do output sobrescrevem o target.
- `object` ausente usa payload atual.
- Para multipart com arquivo e `object` ausente, usa bytes do arquivo.
- `metadata` deve ser convertida para strings.

### TransformConfig: Noop

```yaml
transform:
  engine: noop
```

Regras:

- Retorna o payload original.
- E o default quando `transform` nao for definido.

### TransformConfig: Template

```yaml
transform:
  engine: template
  output:
    body:
      name: "{{ params.name }}"
      payload: "{{ body }}"
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `engine` | sim | `template` |
| `output` | sim | Objeto renderizado para o target request |

Regras:

- Usa sintaxe `{{ ... }}`.
- Pode acessar `params`, `query`, `headers`, `attributes`, `body`, `form`, `file`, `context`.
- Se a string inteira e template, preserva tipo JSON quando possivel.
- Se template esta embutido em outra string, resultado e string.

### TransformConfig: Rhai

```yaml
transform:
  engine: rhai
  script: |
    #{
      body: #{
        eventType: "order.created",
        payload: input.body
      }
    }
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `engine` | sim | `rhai` |
| `script` | condicional | Script inline |
| `scriptRef` | condicional | Caminho/referencia para script |

### TransformConfig: External HTTP

```yaml
transform:
  engine: external-http
  url: https://transformer.example.com/orders
  method: POST
  mode: string
  requestContentType: text/plain
  responseContentType: text/plain
  timeoutMs: 3000
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `engine` | sim | `external-http` |
| `url` | sim | Endpoint externo |
| `method` | nao | Metodo; padrao `POST` |
| `mode` | sim | Inicialmente `string` |
| `requestContentType` | nao | Content-Type enviado |
| `responseContentType` | nao | Content-Type esperado |
| `timeoutMs` | sim | Timeout obrigatorio |

### TransformConfig: External gRPC

```yaml
transform:
  engine: external-grpc
  endpoint: dns:///transformer.default.svc.cluster.local:50051
  service: postio.transform.v1.TransformService
  method: Transform
  mode: string
  timeoutMs: 3000
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `engine` | sim | `external-grpc` |
| `endpoint` | sim | Endpoint gRPC |
| `service` | sim | Nome do service |
| `method` | sim | Metodo chamado |
| `mode` | sim | Inicialmente `string` |
| `timeoutMs` | sim | Timeout obrigatorio |

### ValidationConfig: Noop

```yaml
validate:
  engine: noop
```

Regras:

- Sempre valido.
- E o default quando `validate` e `validation` nao forem definidos.

### ValidationConfig: Content Type

```yaml
validation:
  steps:
    - engine: contentType
      allow:
        - application/json
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `engine` | sim | `contentType` |
| `allow` | sim | Lista de content types permitidos |

### ValidationConfig: JSON Schema

```yaml
validation:
  steps:
    - engine: jsonschema
      schemaRef: ./schemas/order.schema.json
```

Ou:

```yaml
validation:
  steps:
    - engine: jsonschema
      schema:
        type: object
        required:
          - id
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `engine` | sim | `jsonschema` |
| `schema` | condicional | Schema inline |
| `schemaRef` | condicional | Referencia para schema |

### ValidationConfig: Rhai

```yaml
validation:
  steps:
    - engine: rhai
      script: |
        if input.body.total <= 0 {
          throw "total must be greater than zero";
        }
        true
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `engine` | sim | `rhai` |
| `script` | condicional | Script inline |
| `scriptRef` | condicional | Caminho/referencia para script |

### ValidationConfig: Multipart

```yaml
validation:
  steps:
    - engine: multipart
      requiredFile: true
      maxFileSizeBytes: 10485760
      allowedContentTypes:
        - application/pdf
      requiredFields:
        - folder
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `engine` | sim | `multipart` |
| `requiredFile` | nao | Exige arquivo |
| `maxFileSizeBytes` | nao | Tamanho maximo do arquivo |
| `allowedContentTypes` | nao | Content types de arquivo permitidos |
| `requiredFields` | nao | Campos form obrigatorios |

### ValidationConfig: External HTTP

```yaml
validation:
  steps:
    - engine: external-http
      url: https://validator.example.com/orders
      method: POST
      requestContentType: application/json
      successStatus: 2xx
      timeoutMs: 2000
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `engine` | sim | `external-http` |
| `url` | sim | Endpoint externo |
| `method` | nao | Metodo; padrao `POST` |
| `requestContentType` | nao | Content-Type enviado |
| `successStatus` | nao | Padrao `2xx` |
| `timeoutMs` | sim | Timeout obrigatorio |

### ValidationConfig: External gRPC

```yaml
validation:
  steps:
    - engine: external-grpc
      endpoint: dns:///validator.default.svc.cluster.local:50051
      service: postio.validation.v1.ValidationService
      method: Validate
      timeoutMs: 2000
```

Campos:

| Campo | Obrigatorio | Descricao |
| --- | --- | --- |
| `engine` | sim | `external-grpc` |
| `endpoint` | sim | Endpoint gRPC |
| `service` | sim | Nome do service |
| `method` | sim | Metodo chamado |
| `timeoutMs` | sim | Timeout obrigatorio |

### CompletionConfig: HTTP Source

```yaml
source:
  type: http
  path: /orders
  completion:
    onSuccess:
      response:
        status: 202
        body:
          ok: true
          messageId: "{{ context.messageId }}"
    onFailure:
      response:
        status: 502
        body:
          ok: false
          error: target_failed
    onValidationFailure:
      response:
        status: 422
        body:
          ok: false
          error: validation_failed
```

### CompletionConfig: SQS Source

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
        queue: postio-invalid-dlq
```

Decisao de ownership:

- `source.completion` pertence ao source, porque finaliza a interacao com a origem original.
- `target.retry` pertence ao target, porque controla as tentativas de entrega para o destino.
- `deadLetter` fica dentro de `source.completion`, porque e uma decisao de finalizacao da mensagem original apos falha ou rejeicao.

## Tipos De Source

### v1

#### `http`

Recebe requests HTTP e retorna uma response ao cliente.

Propriedades planejadas:

| Propriedade | Descricao |
| --- | --- |
| `type` | `http` |
| `method` | Metodo HTTP, inicialmente `POST` |
| `path` | Path exposto pela API |
| `auth` | Futuro: autenticacao por token, JWT, mTLS, etc. |

#### `sqs`

Busca mensagens em uma fila SQS.

Propriedades planejadas:

| Propriedade | Descricao |
| --- | --- |
| `type` | `sqs` |
| `queue` | Nome da fila |
| `queueUrl` | URL da fila |
| `batchSize` | Quantidade de mensagens por poll |
| `waitTimeSeconds` | Long polling |
| `visibilityTimeoutSeconds` | Tempo de invisibilidade enquanto processa |
| `maxConcurrency` | Numero de workers concorrentes |

### Futuro

- `sns` via HTTP subscription ou SQS subscription.
- `s3` event notifications.
- `amqp`.
- `kafka`.
- `webhook`.
- `cron`.

## Tipos De Target

### v1

#### `http`

Envia request HTTP para um endpoint.

Propriedades planejadas:

| Propriedade | Descricao |
| --- | --- |
| `type` | `http` |
| `url` | URL fixa ou definida pelo transform |
| `method` | Metodo HTTP |
| `headers` | Headers fixos ou renderizados |
| `timeoutMs` | Timeout da chamada |
| `retry` | Politica de retry do target |

#### `sqs`

Envia mensagem para SQS.

#### `sns`

Publica mensagem em SNS.

#### `s3`

Grava objeto em S3.

### Futuro

- `amqp`.
- `kafka`.
- `grpc`.
- `filesystem`.

## Transformacao

### Recomendacao Inicial: Rhai

Rhai deve ser a primeira engine de transformacao.

Motivos:

- Feito para scripting embutido em Rust.
- Baixo peso operacional.
- Mais simples de controlar e limitar.
- Bom o suficiente para transformacoes de JSON, headers, atributos e responses.
- Menor risco do que embutir V8/Deno na v1.

### Engine Plugavel

A config deve nascer preparada para multiplas engines:

```yaml
transform:
  engine: rhai
  script: ./transforms/order.rhai
```

Futuro:

```yaml
transform:
  engine: javascript
  runtime: deno
  script: ./transforms/order.ts
```

### Transformacao Externa

Transformacao externa deve comecar com um contrato simples: string in, string out.

Para HTTP:

- Postio converte o payload atual para string.
- Envia essa string como body do request.
- Espera resposta `2xx`.
- O body da resposta e lido como string.
- Essa string vira o novo payload.
- Qualquer status fora de `2xx`, timeout ou erro de rede e erro de pipeline.

```yaml
transform:
  engine: external-http
  url: https://transformer.example.com/orders
  method: POST
  mode: string
  requestContentType: text/plain
  responseContentType: text/plain
  timeoutMs: 3000
```

Contrato HTTP:

```text
POST /orders
content-type: text/plain

<payload como string>
```

Resposta:

```text
200 OK
content-type: text/plain

<payload transformado como string>
```

Para gRPC:

- Postio envia uma string.
- O servico retorna uma string.
- `OK` significa transformacao bem-sucedida.
- Qualquer outro status gRPC, timeout ou erro de rede e erro de pipeline.

```yaml
transform:
  engine: external-grpc
  endpoint: dns:///order-transformer.default.svc.cluster.local:50051
  service: postio.transform.v1.TransformService
  method: Transform
  mode: string
  timeoutMs: 3000
```

Proto conceitual:

```proto
service TransformService {
  rpc Transform(TransformRequest) returns (TransformResponse);
}

message TransformRequest {
  string payload = 1;
}

message TransformResponse {
  string payload = 1;
}
```

Regras:

- `timeoutMs` e obrigatorio.
- O retorno deve respeitar limite de tamanho.
- Tracing deve propagar `traceparent` em HTTP e metadata equivalente em gRPC.
- Transform externo falhando deve seguir a failure policy do pipeline.
- O modo estruturado com JSON canonico pode ser adicionado depois; o baseline e string in/string out.

## Validacao

Validacao deve ser um step de primeira classe, executado depois do decode e antes da transformacao.

```text
source -> decode -> validate -> transform request -> target
```

Motivos:

- falhar cedo quando o payload nao atende o contrato;
- evitar transforms cheios de verificacoes defensivas;
- produzir erro claro para HTTP;
- decidir DLQ/retry de forma previsivel para SQS;
- emitir spans especificos de validacao;
- separar regra de contrato da regra de transformacao.

### Configuracao Simples

```yaml
validate:
  engine: jsonschema
  schema:
    type: object
    required:
      - id
      - total
    properties:
      id:
        type: string
      total:
        type: number
```

### Multiplos Steps De Validacao

```yaml
validation:
  steps:
    - engine: contentType
      allow:
        - application/json

    - engine: jsonschema
      schemaRef: ./schemas/order.schema.json

    - engine: rhai
      script: |
        if input.body.total <= 0 {
          throw "total must be greater than zero";
        }
        true
```

`validate` pode ser um atalho para um unico step. `validation.steps` deve ser o formato mais explicito para multiplas validacoes.

### Engines De Validacao Planejadas

| Engine | Uso |
| --- | --- |
| `contentType` | Validar tipo de entrada esperado |
| `jsonschema` | Validar payload JSON estruturalmente |
| `rhai` | Validar regra customizada simples |
| `regex` | Validar payload texto |
| `multipart` | Validar campos/arquivo em multipart |
| `external-http` | Validar chamando endpoint HTTP externo; sucesso por status |
| `external-grpc` | Validar chamando servico gRPC externo; sucesso por status |

Futuro:

- `openapi`;
- `protobuf`;
- `avro`;
- `cel`.

### Resultado Da Validacao

```rust
pub enum ValidationResult {
    Valid,
    Invalid {
        code: String,
        message: String,
        details: Value,
    },
}
```

### Falha De Validacao

Falha de validacao deve ser tratada como erro permanente por padrao.

Para HTTP:

```yaml
source:
  type: http
  path: /orders
  completion:
    onValidationFailure:
      response:
        status: 422
        body:
          code: validation_failed
```

Para SQS:

```yaml
source:
  type: sqs
  queue: orders-input
  completion:
    onValidationFailure:
      action: deadLetter
      deadLetter:
        queue: postio-invalid-dlq
```

Acoes possiveis:

| Action | Descricao |
| --- | --- |
| `reject` | Retorna erro para source sincrono, como HTTP |
| `deadLetter` | Envia mensagem invalida para DLQ |
| `ack` | Descarta/ack da mensagem invalida depois de logar |
| `retry` | Reprocessa; nao recomendado por padrao para erro de contrato |

Regra recomendada:

> Erro de validacao nao deve chamar o target.

### Validacao Externa

Validacao externa deve ter um contrato simples.

Para HTTP:

- Postio envia o payload para um endpoint HTTP.
- Qualquer status `2xx` significa valido.
- Qualquer status fora de `2xx`, timeout ou erro de rede significa invalido.
- O body da resposta nao e interpretado.

```yaml
validation:
  steps:
    - engine: external-http
      url: https://validator.example.com/orders
      method: POST
      requestContentType: application/json
      successStatus: 2xx
      timeoutMs: 2000
```

Para gRPC:

- Postio chama um metodo gRPC configurado.
- `OK` significa valido.
- Qualquer outro status gRPC, timeout ou erro de rede significa invalido.

```yaml
validation:
  steps:
    - engine: external-grpc
      endpoint: dns:///order-validator.default.svc.cluster.local:50051
      service: postio.validation.v1.ValidationService
      method: Validate
      timeoutMs: 2000
```

Regras:

- `timeoutMs` e obrigatorio.
- O payload enviado deve respeitar limite de tamanho.
- Tracing deve propagar `traceparent` em HTTP e metadata equivalente em gRPC.
- Falha externa de validacao deve seguir `onValidationFailure`.

## Contexto Do Script

O script deve receber um contexto padronizado.

```json
{
  "input": {
    "sourceType": "sqs",
    "body": {},
    "headers": {},
    "attributes": {},
    "metadata": {}
  },
  "target": {
    "status": 200,
    "body": {},
    "headers": {},
    "metadata": {}
  },
  "context": {
    "pipelineId": "sqs-to-http-orders",
    "requestId": "...",
    "timestamp": "...",
    "attempt": 1
  }
}
```

Para `requestTransform`, `target` ainda nao existe.

Para `responseTransform`, `target` contem a resposta do destino.

## Resultado Do Transform

O resultado do `requestTransform` deve ser convertido para um request canonico do target.

Exemplo para HTTP:

```rhai
#{
  method: "POST",
  url: "https://api.example.com/orders",
  headers: #{
    "content-type": "application/json"
  },
  body: input.body
}
```

Exemplo para SQS:

```rhai
#{
  body: input.body,
  attributes: #{
    source: "postio"
  }
}
```

Resultado do `responseTransform`:

```rhai
#{
  ack: true,
  status: 202,
  body: #{
    ok: true
  }
}
```

## Ack, Retry E Resposta

Cada source tem semantica propria.

### HTTP Source

O resultado final vira HTTP response.

```yaml
source:
  type: http
  path: /orders
  completion:
    onSuccess:
      response:
        status: 202
    onFailure:
      response:
        status: 502
    onValidationFailure:
      response:
        status: 422
```

### SQS Source

O resultado final decide delete, retry ou DLQ.

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
        queue: postio-invalid-dlq
```

Regras:

- `ack: true` deleta a mensagem.
- `retry: true` nao deleta e deixa visibility timeout/redrive atuar.
- erro permanente pode enviar para DLQ e deletar a original.

## Observabilidade

Cada pipeline deve emitir spans claros:

| Span | Descricao |
| --- | --- |
| `postio.pipeline.receive` | Recebimento ou polling da fonte |
| `postio.pipeline.decode` | Decode do payload |
| `postio.pipeline.validate` | Validacao do payload antes da transformacao |
| `postio.pipeline.transform.request` | Transformacao antes do target |
| `postio.pipeline.target.send` | Envio ao destino |
| `postio.pipeline.transform.response` | Transformacao da resposta |
| `postio.pipeline.complete` | Ack/retry/response final |

Atributos:

- `pipeline.id`
- `source.type`
- `target.type`
- `request.id`
- `attempt`
- `result.status`
- `target.status`
- `error.kind`

Por seguranca, payload completo nao deve ir para traces por padrao.

### Propagacao De Trace Entre Steps

Como o runtime usa `tokio::sync::mpsc` entre tasks, cada `PipelineMessage` deve carregar o contexto de trace atual.

Sem essa propagacao, spans como `decode`, `validate`, `transform`, `target` e `complete` podem aparecer no Tempo como traces separados, mesmo pertencendo a mesma mensagem. A implementacao deve extrair o contexto no source, armazenar na mensagem e reanexar esse contexto como parent span em cada step.

Requisitos:

- `PipelineMessage.trace` deve preservar `trace_id`, `span_id`/parent e baggage relevante quando existir.
- Source HTTP deve aceitar `traceparent` recebido e criar o contexto inicial quando ausente.
- Source SQS deve aceitar trace context vindo de message attributes quando existir e criar contexto novo quando ausente.
- Cada worker deve criar seu span como filho do contexto carregado na mensagem.
- Ao enviar para targets HTTP, propagar `traceparent`.
- Ao enviar para targets SQS, propagar trace context por message attributes quando possivel.
- Testes de observabilidade devem confirmar que todos os spans de uma mensagem ficam no mesmo trace.

## Step Channel E Mensagem Interna

Os steps do pipeline devem se comunicar por uma mensagem canonica interna.

Conceitualmente:

```text
Source -> PipelineMessage -> Decode -> PipelineMessage -> Validate -> PipelineMessage -> Transform -> PipelineMessage -> Target -> PipelineMessage -> Completion
```

Na primeira implementacao da nova arquitetura, cada etapa interna deve rodar em uma task Tokio separada e se comunicar por channels bounded.

Importante: pensar em tasks Tokio, nao em threads dedicadas. O runtime Tokio decide quais threads executam as tasks.

O source/input fica fora da cadeia de workers principal:

- HTTP handler cria a mensagem, envia para o primeiro channel e aguarda resposta por `oneshot`.
- SQS poller cria a mensagem, envia para o primeiro channel e deixa a etapa de completion decidir ack/retry/DLQ.
- Outros sources futuros seguem a mesma ideia.

### Nome

Evitar chamar o core de `mcp::channel`, a menos que ele implemente ou dependa realmente do protocolo MCP.

Nomes preferidos:

- `pipeline::channel`
- `pipeline::message`
- `pipeline::bus`
- `pipeline::exchange`

### `PipelineMessage`

Estrutura conceitual:

```rust
pub struct PipelineMessage {
    pub id: Uuid,
    pub pipeline_id: String,
    pub source: SourceInfo,
    pub payload: Payload,
    pub metadata: BTreeMap<String, Value>,
    pub trace: TraceContext,
    pub attempt: u32,
    pub reply: Option<oneshot::Sender<CompletionResponse>>,
}
```

`payload` deve representar o dado canonico depois do decode:

```rust
pub enum Payload {
    Json(Value),
    Text(String),
    Binary(Bytes),
    Multipart(MultipartPayload),
    Empty,
}
```

`metadata` deve carregar informacoes transversais:

- headers HTTP;
- attributes SQS/SNS;
- file metadata;
- content type;
- source identifiers;
- timestamps;
- correlation ids.

### `Step`

Cada etapa deve receber uma mensagem e retornar uma decisao:

```rust
#[async_trait]
pub trait Step {
    async fn handle(&self, msg: PipelineMessage) -> Result<StepOutput>;
}

pub enum StepOutput {
    Continue(PipelineMessage),
    Complete(Completion),
    Fail(PipelineError),
}
```

### Canais Reais No Futuro

O desenho alvo usa channels reais desde o inicio da nova arquitetura:

```text
Source task
  -> mpsc
Decode task
  -> mpsc
Validation task
  -> mpsc
Transform task
  -> mpsc
Target task
  -> mpsc
Completion task
```

Usos provaveis:

- `mpsc` para filas internas de pipeline;
- `oneshot` para HTTP aguardar resposta final;
- `broadcast` para fan-out;
- `watch` para reload de configuracao;
- `Semaphore` para controlar concorrencia por target.

### PipelineChannels

Estrutura conceitual:

```rust
pub struct PipelineChannels {
    pub input_tx: mpsc::Sender<PipelineMessage>,
    pub validate_tx: mpsc::Sender<PipelineMessage>,
    pub transform_tx: mpsc::Sender<PipelineMessage>,
    pub target_tx: mpsc::Sender<PipelineMessage>,
    pub completion_tx: mpsc::Sender<PipelineResult>,
}
```

Os channels devem ser bounded para criar backpressure:

```yaml
runtime:
  channelBuffer: 1000
```

### Workers Por Etapa

Na primeira versao:

- uma task Tokio por etapa;
- channels bounded entre etapas;
- source fora do worker chain;
- completion finaliza HTTP via `oneshot` ou SQS via completion policy.

```text
HTTP/SQS Source
  -> input_tx

decode task
  input_rx -> validate_tx

validation task
  validate_rx -> transform_tx

transform task
  transform_rx -> target_tx

target task
  target_rx -> completion_tx

completion task
  completion_rx -> reply/ack/retry
```

Exemplo conceitual:

```rust
async fn run_step_worker<S>(
    mut rx: mpsc::Receiver<PipelineMessage>,
    tx: mpsc::Sender<PipelineMessage>,
    step: Arc<S>,
)
where
    S: Step,
{
    while let Some(msg) = rx.recv().await {
        match step.handle(msg).await {
            Ok(StepOutput::Continue(msg)) => {
                let _ = tx.send(msg).await;
            }
            Ok(StepOutput::Complete(done)) => {
                // send to completion
            }
            Ok(StepOutput::Fail(error)) | Err(error) => {
                // send failure to completion
            }
        }
    }
}
```

### HTTP Reply Com `oneshot`

HTTP precisa aguardar a resposta final do pipeline.

Fluxo:

```text
HTTP handler
  cria PipelineMessage
  cria oneshot channel
  coloca Sender em msg.reply
  envia msg para input_tx
  aguarda Receiver
  converte CompletionResponse em HTTP response
```

### SQS Completion

SQS nao precisa de reply sincrono.

Fluxo:

```text
SQS poller
  cria PipelineMessage sem reply
  envia msg para input_tx

completion task
  sucesso -> DeleteMessage
  erro transiente -> nao deleta, deixa visibility timeout atuar
  erro permanente -> DLQ/ack conforme policy
```

### Concorrencia Por Etapa

Comecar com uma task por etapa.

Depois, adicionar concorrencia configuravel:

```yaml
runtime:
  channelBuffer: 1000
  steps:
    validate:
      concurrency: 4
    transform:
      concurrency: 4
    target:
      concurrency: 8
```

Com `tokio::sync::mpsc`, `Receiver` nao e clonavel. Para concorrencia, opcoes:

- manter um receiver unico e usar `Semaphore` para limitar tasks por mensagem;
- criar dispatcher por etapa que distribui para workers;
- trocar para crate com receiver clonavel, se fizer sentido.

Preferencia inicial:

> Usar `tokio::sync::mpsc`, uma task por etapa, channels bounded e `oneshot` para HTTP. Adicionar concorrencia por etapa depois com `Semaphore` ou dispatcher.

### Ordem De Implementacao Da Espinha Dorsal

1. Criar `PipelineMessage`.
2. Criar `Step` e `StepOutput`.
3. Criar `PipelineChannels`.
4. Criar `PipelineRuntime`.
5. Rodar pipeline com steps mock/noop.
6. Adaptar HTTP source atual para enviar mensagens no channel e aguardar `oneshot`.
7. Plugar validation/transform/target reais em etapas posteriores.

## Runtime Resources E Client Registry

SDK clients e recursos de integracao nao devem ser criados por mensagem.

O runtime deve inicializar recursos compartilhados no startup e injeta-los nos sources, targets e transforms.

### Objetivo

- Evitar criar clients AWS/HTTP a cada request.
- Reaproveitar connection pools.
- Centralizar cache de resolucao de recursos.
- Facilitar testes com providers fake.
- Evitar que steps conhecam detalhes de construcao de clients.

### Modelo Conceitual

```rust
pub struct RuntimeContext {
    pub resources: Arc<ResourceRegistry>,
    pub pipeline: Arc<PipelineRuntime>,
}

pub struct ResourceRegistry {
    pub aws: AwsResourceProvider,
    pub http: HttpResourceProvider,
    pub transforms: TransformRegistry,
}

pub struct AwsResourceProvider {
    pub clients: AwsClients,
    pub cache: AwsResourceCache,
}

#[derive(Clone)]
pub struct AwsClients {
    pub sns: aws_sdk_sns::Client,
    pub sqs: aws_sdk_sqs::Client,
    pub s3: aws_sdk_s3::Client,
}
```

Na maioria dos casos, clients AWS sao baratos de clonar e ja carregam compartilhamento interno. Portanto, a preferencia e:

```rust
#[derive(Clone)]
pub struct SqsTarget {
    resources: Arc<ResourceRegistry>,
    config: SqsTargetConfig,
}
```

E nao:

```rust
Arc<Mutex<aws_sdk_sqs::Client>>
```

`Mutex` ou `RwLock` deve ser usado para estado mutavel nosso, como caches:

```rust
pub struct AwsResourceCache {
    pub topic_arns: tokio::sync::RwLock<BTreeMap<String, String>>,
    pub queue_urls: tokio::sync::RwLock<BTreeMap<String, String>>,
}
```

### Separacao De Responsabilidades

Separar explicitamente:

| Area | Exemplo | Observacao |
| --- | --- | --- |
| Client compartilhado | AWS SDK client, reqwest client | Criado no startup |
| Config do target | queue, topic, bucket, url | Vem do YAML/JSON |
| Cache de resolucao | queueUrl, topicArn, compiled scripts | Compartilhado entre pipelines |
| Estado de execucao | requestId, attempt, payload | Vive na `PipelineMessage` |

### Regra

> Sources, targets e transforms nunca devem criar SDK clients por mensagem. Eles devem usar `Arc<ResourceRegistry>` ou providers derivados dele.

### Exemplo De Uso Em Target

```rust
impl SqsTarget {
    async fn send(&self, msg: PipelineMessage) -> Result<TargetResponse> {
        let queue_url = self
            .resources
            .aws
            .resolve_queue_url(&self.config)
            .await?;

        let response = self
            .resources
            .aws
            .clients
            .sqs
            .send_message()
            .queue_url(queue_url)
            .message_body(msg.payload.to_string())
            .send()
            .await?;

        Ok(TargetResponse::from_sqs(response))
    }
}
```

### Recursos Planejados

```text
RuntimeResources
  AwsClients
    sns
    sqs
    s3
  HttpClient
    reqwest::Client
  TransformRegistry
    compiled Rhai ASTs
  ResourceCache
    topic ARNs
    queue URLs
    maybe endpoint metadata
```

## Arquitetura Interna

Interfaces conceituais:

```rust
trait Source {
    async fn run(&self, pipeline: Pipeline, runtime: RuntimeContext);
}

trait Target {
    async fn send(&self, msg: PipelineMessage) -> TargetResponse;
}

trait TransformEngine {
    fn compile(&self, transform: TransformConfig) -> CompiledTransform;
}

trait CompiledTransform {
    fn apply(&self, ctx: TransformContext) -> TransformResult;
}

trait ValidationEngine {
    fn kind(&self) -> &'static str;
    fn compile(&self, config: ValidationConfig) -> CompiledValidator;
}

trait CompiledValidator {
    fn validate(&self, ctx: ValidationContext) -> ValidationResult;
}

trait Codec {
    fn decode(&self, input: Bytes, content_type: Option<&str>) -> DecodedInput;
    fn encode(&self, output: Value, content_type: Option<&str>) -> Bytes;
}
```

## Modulos Sugeridos

```text
src/
  pipeline/
    config.rs
    model.rs
    message.rs
    channel.rs
    runtime.rs
    context.rs
    completion.rs
    resources.rs
    validation.rs
  sources/
    http.rs
    sqs.rs
  targets/
    http.rs
    sns.rs
    sqs.rs
    s3.rs
  transforms/
    mod.rs
    rhai.rs
  validation/
    mod.rs
    content_type.rs
    external_grpc.rs
    external_http.rs
    jsonschema.rs
    rhai.rs
  codecs/
    json.rs
    text.rs
    multipart.rs
  resources/
    aws.rs
    http.rs
    cache.rs
```

## Extensibilidade

Um objetivo central do design e facilitar a adicao de novos inputs, targets e engines de transformacao.

O runtime de pipeline deve conhecer contratos estaveis. Implementacoes concretas devem ficar em modulos isolados e ser registradas por factories.

```text
core runtime
  conhece traits: SourceFactory, TargetFactory, TransformEngine, ValidationEngine

adapters
  implementam traits: http, sqs, sns, s3, amqp, kafka, rhai, javascript, external-http, external-grpc
```

### Source Plugin Contract

Todo novo source deve implementar um contrato parecido com:

```rust
#[async_trait]
pub trait SourceFactory {
    fn kind(&self) -> &'static str;
    fn build(&self, config: SourceConfig, runtime: RuntimeContext) -> Result<Box<dyn Source>>;
}

#[async_trait]
pub trait Source {
    async fn run(&self, pipeline: Pipeline, runtime: RuntimeContext) -> Result<()>;
}
```

Checklist para novo source:

- Definir schema de config.
- Validar config no startup.
- Produzir `PipelineMessage`.
- Preencher `SourceInfo` e metadata.
- Integrar tracing.
- Definir semantica de completion.
- Criar testes de contrato.
- Documentar propriedades.

Exemplo de source futuro:

```yaml
source:
  type: amqp
  connectionRef: rabbit-main
  queue: orders-input
  prefetch: 25
```

### Target Plugin Contract

Todo novo target deve implementar um contrato parecido com:

```rust
#[async_trait]
pub trait TargetFactory {
    fn kind(&self) -> &'static str;
    fn build(&self, config: TargetConfig, runtime: RuntimeContext) -> Result<Box<dyn Target>>;
}

#[async_trait]
pub trait Target {
    async fn send(&self, msg: PipelineMessage) -> Result<TargetResponse>;
}
```

Checklist para novo target:

- Definir schema de config.
- Validar config no startup.
- Converter `PipelineMessage` ou `TargetRequest` para request externo.
- Retornar `TargetResponse` canonico.
- Reaproveitar clients via `ResourceRegistry`.
- Integrar tracing.
- Criar testes de contrato.
- Documentar propriedades.

### Transform Engine Contract

Engines de transformacao tambem devem ser plugaveis.

```rust
pub trait TransformEngine {
    fn kind(&self) -> &'static str;
    fn compile(&self, config: TransformConfig) -> Result<Box<dyn CompiledTransform>>;
}

pub trait CompiledTransform {
    fn apply(&self, ctx: TransformContext) -> Result<TransformResult>;
}
```

Checklist para nova engine:

- Definir como script e carregado: inline, path, ConfigMap, Secret ou bundle.
- Para engine externa, definir protocolo, endpoint, timeout e formato string in/string out.
- Compilar/cachear no startup quando possivel.
- Definir limites de execucao.
- Definir funcoes utilitarias permitidas.
- Mapear tipos entre engine e `serde_json::Value`.
- Integrar tracing.
- Criar testes de contrato.
- Documentar exemplos.

Exemplo futuro:

```yaml
transform:
  engine: javascript
  runtime: deno
  script: ./transforms/order.ts
```

Exemplo de transform externo:

```yaml
transform:
  engine: external-http
  url: https://transformer.example.com/orders
  mode: string
  timeoutMs: 3000
```

### Validation Engine Contract

Validadores tambem devem ser plugaveis.

```rust
pub trait ValidationEngine {
    fn kind(&self) -> &'static str;
    fn compile(&self, config: ValidationConfig) -> Result<Box<dyn CompiledValidator>>;
}

pub trait CompiledValidator {
    fn validate(&self, ctx: ValidationContext) -> Result<ValidationResult>;
}
```

Checklist para nova engine de validacao:

- Definir schema de config.
- Para engine externa, definir protocolo, endpoint, timeout e criterio de sucesso por status.
- Compilar/cachear schema ou regra no startup quando possivel.
- Retornar `ValidationResult` canonico.
- Garantir erro claro e estruturado.
- Integrar tracing.
- Criar testes de contrato.
- Documentar exemplos.

Exemplo futuro:

```yaml
validation:
  steps:
    - engine: protobuf
      schemaRef: ./schemas/order.proto
      message: OrderCreated
```

Exemplo de validacao externa:

```yaml
validation:
  steps:
    - engine: external-http
      url: https://validator.example.com/orders
      successStatus: 2xx
      timeoutMs: 2000
```

### Registries

O startup deve montar registries de implementacoes disponiveis:

```rust
pub struct AdapterRegistry {
    pub sources: BTreeMap<String, Arc<dyn SourceFactory>>,
    pub targets: BTreeMap<String, Arc<dyn TargetFactory>>,
    pub validators: BTreeMap<String, Arc<dyn ValidationEngine>>,
    pub transforms: BTreeMap<String, Arc<dyn TransformEngine>>,
}
```

Fluxo no startup:

```text
load config
  -> validate pipeline schemas
  -> resolve source factory by type
  -> resolve target factory by type
  -> compile validators by engine
  -> compile transforms by engine
  -> build PipelineRuntime
```

Se uma config usa um tipo desconhecido, o erro deve ser claro:

```text
unknown target type "amqp"; available targets: http, sns, sqs, s3
```

### Regra De Design

Adicionar um novo adapter nao deve exigir alterar o loop principal do pipeline.

Permitido:

- adicionar novo modulo;
- adicionar factory ao registry;
- adicionar config/schema;
- adicionar testes e docs.

Evitar:

- `match` gigante espalhado pelo runtime;
- target criando SDK client por mensagem;
- source retornando tipo especifico de protocolo;
- transform dependendo diretamente de HTTP/SQS/S3.
- validacao implementada como efeito colateral dentro do transform principal.

## Fases De Implementacao

## Corte v0 vs v1

Nem tudo precisa esperar o novo motor de pipelines. Algumas melhorias fortalecem a v0 atual sem mudar o core.

### v0: Fortalecimento Do Bridge Atual

Entram na v0 itens que melhoram a confiabilidade das rotas atuais `HTTP -> SNS/SQS/S3`.

Prioridade recomendada:

1. LocalStack E2E para rotas atuais.
2. `/ready`.
3. Error response padronizado.
4. `apiVersion` / `kind` opcional no config, mantendo compatibilidade.
5. Documentacao e validacao de limites de payload.
6. Redacao de dados sensiveis em logs/traces.

#### Testing Strategy v0

- Testar `HTTP -> SNS`.
- Testar `HTTP -> SQS`.
- Testar `HTTP -> S3` com JSON.
- Testar `HTTP -> S3` com multipart.
- Usar LocalStack para SNS/SQS/S3.
- Usar fila de captura para validar SNS via subscription SNS -> SQS.

Comando desejado:

```bash
docker compose up -d localstack
OTEL_ENABLED=false cargo test --test localstack_e2e -- --ignored
```

#### Readiness v0

Adicionar `/ready` para validar:

- config carregada;
- rotas registradas;
- dispatcher inicializado;
- opcionalmente, AWS SDK config criada.

Nao precisa validar conectividade AWS em toda chamada de readiness.

#### Error Model Simples v0

Padronizar erros atuais:

| Caso | Status |
| --- | --- |
| Body invalido | `400` |
| Config invalida no startup | falha startup |
| Target AWS falhou | `502` ou `500`, decidir |
| Payload acima do limite | `413` |

Formato desejado:

```json
{
  "error": {
    "code": "bad_request",
    "message": "request body must be valid utf-8",
    "requestId": "..."
  }
}
```

#### Config Versioning Leve v0

Permitir, sem obrigar inicialmente:

```yaml
apiVersion: postio.dev/v0
kind: RouteConfig
routes:
  - id: postio-s3-api
    path: /postio/s3
    sink:
      type: s3
      bucket: my-bucket
      key: "{{ context.requestId }}.json"
```

Regras:

- Config sem `apiVersion` continua funcionando.
- `routes[]` continua sendo o contrato v0.
- `pipeline` fica para v1.

#### Payload Limits v0

- Manter `APP_BODY_LIMIT_BYTES`.
- Documentar limite padrao.
- Testar retorno `413`.
- Considerar limite especifico por rota em versao futura.

#### Security Basica v0

- Nao logar payload completo por padrao.
- Nao colocar body completo em traces.
- Redigir headers sensiveis:
  - `authorization`;
  - `cookie`;
  - `set-cookie`;
  - `x-api-key`;
  - `x-amz-security-token`.

### v1: Novo Motor De Pipelines

Entram na v1 itens que dependem do core novo `source -> decode -> validate -> transform -> target -> completion`.

Escopo v1:

- `pipeline`.
- `PipelineMessage`.
- `PipelineChannels`.
- `PipelineRuntime`.
- Sources `http` e `sqs`.
- Targets `http` e `sqs` na primeira entrega.
- Validate noop default.
- Transform noop default.
- Completion policy minima com `source.completion`.

Regra operacional v1:

> Um processo Postio executa uma unica pipeline. Para executar varias pipelines, subir varios deployments/processos Postio, cada um com seu proprio arquivo de configuracao.

Fora da primeira entrega v1:

- Targets `sns` e `s3` no novo motor.
- Transform template.
- Transform Rhai.
- External transform HTTP/gRPC.
- Validation real.
- Retry/backoff avancado.
- DLQ para targets diferentes de SQS.
- Idempotencia.
- Hot reload.
- Metrics por step.

Itens que devem ficar em v1:

| Tema | Motivo |
| --- | --- |
| Error model completo | Depende de source-specific completion |
| Retry/backoff | Depende de pipeline runtime e completion |
| DLQ/failure target | SQS implementado; outros destinos dependem de plugins de target |
| Idempotencia | Depende de `PipelineMessage` e retry |
| Secret references | Necessario para HTTP/external processors |
| Ordering/concurrency | Depende de channels e workers |
| Hot reload | Depende do runtime de pipeline |
| Metrics por step | Depende de steps canonicos |
| Admin API completa | Depende do status do runtime |
| Security avancada | Mais relevante com HTTP/external targets |

#### Error Model Completo v1

- erro transiente;
- erro permanente;
- erro de validacao;
- erro de target;
- erro de transform;
- erro de completion.

#### Retry/Backoff v1

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

#### DLQ / Dead Letter v1

```yaml
source:
  type: sqs
  queue: orders-input
  completion:
    onFailure:
      action: deadLetter
      deadLetter:
        queue: postio-dlq
    onValidationFailure:
      action: deadLetter
      deadLetter:
        queue: postio-invalid-dlq
```

#### Idempotencia v1

```yaml
idempotency:
  key: "{{ body.orderId }}"
  ttlSeconds: 86400
```

#### Secret References v1

```yaml
target:
  type: http
  url: https://api.example.com/orders
  auth:
    type: bearer
    tokenRef:
      name: orders-api
      key: token
```

#### Ordering E Concurrency v1

```yaml
runtime:
  steps:
    target:
      concurrency: 8
ordering:
  key: "{{ body.customerId }}"
```

#### Metrics v1

Metricas planejadas:

- `postio_pipeline_messages_total`
- `postio_pipeline_failures_total`
- `postio_pipeline_duration_seconds`
- `postio_pipeline_step_duration_seconds`
- `postio_target_duration_seconds`
- `postio_sqs_messages_received_total`
- `postio_sqs_messages_deleted_total`

#### Admin API v1

Endpoints planejados:

- `/health`
- `/ready`
- `/pipeline`
- `/pipeline/status`
- `/pipeline/metrics`

#### Security Avancada v1

- allowlist de hosts para target HTTP e external processors;
- SSRF protection;
- TLS customizado;
- mTLS;
- secret refs;
- redacao de logs/traces por policy.

### Fase 1: Runtime De Pipeline Unica Com Noop Steps

- Criar modelo `pipeline`.
- Criar representacoes canonicas de input, target request e target response.
- Criar `PipelineMessage`.
- Criar `Step` e `StepOutput`.
- Criar `PipelineChannels` com `tokio::sync::mpsc` bounded.
- Criar `PipelineRuntime` com uma task Tokio por etapa.
- Criar `RuntimeContext` e `ResourceRegistry`.
- Criar validate noop, sempre valido.
- Criar transform noop, retorna payload original.
- Criar completion via `oneshot` para source HTTP.
- Rodar pipeline inicial com steps mock/noop.
- Converter `routes[]` atual para pipeline interno.
- Manter comportamento v0.

Regra:

> `validate` e `transform` devem ser opcionais. Quando ausentes, o runtime usa `noop`.

Exemplo minimo:

```yaml
pipeline:
  id: http-to-sqs
  source:
    type: http
    method: POST
    path: /events
  target:
    type: sqs
    queue: events
```

### Fase 2: Sources HTTP/SQS

- Adaptar source `http` para enviar `PipelineMessage` ao channel inicial.
- Source HTTP deve aguardar `oneshot` e retornar `CompletionResponse`.
- Implementar source `sqs` com polling.
- Source SQS deve criar `PipelineMessage` sem `reply`.
- Source SQS deve permitir `queue`, `queueUrl`, `batchSize`, `waitTimeSeconds` e `visibilityTimeoutSeconds`.
- Completion deve deletar mensagem SQS em sucesso.
- Em erro, SQS nao deve deletar mensagem na primeira versao.

### Fase 3: Targets HTTP/SQS

- Adicionar target `http`.
- Suportar headers, method, url, body e timeout.
- Observar status, headers e body da resposta.
- Adaptar target `sqs` para o novo contrato de pipeline.
- Reusar `ResourceRegistry` para clients AWS e HTTP.
- Garantir target response canonico para HTTP e SQS.

### Fase 4: LocalStack E2E Matrix HTTP/SQS

Subir ambiente local para validar todos os fluxos de entrada e saida antes de implementar validacao/transformacao reais.

Servicos:

- LocalStack para SQS.
- HTTP mock server para target HTTP.

Comando desejado:

```bash
docker compose up -d localstack mock-http
cargo test --test pipeline_e2e -- --ignored
```

Matriz inicial:

| Source | Target |
| --- | --- |
| HTTP | HTTP |
| HTTP | SQS |
| SQS | HTTP |
| SQS | SQS |

Verificacoes:

- HTTP target: mock recebeu payload esperado.
- SQS target: mensagem chegou na fila destino.
- SQS source: mensagem original foi deletada quando target teve sucesso.
- HTTP source: resposta final voltou ao cliente.

Exemplo HTTP -> SQS:

```yaml
pipeline:
  id: http-to-sqs
  source:
    type: http
    method: POST
    path: /in/http-to-sqs
  target:
    type: sqs
    queueUrl: http://localhost:4566/000000000000/out-queue
```

Exemplo SQS -> HTTP:

```yaml
pipeline:
  id: sqs-to-http
  source:
    type: sqs
    queueUrl: http://localhost:4566/000000000000/in-queue
    batchSize: 1
    waitTimeSeconds: 1
  target:
    type: http
    method: POST
    url: http://localhost:9090/receive
```

Exemplo SQS -> HTTP com `transform.engine: template`:

```yaml
pipeline:
  id: sqs-to-http-template
  source:
    type: sqs
    queueUrl: http://localhost:4566/000000000000/in-queue
    batchSize: 1
    waitTimeSeconds: 1
    visibilityTimeoutSeconds: 30
  transform:
    engine: template
    output:
      headers:
        content-type: application/json
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
    url: http://localhost:9090/receive
    timeoutMs: 5000
```

### Fase 5: Targets SNS/S3 No Pipeline Engine

- Adaptar target `sns` para o novo contrato de pipeline.
- Adaptar target `s3` para o novo contrato de pipeline.
- Expandir matriz LocalStack:
  - HTTP -> SNS
  - HTTP -> S3
  - SQS -> SNS
  - SQS -> S3
- Validar SNS com subscription SNS -> SQS.
- Validar S3 baixando objeto e comparando conteudo.

### Fase 6: Transform Template

- Implementar engine `template`.
- Permitir montar JSON dinamico em `transform.output`.
- Suportar sintaxe `{{ ... }}`.
- Acessar `params`, `query`, `headers`, `attributes`, `body`, `form`, `file`, `context`.
- Garantir merge entre `target` default e `transform.output`.
- `transform.output.query` monta query string dinamica para targets HTTP.
- `transform.output.attributes` monta atributos dinamicos para targets SQS.

Exemplo:

```yaml
transform:
  engine: template
  output:
    body:
      name: "{{ params.name }}"
      payload: "{{ body }}"
```

### Fase 7: Transform Rhai

- Adicionar engine Rhai.
- Compilar scripts no startup.
- Cachear AST.
- Definir funcoes utilitarias seguras.
- Adicionar timeouts/limites quando possivel.

### Fase 8: External Transform HTTP

- Implementar `external-http`.
- Contrato string in/string out.
- `2xx` significa sucesso.
- Body da resposta vira payload transformado.
- `timeoutMs` obrigatorio.

### Fase 9: External Transform gRPC

- Implementar `external-grpc`.
- Contrato string in/string out.
- gRPC `OK` significa sucesso.
- Response payload vira payload transformado.
- `timeoutMs` obrigatorio.

### Fase 10: Validation Step

- Adicionar etapa oficial `validate` / `validation.steps`.
- Implementar engine `contentType`.
- Implementar engine `jsonschema`.
- Planejar engine `rhai` para regras customizadas.
- Definir `ValidationResult`.
- Definir `onValidationFailure`.
- Garantir que target nao seja chamado quando validacao falhar.
- Adicionar span `postio.pipeline.validate`.

Status da primeira fatia:

- `[x]` `validate.engine: jsonschema` com `schema` inline.
- `[x]` Fallback noop quando `validate` nao existe.
- `[x]` HTTP source retorna `422` e `status: rejected` quando a validacao falha, salvo override em `source.completion`.
- `[x]` SQS source nao deleta a mensagem quando a validacao falha, salvo override em `source.completion`.
- `[x]` Target nao e chamado quando a validacao falha.
- `[x]` `source.completion.onValidationFailure` para HTTP e SQS.
- `[ ]` `schemaRef`.
- `[ ]` `validation.steps`.
- `[ ]` Engines `contentType`, `rhai`, `http` e `grpc`.

### Fase 11: Response Transform E Completion

- Permitir manipular resposta do target.
- Decidir HTTP response ou SQS ack/retry.
- Padronizar erros.

Status:

- `[x]` HTTP `source.completion` customiza status/body de sucesso, falha de target e falha de validacao.
- `[x]` SQS `source.completion` suporta `ack`, `retry`, `drop` e `deadLetter`.
- `[x]` `deadLetter` SQS envia para DLQ e so depois deleta a mensagem original.
- `[ ]` `responseTransform` dedicado para manipular resposta do target antes do completion.

### Fase 12: Hardening

- Validacao forte de config.
- Testes de contrato por source/target.
- Observabilidade completa.
- Protecao contra scripts caros.
- Documentacao completa.

## Decisoes Em Aberto

- Nome final da nova secao: `pipelines`, `flows` ou `bridges`.
- Rhai inline versus arquivo externo no config.
- Como empacotar scripts em Kubernetes: ConfigMap, Secret ou imagem.
- Como lidar com secrets em transforms.
- Qual limite maximo de payload por source.
- Como versionar config v0/v1.
- Se target HTTP deve permitir TLS customizado/mTLS na primeira versao.
- Como tratar resposta binaria do target HTTP.

## Recomendacao Atual

Seguir com Rhai na primeira versao de pipelines e manter a arquitetura preparada para engines futuras.

Deno/JavaScript deve ser considerado depois, quando houver necessidade real de TypeScript, bibliotecas externas ou plugin runtime mais poderoso.
