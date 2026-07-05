# delphi-amqp-faa

Cliente AMQP 0-9-1 para Delphi, implementado a partir da especificação pública do protocolo.

Status: **em desenvolvimento inicial** (WIP). Ainda não há release nem API estável.

## Uso pretendido

Biblioteca de uso geral, mas o primeiro caso de uso real e critério de aceite é uma integração PDV → autorizador → retaguarda (emissão de NFe), com consumo concorrente de mensagens via thread pool nativo.

## Estrutura

- `src/` — código da biblioteca.
  - `AMQP.Protocol.pas` — constantes do protocolo (header, tipos de frame, IDs de classe/método).
  - `AMQP.Wire.pas` — codec big-endian dos tipos primitivos (octet, short, long, longlong, shortstr, longstr, bits, timestamp, field-table).
  - `AMQP.Frame.pas` — leitura/escrita de frames + cabeçalho de protocolo.
  - `AMQP.Method.pas` — cabeçalho de método (class-id/method-id).
  - `AMQP.Connection.Methods.pas` — métodos `Connection.*` do handshake + negociação de tune.
  - `AMQP.Connection.pas` — conexão sobre socket TCP e handshake completo.
- `tests/Unit/` — testes DUnitX sem broker (runner console, sem VCL).
- `tests/Integration/` — testes DUnitX que exigem um RabbitMQ real.
- `docker/` — RabbitMQ real para testes de integração.
- `AMQP.groupproj` — group project com os projetos de teste.

## Build e testes

Abra `AMQP.groupproj` no RAD Studio (Delphi 12).

- **Unitários** (`AMQP.UnitTests`): aplicação console, não precisam de broker.
- **Integração** (`AMQP.IntegrationTests`): exigem o RabbitMQ no ar. Suba antes com:

  ```
  docker compose -f docker/docker-compose.yml up -d
  ```

## Roadmap

Ver [CLAUDE.md](CLAUDE.md). Progresso atual: **item 1 concluído** — framing
binário, codec de tipos e handshake de conexão (validável contra o RabbitMQ do
`docker/docker-compose.yml`). A seguir, item 2: canais, `exchange.*`/`queue.*`
e `Basic.Publish`.

## Licença

MIT — ver [LICENSE](LICENSE).
