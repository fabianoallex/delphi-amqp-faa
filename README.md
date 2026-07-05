# delphi-amqp-faa

Cliente **AMQP 0-9-1** para Delphi, escrito do zero a partir da especificação
pública do protocolo — sem dependências externas (usa só a RTL) e com licença
**MIT** desde o primeiro commit.

Testado ponta a ponta contra um **RabbitMQ** real: **71 testes unitários** (sem
broker) + **12 de integração**.

## Recursos

- Handshake de conexão completo (`AMQP0091`, PLAIN, negociação de tune).
- Canais, `exchange.declare/delete`, `queue.declare/bind/purge/delete`.
- **Publish** (com propriedades da mensagem) e **Basic.Get** (pull).
- **Basic.Consume** (push) com despacho de cada mensagem para um **thread pool
  nativo** (`TTask`) — mensagens diferentes são processadas **em paralelo**,
  nunca serializadas.
- **Ack/Nack manual** (at-least-once), com `Qos`/prefetch.
- **Heartbeat** (thread dedicada, espera interrompível — não usa `TTimer`) e
  detecção de conexão morta.
- **Reconexão automática** com recuperação de topologia (redeclara filas/
  exchanges/bindings, restaura Qos e re-consome).
- Sem VCL: funciona em console, serviço Windows, etc. (`System.Net.Socket`).

## Requisitos

- Delphi (RAD Studio) 12 (Win32/Win64). Deve funcionar em versões próximas.
- Um broker AMQP 0-9-1 (RabbitMQ). Para desenvolvimento:
  `docker compose -f docker/docker-compose.yml up -d` (guest/guest, portas 5672
  e 15672).

## Instalação

Adicione a pasta `src/` ao *search path* do seu projeto (ou inclua as units no
`.dpr`). As units principais para uso:

```pascal
uses
  AMQP.Connection,        // TAMQPConnection, TAMQPChannel, TAMQPDelivery
  AMQP.Exchange.Methods,  // TAMQPExchangeDeclare, AMQP_EXCHANGE_TYPE_*
  AMQP.Queue.Methods,     // TAMQPQueueDeclare, TAMQPQueueBind
  AMQP.Basic.Methods;     // TAMQPBasicProperties
```

## Uso

### Conectar

```pascal
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
  LChannel: TAMQPChannel;
begin
  LParams := TAMQPConnectionParams.Localhost; // localhost:5672, '/', guest/guest
  // LParams.Host := '192.168.0.10';
  // LParams.User := 'app'; LParams.Password := 'segredo';

  LConn := TAMQPConnection.Create(LParams);
  try
    LConn.Open;                     // conecta + handshake; inicia thread de leitura
    LChannel := LConn.CreateChannel; // canal já aberto
    // ... use o canal ...
  finally
    LConn.Free; // fecha canais e conexão
  end;
end;
```

> O `TAMQPConnection` é dono das threads internas (leitura + heartbeat). Libere
> os canais antes da conexão (ou deixe o `Free` da conexão cuidar deles).

### Declarar fila / exchange / binding

```pascal
// Fila durável nomeada:
LChannel.DeclareQueue(TAMQPQueueDeclare.Create('nfe.respostas', True));

// Exchange topic + binding:
var LBind: TAMQPQueueBind;
LChannel.DeclareExchange(TAMQPExchangeDeclare.Create('nfe', AMQP_EXCHANGE_TYPE_TOPIC));
LBind := Default(TAMQPQueueBind);
LBind.QueueName := 'nfe.respostas';
LBind.ExchangeName := 'nfe';
LBind.RoutingKey := 'resposta.#';
LChannel.BindQueue(LBind);
```

### Publicar

```pascal
// Texto simples (content-type text/plain, persistente):
LChannel.PublishText('', 'nfe.respostas', 'olá mundo');

// Com propriedades (JSON persistente, correlacionado):
var LProps: TAMQPBasicProperties;
LProps := TAMQPBasicProperties.Empty;
LProps.SetContentType('application/json');
LProps.SetPersistent;                       // delivery-mode 2
LProps.SetCorrelationId('NFe3524...9012');
LProps.SetMessageId('req-42');
LChannel.Publish('', 'nfe.respostas',
  TEncoding.UTF8.GetBytes('{"status":"autorizada"}'), LProps);
```

O exchange vazio (`''`) roteia pela *routing key* = nome da fila (default
exchange). Publicar é fire-and-forget (sem publisher confirms).

### Consumir uma mensagem (pull)

```pascal
var LMsg: TAMQPGetResult;
LMsg := LChannel.BasicGet('nfe.respostas', True {no-ack});
if LMsg.Found then
  Writeln(LMsg.BodyAsText);
```

### Consumir continuamente (push) — concorrente, com ack manual

```pascal
LChannel.Qos(20); // prefetch: até 20 mensagens não confirmadas por vez

LChannel.Consume('nfe.respostas',
  procedure(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery)
  begin
    // Roda numa thread do pool: mensagens diferentes são processadas em
    // paralelo. Não bloqueia a thread de leitura.
    try
      ProcessarResposta(ADelivery.Properties.CorrelationId, ADelivery.BodyAsText);
      AChannel.Ack(ADelivery.DeliveryTag);        // confirma (removeu da fila)
    except
      AChannel.Nack(ADelivery.DeliveryTag, True); // requeue em caso de erro
    end;
  end);
```

`Consume` devolve o *consumer-tag* (use em `Cancel`). Com `ANoAck=False`
(padrão), confirme cada mensagem com `Ack` (ou `Nack`).

### Reconexão automática

```pascal
LParams := TAMQPConnectionParams.Localhost;
LParams.AutoReconnect := True;
LParams.ReconnectDelayMs := 2000;   // backoff entre tentativas
LParams.MaxReconnectAttempts := 0;  // 0 = infinitas

LConn := TAMQPConnection.Create(LParams);
LConn.OnReconnect :=
  procedure(AConnection: TAMQPConnection)
  begin
    // disparado após reconectar E restaurar a topologia
    Log('reconectado e consumindo de novo');
  end;
LConn.Open;
```

Na queda, a lib reconecta e **restaura a topologia** declarada naquele canal
(filas, exchanges, bindings, Qos) e **re-registra os consumers** — o seu
callback volta a receber mensagens sem intervenção. Como *delivery-tags*
reiniciam a cada sessão, mensagens não confirmadas são reentregues: projete os
handlers para serem **idempotentes** (at-least-once).

## Caso de uso alvo (PDV → autorizador → retaguarda)

O fluxo que motivou a lib: o autorizador publica a resposta da NFe numa fila; a
**retaguarda** consome essa fila e responde ao polling de **vários PDVs
simultâneos**. O consumo com thread pool + ack manual atende isso diretamente —
cada resposta é processada em paralelo, correlacionada pela chave da NFe
(`CorrelationId` ou um header), e só é confirmada após o processamento.

## Arquitetura (resumo)

- **Uma thread de leitura** é a única que lê o socket após o handshake; ela
  demultiplexa frames por canal e **despacha callbacks de consumer para o thread
  pool** — nunca roda código do usuário nem bloqueia.
- Todas as **escritas** são serializadas por um lock; os frames de uma mensagem
  (método + header + corpo) saem juntos.
- **RPC** (declare/bind/get/consume/close) é feito por evento: envia e aguarda a
  thread de leitura entregar a resposta.
- **Heartbeat** e **reconexão** rodam em threads próprias com espera
  interrompível (`TEvent`).

## Testes

Abra `AMQP.groupproj` no RAD Studio.

- **`AMQP.UnitTests`** — não precisa de broker (encode/decode de frames,
  métodos, content header, negociação de tune).
- **`AMQP.IntegrationTests`** — precisa do RabbitMQ no ar:

  ```
  docker compose -f docker/docker-compose.yml up -d
  ```

## Limitações conhecidas

- Publisher confirms e transações ainda não implementados (publish é
  fire-and-forget).
- A recuperação de topologia na reconexão assume filas **nomeadas**; filas com
  nome gerado pelo servidor recebem um novo nome ao serem redeclaradas.
- `Basic.Return` (mensagem `mandatory` não roteável) é descartado por ora.

## Integração com delphi-api-infra-faa

`src/Messaging.Adapters.DelphiAmqpFaa.pas` implementa o contrato de
mensageria agnóstico de `delphi-api-infra-faa` (`Messaging.Interfaces`:
`IMessagingFactory`, `IMessageConsumer`, `IMessagePublisher`,
`IMessagePayload`). Fica fora do núcleo da lib — quem só quer o cliente AMQP
não precisa da infra no *search path*; quem quiser o padrão de registry da
infra inclui essa unit, que se registra sozinha (nome `'rabbitmq'`) na
`initialization`.

Requer, no projeto consumidor, os dois *search paths*:
- a pasta `src` desta lib;
- `src/Messaging` de `delphi-api-infra-faa` (unit `Messaging.Interfaces`).

```pascal
uses
  Messaging.Adapters.Registry,
  Messaging.Adapters.DelphiAmqpFaa; // basta estar no uses para se registrar

LFactory  := TMessagingRegistry.GetFactory('rabbitmq');
LConsumer := LFactory.CreateConsumer(LConfig);
LConsumer.Subscribe('nfe.respostas', TMeuHandler.Create(LService));
LConsumer.Start;
```

Mapeamento de ack: o adapter confirma (`Ack`) após `IMessageHandler.Handle`
retornar sem erro, e devolve à fila (`Nack` com requeue) se o handler
levantar exceção — at-least-once por padrão, com prefetch 20.

## Roadmap

Itens 1–4 concluídos (framing/handshake, canais + publish/get, consumo
concorrente + ack, heartbeat + reconexão), mais o adapter para
`delphi-api-infra-faa`. Ver [CLAUDE.md](CLAUDE.md).

## Licença

MIT — ver [LICENSE](LICENSE). Copyright (c) 2026 Fabiano Arndt.
