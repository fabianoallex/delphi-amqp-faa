# delphi-amqp-faa

Cliente **AMQP 0-9-1** para Delphi, escrito do zero a partir da especificação
pública do protocolo — sem dependências externas (usa só a RTL) e com licença
**MIT** desde o primeiro commit.

Testado ponta a ponta contra um **RabbitMQ** real: **75 testes unitários** (sem
broker) + **18 de integração**.

## Recursos

- Handshake de conexão completo (`AMQP0091`, PLAIN, negociação de tune).
- Canais, `exchange.declare/delete`, `queue.declare/bind/unbind/purge/delete`.
- **Publish** (com propriedades da mensagem) e **Basic.Get** (pull).
- **Basic.Consume** (push) com despacho de cada mensagem para um **thread pool
  nativo** (`TTask`) — mensagens diferentes são processadas **em paralelo**,
  nunca serializadas.
- **Ack/Nack manual** (at-least-once), com `Qos`/prefetch.
- **Heartbeat** (thread dedicada, espera interrompível — não usa `TTimer`) e
  detecção de conexão morta.
- **Reconexão automática** com recuperação de topologia (redeclara filas/
  exchanges/bindings, restaura Qos e re-consome).
- **`Basic.Return`**: publish `mandatory` não roteável dispara `OnBasicReturn`
  (thread pool), em vez de ser descartado silenciosamente.
- **Publisher confirms** (`confirm.select`): cada publish recebe um seq-no e é
  confirmado pelo broker — via `OnConfirm` (assíncrono) e/ou
  `WaitForConfirm`/`WaitForConfirms` (síncrono).
- **TLS (amqps://)** via **SChannel nativo do Windows** (SSPI) — sem OpenSSL nem
  DLLs externas; validação de certificado pela cadeia de confiança do Windows.
- **`Connection.Blocked`/`Unblocked`**: quando o broker entra em *resource alarm*
  (memória/disco) e para de aceitar publishes, dispara `OnBlocked`/`OnUnblocked`
  (thread pool) — dá para pausar o publish até o broker liberar.
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

### Conectar com TLS (amqps://)

TLS usa o **SChannel nativo do Windows** (via SSPI) — sem OpenSSL, sem DLLs
externas. Basta ligar `UseTls` (porta padrão do broker: 5671):

```pascal
LParams := TAMQPConnectionParams.Localhost;
LParams.Host := 'broker.example.com';
LParams.Port := 5671;
LParams.UseTls := True;          // handshake TLS antes do AMQP
// LParams.TlsVerifyPeer := True; // padrão: valida cadeia + hostname (Windows)
// LParams.TlsServerName := '';   // '' => usa Host (SNI / nome p/ validação)
```

Por padrão o certificado do servidor é validado pela cadeia de confiança do
Windows (inclui checagem de hostname). Para um broker de **desenvolvimento** com
certificado *self-signed*, desligue a validação — ou use o atalho `LocalhostTls`
(porta 5671, `TlsVerifyPeer=False`):

```pascal
LParams := TAMQPConnectionParams.LocalhostTls; // TLS em 5671, validação off (dev)
```

> Só Windows (SChannel). mTLS/client-cert não estão nesta versão.

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
exchange). Sem confirm mode, publicar é fire-and-forget e `Publish` retorna 0.

### Publish com confirmação (publisher confirms)

`ConfirmSelect` coloca o canal em modo confirm: a partir daí cada `Publish`
recebe um *seq-no* (1, 2, 3, ...) e o broker o confirma (`ack`) ou rejeita
(`nack`). Dá para tratar de forma assíncrona com `OnConfirm` e/ou bloquear até a
confirmação com `WaitForConfirm` (um publish) ou `WaitForConfirms` (todos os
pendentes):

```pascal
LChannel.ConfirmSelect;

// Assíncrono: notificado numa thread do pool quando cada publish é confirmado.
LChannel.OnConfirm :=
  procedure(AChannel: TAMQPChannel; ASeqNo: UInt64; AAck: Boolean)
  begin
    if not AAck then
      Log(Format('publish %d foi NACK-ado pelo broker', [ASeqNo]));
  end;

// Síncrono: bloqueia até o broker confirmar este publish.
var LSeq: UInt64;
LSeq := LChannel.Publish('', 'nfe.respostas', LBody, LProps);
if LChannel.WaitForConfirm(LSeq, 5000) then
  Writeln('confirmado')
else
  Writeln('não confirmado (nack, queda de conexão ou timeout)');

// Ou publique em lote e espere todos de uma vez:
LChannel.Publish('', 'nfe.respostas', LBody1, LProps);
LChannel.Publish('', 'nfe.respostas', LBody2, LProps);
if not LChannel.WaitForConfirms(5000) then
  Writeln('algum publish não foi confirmado');
```

Se a conexão cair antes da confirmação, o publish pendente é reportado como
**não confirmado** (`WaitForConfirm` retorna `False`); `OnConfirm` dispara apenas
para confirmações reais do broker. Após uma reconexão a numeração de seq-no
reinicia (canal novo) — ver *Limitações conhecidas*.

**Reenvio automático (opt-in)**: com `RepublishUnconfirmedOnReconnect := True` nos
parâmetros da conexão (junto de `AutoReconnect`), os publishes que ficaram sem
confirmação numa queda são **re-publicados automaticamente na reconexão**, com
seq-nos novos (observáveis via `OnConfirm`). É *at-least-once* — pode haver
duplicatas quando o broker recebeu a mensagem mas o `ack` se perdeu na queda; os
seq-nos originais seguem reportando "não confirmado". Custo: guarda o corpo de
cada publish pendente até a confirmação.

Para saber se um publish `mandatory` não foi roteado a nenhuma fila, trate
`OnBasicReturn` (dispara numa thread do pool, como o callback de consumer):

```pascal
LChannel.OnBasicReturn :=
  procedure(AChannel: TAMQPChannel; const AReturned: TAMQPReturnedMessage)
  begin
    Log(Format('mensagem não roteada: %s (%d) exchange=%s rk=%s',
      [AReturned.ReplyText, AReturned.ReplyCode, AReturned.Exchange, AReturned.RoutingKey]));
  end;

LChannel.Publish('nfe', 'resposta.inexistente', LBody, LProps, True {mandatory});
```

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

Exemplo executável em `samples/AutorizadorSim` (publica N retornos simulados)
e `samples/Retaguarda` (consome e processa concorrentemente, com um comando
de status no console). Suba o broker (`docker compose -f
docker/docker-compose.yml up -d`), rode o `Retaguarda` e, em seguida, o
`AutorizadorSim` — as linhas `[worker N] iniciando...` de notas diferentes
aparecem intercaladas no console do `Retaguarda`, confirmando o processamento
em paralelo.

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

- Transações (`tx.*`) **não implementadas — por decisão de design**, não por
  dificuldade técnica (o protocolo `tx.*` é trivial). Uma transação AMQP é
  *stateful e por canal* ("tudo que publiquei/dei ack desde o último commit"),
  o que casa mal com o modelo *concurrency-first* desta lib: várias threads
  publicando no mesmo canal cairiam todas na mesma transação, sem escopo
  por-thread. Além disso, `tx.*` é síncrono e lento, e a própria RabbitMQ
  recomenda **publisher confirms** no lugar — que já estão implementados
  (`ConfirmSelect`) e cobrem a garantia "meu publish chegou?". Se surgir
  necessidade real de lote atômico, o subconjunto tratável é
  `Tx.Select/Commit/Rollback` para uso serial em canal dedicado.
- Publisher confirms + reconexão: ao reconectar, o modo confirm é re-armado e os
  seq-nos devolvidos por `Publish` seguem **monotônicos** (não reiniciam). Os
  publishes não confirmados antes da queda são reportados como **não confirmados**
  (`WaitForConfirm`/`WaitForConfirms` retornam `False`), porém **não reenviados
  automaticamente** — reenvie na sua camada se precisar de garantia ponta a ponta.
- A recuperação de topologia na reconexão assume filas **nomeadas**; filas com
  nome gerado pelo servidor recebem um novo nome ao serem redeclaradas.
- TLS é **só Windows** (SChannel). mTLS/client-cert, escolha manual de versão/
  cipher suite e exposição de TLS no adapter de `delphi-api-infra-faa` ainda não
  estão implementados.

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

**TLS**: como `TMessagingConfig` é agnóstico (não tem campos de TLS), o adapter
liga o TLS por convenção de porta — `Port = 5671` (amqps) ativa `UseTls` com
validação de certificado (`TlsVerifyPeer=True`). Cert self-signed/dev ou
`verify-off` exigem estender a config no infra.

**Escape hatch** (recursos que não cabem no contrato genérico — publisher
confirms, `Connection.Blocked/Unblocked`, `Queue.Unbind`): o consumer e o
publisher também expõem `IDelphiAmqpFaaNative`, obtido via `Supports`, para
descer ao objeto nativo desta lib de forma explícita e opcional:

```pascal
var LNative: IDelphiAmqpFaaNative;
if Supports(LPublisher, IDelphiAmqpFaaNative, LNative) then
begin
  LNative.NativeChannel.ConfirmSelect;
  LNative.NativeConnection.OnBlocked :=
    procedure(AConn: TAMQPConnection; const AReason: string)
    begin { pausa o publish enquanto o broker está em resource alarm } end;
end;
```

Os objetos nativos são propriedade do adapter (não liberar); no consumer só
ficam válidos após `Start`. Em confirm mode, publique pelo `NativeChannel` para
receber o seq-no (o `Publish` genérico da interface o descarta).

## Roadmap

Itens 1–4 concluídos (framing/handshake, canais + publish/get, consumo
concorrente + ack, heartbeat + reconexão), mais o adapter para
`delphi-api-infra-faa`. Ver [CLAUDE.md](CLAUDE.md).

## Licença

MIT — ver [LICENSE](LICENSE). Copyright (c) 2026 Fabiano Arndt.
