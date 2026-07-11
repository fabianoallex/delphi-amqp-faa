# delphi-amqp-faa

Cliente **AMQP 0-9-1** para Delphi, escrito do zero a partir da especificação
pública do protocolo — sem dependências externas (usa só a RTL) e com licença
**MIT** desde o primeiro commit.

Testado ponta a ponta contra um **RabbitMQ** real: **80 testes unitários** (sem
broker) + **24 de integração**.

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

> **O callback precisa sempre terminar sozinho.** `Close`/`Destroy` do canal
> esperam (sem timeout, de propósito) os callbacks em voo terminarem antes de
> liberar o objeto — IO demorado é ok, mas um callback que bloqueia
> indefinidamente esperando interação do usuário ou um evento que só outra
> thread da aplicação sinaliza trava esse fechamento; se o `Free` roda na
> thread principal de uma app VCL, a UI congela junto (deadlock). Se o fluxo
> depende de aprovação humana, prefira **não bloquear**: guarde o
> *delivery-tag* e o conteúdo numa estrutura própria, retorne, e confirme
> depois (`Ack`/`Nack` podem ser chamados de qualquer thread). Se optar por
> bloquear num `TEvent`, o encerramento precisa acordar **todas** as esperas e
> também cobrir entregas que cheguem *durante* a desconexão — um nack+requeue
> pode ser reentregue imediatamente ao mesmo consumer até o `Cancel`
> completar (`samples/RetaguardaVcl` mostra o padrão com flag de
> encerramento).

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

## Concorrência e ordenação de mensagens

Cada entrega é despachada pro thread pool (`TTask`) como um item de trabalho independente — não existe uma fila única por canal/consumer sendo drenada em ordem. É uma escolha de design deliberada, diferente do padrão comum em outras linguagens:

- **RabbitMQ Java/.NET client**: cada canal é processado sequencialmente por um único worker tirado de um pool compartilhado — canais diferentes rodam em paralelo entre si, mas dentro de um canal a ordem é preservada por padrão.
- **pika (Python) / node-amqplib**: single-threaded, orientado a event loop — todos os callbacks rodam na mesma thread/loop; paralelismo é opt-in, por conta do código do usuário.
- **amqp091-go**: expõe um channel de Go com as entregas; quem consome decide se processa serial ou dispara goroutines.

Aqui o padrão é o oposto: **paralelismo por padrão, ordem por opt-in** (é o que motivou a lib — ver *Caso de uso alvo* acima). O ganho real de throughput aparece quando o callback faz algo bloqueante (I/O de rede, banco, disco) — várias mensagens processam ao mesmo tempo em vez de uma esperar a outra terminar. Se o callback for leve/CPU-bound, o ganho é marginal (o hand-off pro pool custa mais que processar inline). Em qualquer um dos dois casos, o efeito colateral não muda: **duas entregas do mesmo consumer podem terminar de processar fora de ordem.**

### Como obter ordem quando ela importa

**1. `Qos(1)` + ack só ao final do processamento** — a opção mais simples, usa só API pública já existente. O broker não entrega a próxima mensagem daquele consumer enquanto a anterior não for confirmada:

```pascal
LChannel.Qos(1);
LChannel.Consume('nfe.respostas',
  procedure(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery)
  begin
    ProcessarResposta(ADelivery.BodyAsText);
    AChannel.Ack(ADelivery.DeliveryTag); // só confirma ao final
  end);
```

Serializa **tudo** daquele consumer — sem paralelismo algum. Boa escolha quando ordem estrita é obrigatória e o volume não é o gargalo.

**2. Fila própria da aplicação + uma thread dedicada** — o callback só empilha a entrega numa fila thread-safe (ex. `TCriticalSection` + `TQueue`); uma única thread dedicada drena e processa em ordem de chegada, chamando `Ack` no fim de cada item. Preserva ordem real de chegada e deixa o `Qos`/prefetch livre pra continuar recebendo em paralelo do broker enquanto sua fila processa. É a mesma ideia do padrão "capture o delivery-tag e confirme depois" já descrito acima para fluxos de aprovação humana — aqui a diferença é que quem drena é uma única thread, não N.

**3. Sharding por chave** — variação da opção 2 com N filas de uma thread cada, escolhida por hash de alguma chave de domínio (ex. `CorrelationId`, id do PDV). Ordem garantida *por chave*, paralelismo entre chaves diferentes — bom meio-termo quando só importa "ordem por entidade", não ordem global.

As opções 2 e 3 são implementadas inteiramente na aplicação, com a API pública já existente (`Consume`/`Ack`/`Nack`/`Qos`/`TAMQPDelivery`) — não exigem nenhuma mudança na lib. Duas pegadinhas a observar:

- **`ADelivery.Properties.Headers` é liberado pela lib assim que a callback retorna** (o dono é o chamador, mas o pool libera automaticamente depois que o callback volta). Se o callback só empilha a entrega e retorna na hora, `Headers` já estará inválido quando a thread dedicada for processar depois — extraia o que precisar dos headers **antes** de retornar do callback; não guarde a referência ao `TAMQPFieldTable` pra usar depois.
- **`Channel.Close`/`Free` não sabe da sua fila própria.** Como o callback retorna assim que empilha, o contador interno de "em voo" da lib já dá aquela mensagem como concluída. Ao fechar a aplicação, é responsabilidade de quem implementou a fila própria esperá-la esvaziar antes de fechar o canal — senão mensagens já retiradas da lib mas ainda não processadas/ackadas podem se perder.

Por que começar do lado paralelo em vez do serializado: adicionar ordem sobre uma lib *concurrency-first* é aditivo e contido (as opções acima). O caminho inverso — adicionar paralelismo numa lib que serializa por padrão — normalmente exige reconstruir na aplicação o que aqui já vem pronto: garantir que operações no canal são seguras por múltiplas threads (bibliotecas serializadas por padrão costumam não garantir isso, forçando um canal por worker), backpressure própria e drenagem segura no encerramento.

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
  (`WaitForConfirm`/`WaitForConfirms` retornam `False`). O reenvio na reconexão é
  **opt-in** (`RepublishUnconfirmedOnReconnect`, at-least-once); sem ele, reenvie
  na sua camada se precisar de garantia ponta a ponta.
- **Recuperação de topologia com filas de nome gerado pelo servidor** — ver a
  seção dedicada abaixo.
- TLS é **só Windows** (SChannel). O adapter liga TLS por convenção de porta
  (`5671`); ainda **não** implementados: mTLS/client-cert, escolha manual de
  versão/cipher suite, e campos de TLS na config agnóstica do infra (para
  `verify-off`/dev e `server-name` custom via adapter).

### Recuperação de filas com nome gerado pelo servidor

Ao declarar `QueueName = ''`, o servidor **gera** o nome (`amq.gen-XXXX`). A
recuperação de topologia na reconexão **assume filas nomeadas**: ela guarda os
payloads de `declare`/`bind`/`consume` já serializados e os re-executa. Como o
redeclare com nome vazio gera um nome **novo** e os `bind`/`consume` gravados
carregam o nome **antigo**, o replay quebra (`basic.consume` na fila inexistente
→ `404` → o servidor fecha o canal).

**Workaround (recomendado):** se você precisa de fila temporária **e** de
reconexão, **gere o nome no cliente** em vez de usar `''`:

```pascal
LDecl.QueueName := 'reply-' + TGUID.NewGuid.ToString;
LDecl.Exclusive := True;   // some quando a conexão fecha
LDecl.AutoDelete := True;
```

Assim a fila é temporária com nome **estável e conhecido**, e a recuperação
funciona como a de qualquer fila nomeada. Para RPC request/reply, prefira o
**Direct Reply-to** (`amq.rabbitmq.reply-to`) — um nome-mágico fixo, sem declarar
fila por requisição, que também não sofre desse problema.

**Caminho das pedras (para quem for forkar e precisar de server-named real):**

1. **Rastrear os nomes gerados** — em `TAMQPChannel.DeclareQueue`, quando o nome
   informado é `''`, guardar o nome devolvido pelo `Declare-Ok` num conjunto do
   canal (ex.: `FServerNamedQueues`). Sem isso o canal não sabe que
   `amq.gen-ABC` é "gerado".
2. **Recovery estruturado** — hoje `TAMQPRecoveryAction` só guarda
   `Payload: TBytes` (opaco). Para as ações que referenciam uma fila gerada,
   guardar também **qual fila** cada `bind`/`consume` referencia (nome-alvo), de
   modo a poder reconstruir o payload depois.
3. **Reescrever no replay** — em `TAMQPChannel.Recover`: ao redeclarar com `''`,
   **capturar o novo nome** do `Declare-Ok` (hoje é descartado), montar um mapa
   `antigo → novo` e **reconstruir** os payloads de `bind`/`consume` com o nome
   novo antes de enviá-los.
4. **Expor o novo nome ao app** — mesmo com o passo 3, o código do usuário ainda
   segura o nome antigo. Para transparência total, adicionar um callback
   (ex.: `OnQueueRenamed(old, new)`) para o app atualizar suas referências.

Tudo isso deve ser **gated** por "há fila server-named neste canal?", para o
caminho de filas nomeadas (já testado) permanecer intacto. Consumer-tags **não**
são afetados — são gerados no cliente (`ctag-<canal>-<n>`), estáveis na reconexão.

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
