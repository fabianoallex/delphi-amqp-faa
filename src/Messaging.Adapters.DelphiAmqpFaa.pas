unit Messaging.Adapters.DelphiAmqpFaa;

{ Adapter que implementa o contrato de mensageria de `delphi-api-infra-faa`
  (`Messaging.Interfaces`) usando esta biblioteca (`AMQP.Connection`).

  Fica fora do núcleo da lib de propósito: quem só quer o cliente AMQP não
  precisa da infra no search path. Quem quiser plugar no padrão de registry
  da infra (`TMessagingRegistry.GetFactory('rabbitmq')`) inclui esta unit,
  que se registra sozinha na `initialization`.

  Requer, no projeto consumidor, os dois search paths:
    - a pasta `src` desta lib (delphi-amqp-faa)
    - `src/Messaging` de `delphi-api-infra-faa` (unit `Messaging.Interfaces`)

  Decisões de mapeamento:
  - `IMessageConsumer.Subscribe` pode ser chamado várias vezes (uma fila por
    chamada) antes de `Start`; `Start` abre conexão + canal e inicia o
    consumo de todas de uma vez. `Stop` cancela e fecha tudo.
  - Ack manual: confirma após `IMessageHandler.Handle` retornar sem erro;
    `Nack` com requeue se o handler levantar exceção (at-least-once).
  - `IMessagePayload.GetHeader` expõe as propriedades AMQP padrão por nome
    (`correlation-id`, `message-id`, `content-type`, `reply-to`) além de
    procurar em `Headers` (tabela de campos customizados). }

interface

uses
  System.SysUtils,
  System.Rtti,
  System.SyncObjs,
  System.Generics.Collections,
  Messaging.Interfaces,
  AMQP.Connection,
  AMQP.Basic.Methods,
  AMQP.Wire;

type
  { Escape hatch: acesso ao objeto NATIVO desta lib, para recursos que não cabem
    no contrato genérico de Messaging.Interfaces — publisher confirms,
    Connection.Blocked/Unblocked, Queue.Unbind, arguments tables etc.

    Obtido por Supports(publisher_ou_consumer, IDelphiAmqpFaaNative, LNative).
    Os objetos retornados são PROPRIEDADE do adapter — NÃO liberar. Só são
    válidos enquanto o publisher/consumer viver e estiver conectado; para o
    consumer, isso é DEPOIS de Start (antes, retornam nil).

    Em confirm mode, publique pelo canal nativo (NativeChannel.Publish/PublishText)
    para receber o seq-no — o Publish genérico da interface o descarta. }
  IDelphiAmqpFaaNative = interface
    ['{7B2E5A44-9C31-4E88-A0F2-1D6C3B9E4F70}']
    function NativeConnection: TAMQPConnection;
    function NativeChannel: TAMQPChannel;
  end;

  TDelphiAmqpFaaPayload = class(TInterfacedObject, IMessagePayload)
  private
    FDelivery: TAMQPDelivery;
  public
    constructor Create(const ADelivery: TAMQPDelivery);
    function GetBody: string;
    function GetRoutingKey: string;
    function GetHeader(const AKey: string): string;
  end;

  TDelphiAmqpFaaConsumer = class(TInterfacedObject, IMessageConsumer, IDelphiAmqpFaaNative)
  private
    FParams: TAMQPConnectionParams;
    FConnection: TAMQPConnection;
    FChannel: TAMQPChannel;
    FPending: TList<TPair<string, IMessageHandler>>; // (fila, handler) antes de Start
    FTagHandlers: TDictionary<string, IMessageHandler>; // consumer-tag -> handler
    FLock: TCriticalSection;
    FRunning: Boolean;
    procedure HandleDelivery(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery);
  public
    constructor Create(const AConfig: TMessagingConfig);
    destructor Destroy; override;
    procedure Subscribe(const AQueue: string; const AHandler: IMessageHandler);
    procedure Start;
    procedure Stop;
    function IsRunning: Boolean;
    // IDelphiAmqpFaaNative (nil antes de Start)
    function NativeConnection: TAMQPConnection;
    function NativeChannel: TAMQPChannel;
  end;

  TDelphiAmqpFaaPublisher = class(TInterfacedObject, IMessagePublisher, IDelphiAmqpFaaNative)
  private
    FConnection: TAMQPConnection;
    FChannel: TAMQPChannel;
  public
    constructor Create(const AConfig: TMessagingConfig);
    destructor Destroy; override;
    procedure Publish(const AExchange, ARoutingKey, ABody: string);
    // IDelphiAmqpFaaNative (disponível já após o constructor)
    function NativeConnection: TAMQPConnection;
    function NativeChannel: TAMQPChannel;
  end;

  TDelphiAmqpFaaMessagingFactory = class(TInterfacedObject, IMessagingFactory)
  public
    function CreateConsumer(const AConfig: TMessagingConfig): IMessageConsumer;
    function CreatePublisher(const AConfig: TMessagingConfig): IMessagePublisher;
  end;

implementation

uses
  Messaging.Adapters.Registry;

function ToParams(const AConfig: TMessagingConfig): TAMQPConnectionParams;
begin
  Result := TAMQPConnectionParams.Localhost;
  Result.Host := AConfig.Host;
  Result.Port := Word(AConfig.Port);
  Result.VirtualHost := AConfig.VHost;
  Result.User := AConfig.User;
  Result.Password := AConfig.Password;
  // TLS por convenção de porta: 5671 (amqps, IANA) liga o TLS com validação de
  // cadeia/hostname (TlsVerifyPeer=True, o padrão seguro). TMessagingConfig ainda
  // não tem campos de TLS (é agnóstico, fica no infra) — cert self-signed/dev,
  // verify-off ou server-name custom exigem estender a config lá (ver backlog).
  if Result.Port = 5671 then
    Result.UseTls := True;
end;

{ TDelphiAmqpFaaPayload }

constructor TDelphiAmqpFaaPayload.Create(const ADelivery: TAMQPDelivery);
begin
  inherited Create;
  FDelivery := ADelivery;
end;

function TDelphiAmqpFaaPayload.GetBody: string;
begin
  Result := FDelivery.BodyAsText;
end;

function TDelphiAmqpFaaPayload.GetRoutingKey: string;
begin
  Result := FDelivery.RoutingKey;
end;

function TDelphiAmqpFaaPayload.GetHeader(const AKey: string): string;
var
  LValue: TValue;
begin
  if SameText(AKey, 'correlation-id') then
    Exit(FDelivery.Properties.CorrelationId);
  if SameText(AKey, 'message-id') then
    Exit(FDelivery.Properties.MessageId);
  if SameText(AKey, 'content-type') then
    Exit(FDelivery.Properties.ContentType);
  if SameText(AKey, 'reply-to') then
    Exit(FDelivery.Properties.ReplyTo);

  Result := '';
  if FDelivery.Properties.Has(bpHeaders) and Assigned(FDelivery.Properties.Headers) then
    if FDelivery.Properties.Headers.TryGetValue(AKey, LValue) then
      Result := LValue.ToString;
end;

{ TDelphiAmqpFaaConsumer }

constructor TDelphiAmqpFaaConsumer.Create(const AConfig: TMessagingConfig);
begin
  inherited Create;
  FParams := ToParams(AConfig);
  FPending := TList<TPair<string, IMessageHandler>>.Create;
  FTagHandlers := TDictionary<string, IMessageHandler>.Create;
  FLock := TCriticalSection.Create;
end;

destructor TDelphiAmqpFaaConsumer.Destroy;
begin
  Stop;
  FLock.Free;
  FTagHandlers.Free;
  FPending.Free;
  inherited;
end;

procedure TDelphiAmqpFaaConsumer.Subscribe(const AQueue: string; const AHandler: IMessageHandler);
begin
  if FRunning then
    raise Exception.Create('Subscribe deve ser chamado antes de Start');
  FPending.Add(TPair<string, IMessageHandler>.Create(AQueue, AHandler));
end;

procedure TDelphiAmqpFaaConsumer.HandleDelivery(AChannel: TAMQPChannel;
  const ADelivery: TAMQPDelivery);
var
  LHandler: IMessageHandler;
  LPayload: IMessagePayload;
begin
  // Já roda numa thread do pool (despachada por TAMQPChannel.DispatchDelivery) -
  // mensagens de filas/handlers diferentes são processadas em paralelo.
  FLock.Enter;
  try
    if not FTagHandlers.TryGetValue(ADelivery.ConsumerTag, LHandler) then
      LHandler := nil;
  finally
    FLock.Leave;
  end;

  if not Assigned(LHandler) then
  begin
    AChannel.Nack(ADelivery.DeliveryTag, True);
    Exit;
  end;

  LPayload := TDelphiAmqpFaaPayload.Create(ADelivery);
  try
    LHandler.Handle(LPayload);
    AChannel.Ack(ADelivery.DeliveryTag);
  except
    AChannel.Nack(ADelivery.DeliveryTag, True); // requeue: at-least-once
  end;
end;

procedure TDelphiAmqpFaaConsumer.Start;
var
  LPair: TPair<string, IMessageHandler>;
  LTag: string;
begin
  if FRunning then
    Exit;
  FConnection := TAMQPConnection.Create(FParams);
  FConnection.Open;
  FChannel := FConnection.CreateChannel;
  FChannel.Qos(20); // prefetch: limita mensagens não confirmadas em voo por vez
  for LPair in FPending do
  begin
    LTag := FChannel.Consume(LPair.Key, HandleDelivery);
    FTagHandlers.Add(LTag, LPair.Value);
  end;
  FRunning := True;
end;

procedure TDelphiAmqpFaaConsumer.Stop;
var
  LTag: string;
begin
  if not FRunning then
    Exit;
  for LTag in FTagHandlers.Keys do
    try
      FChannel.Cancel(LTag);
    except
    end;
  FTagHandlers.Clear;
  FreeAndNil(FChannel);
  FreeAndNil(FConnection);
  FRunning := False;
end;

function TDelphiAmqpFaaConsumer.IsRunning: Boolean;
begin
  Result := FRunning;
end;

function TDelphiAmqpFaaConsumer.NativeConnection: TAMQPConnection;
begin
  Result := FConnection; // nil antes de Start
end;

function TDelphiAmqpFaaConsumer.NativeChannel: TAMQPChannel;
begin
  Result := FChannel; // nil antes de Start
end;

{ TDelphiAmqpFaaPublisher }

constructor TDelphiAmqpFaaPublisher.Create(const AConfig: TMessagingConfig);
begin
  inherited Create;
  FConnection := TAMQPConnection.Create(ToParams(AConfig));
  FConnection.Open;
  FChannel := FConnection.CreateChannel;
end;

destructor TDelphiAmqpFaaPublisher.Destroy;
begin
  FreeAndNil(FChannel);
  FreeAndNil(FConnection);
  inherited;
end;

procedure TDelphiAmqpFaaPublisher.Publish(const AExchange, ARoutingKey, ABody: string);
begin
  FChannel.PublishText(AExchange, ARoutingKey, ABody);
end;

function TDelphiAmqpFaaPublisher.NativeConnection: TAMQPConnection;
begin
  Result := FConnection; // pronto já no constructor
end;

function TDelphiAmqpFaaPublisher.NativeChannel: TAMQPChannel;
begin
  Result := FChannel;
end;

{ TDelphiAmqpFaaMessagingFactory }

function TDelphiAmqpFaaMessagingFactory.CreateConsumer(const AConfig: TMessagingConfig): IMessageConsumer;
begin
  Result := TDelphiAmqpFaaConsumer.Create(AConfig);
end;

function TDelphiAmqpFaaMessagingFactory.CreatePublisher(const AConfig: TMessagingConfig): IMessagePublisher;
begin
  Result := TDelphiAmqpFaaPublisher.Create(AConfig);
end;

initialization
  TMessagingRegistry.RegisterFactory('rabbitmq', TDelphiAmqpFaaMessagingFactory.Create);

end.
