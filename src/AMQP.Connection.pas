unit AMQP.Connection;

{ Conexão AMQP 0-9-1 sobre socket TCP, com thread de leitura dedicada.

  Arquitetura de concorrência (item 3 do roadmap):
  - O handshake (Open) é feito de forma síncrona, lendo o socket inline, ANTES
    de qualquer thread existir.
  - Depois do Open-Ok, uma única thread de leitura (TAMQPReaderThread) passa a
    ser a ÚNICA que lê o socket. Ela só faz: ler frame -> demultiplexar por canal
    -> entregar. Nunca roda código do usuário nem bloqueia.
  - TODAS as escritas no socket passam por FWriteLock (uma TCriticalSection), de
    modo que RPCs, publishes, acks e as respostas da própria thread de leitura
    (Close-Ok) não se embaralham.
  - RPC (declare/bind/get/close/...) é feito por evento: o chamador registra o
    que espera, envia o método e aguarda um TEvent que a thread de leitura sinaliza
    ao entregar a resposta.

  Invariantes (para não introduzir corrida/deadlock):
  - Só a thread de leitura lê o socket depois do handshake.
  - Ordem de locks sempre "de fora pra dentro": FChannelsLock -> FWriteLock e
    FRpcLock -> FWriteLock. FWriteLock é sempre o mais interno (nunca se adquire
    outro lock segurando ele).
  - A thread de leitura escreve o slot de RPC e chama SetEvent; o chamador só lê
    o slot depois de WaitFor retornar (o evento é a barreira de sincronização).

  Consumo (Basic.Consume + despacho para thread pool) e heartbeat vêm nos
  próximos incrementos. }

interface

uses
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Generics.Collections,
  System.Net.Socket,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method,
  AMQP.Frame,
  AMQP.Connection.Methods,
  AMQP.Exchange.Methods,
  AMQP.Queue.Methods,
  AMQP.Basic.Methods;

type
  EAMQPConnection = class(Exception);
  EAMQPChannel = class(Exception);

  { Adapta um TSocket como TStream, para os frames trafegarem por
    TAMQPFrame.ReadFrom/WriteTo. Não é dono do socket. }
  TAMQPSocketStream = class(TStream)
  private
    FSocket: TSocket;
  public
    constructor Create(ASocket: TSocket);
    function Read(var Buffer; Count: Longint): Longint; override;
    function Write(const Buffer; Count: Longint): Longint; override;
    function Seek(const Offset: Int64; Origin: TSeekOrigin): Int64; override;
  end;

  TAMQPConnectionParams = record
    Host: string;
    Port: Word;
    VirtualHost: string;
    User: string;
    Password: string;
    // Preferências de tune do cliente (0 = sem limite; ver NegotiateTune).
    ChannelMax: Word;
    FrameMax: Cardinal;
    Heartbeat: Word;
    // Reconexão automática (opt-in).
    AutoReconnect: Boolean;
    ReconnectDelayMs: Cardinal;    // espera entre tentativas (padrão 2000)
    MaxReconnectAttempts: Integer; // 0 = infinitas
    ConnectionName: string;        // opcional; vai em client-properties
    /// Parâmetros padrão: localhost:5672, vhost '/', guest/guest.
    class function Localhost: TAMQPConnectionParams; static;
  end;

  { Resultado de Basic.Get. Se Found=False, a fila estava vazia. Se
    Properties tiver Headers, o chamador é dono dessa tabela (liberar). }
  TAMQPGetResult = record
    Found: Boolean;
    DeliveryTag: UInt64;
    Redelivered: Boolean;
    Exchange: string;
    RoutingKey: string;
    MessageCount: Cardinal;
    Properties: TAMQPBasicProperties;
    Body: TBytes;
    function BodyAsText: string;
  end;

  TAMQPConnection = class;
  TAMQPChannel = class;

  { Callback de eventos de conexão (desconexão / reconexão). Roda numa thread
    interna; mantenha-o curto e não bloqueante. }
  TAMQPConnectionEvent = reference to procedure(AConnection: TAMQPConnection);

  { Mensagem entregue a um consumer (Basic.Deliver). Se Properties tiver
    Headers, a tabela é liberada automaticamente após o callback retornar. }
  TAMQPDelivery = record
    ConsumerTag: string;
    DeliveryTag: UInt64;
    Redelivered: Boolean;
    Exchange: string;
    RoutingKey: string;
    Properties: TAMQPBasicProperties;
    Body: TBytes;
    function BodyAsText: string;
  end;

  { Callback de consumer. Roda numa thread do pool (TTask), então NÃO bloqueia a
    thread de leitura. Use AChannel.Ack(ADelivery.DeliveryTag) para confirmar. }
  TAMQPConsumerCallback = reference to procedure(AChannel: TAMQPChannel;
    const ADelivery: TAMQPDelivery);

  { Ação de topologia gravada para replay após reconexão. Guardamos o payload
    já serializado do método (qos/declare/bind/consume) — assim argumentos e
    flags vêm de graça. Consumers guardam também o tag e o callback. }
  TAMQPRecoveryAction = record
    Payload: TBytes;
    AwaitReply: Boolean;    // True: espera o -Ok (CallRpc); False: só envia
    IsConsume: Boolean;
    ConsumerTag: string;
    Callback: TAMQPConsumerCallback;
  end;

  TAMQPRpcKind = (rkNone, rkMethod, rkMessage, rkError);

  { Canal de dados. Criado via TAMQPConnection.CreateChannel (que já o abre).
    O chamador é dono do canal e deve liberá-lo (Free). }
  TAMQPChannel = class
  private
    FConnection: TAMQPConnection;
    FChannelId: Word;
    FIsOpen: Boolean;
    FClosed: Boolean;
    // --- RPC (uma chamada por vez neste canal) ---
    FRpcLock: TCriticalSection;
    FRpcEvent: TEvent;
    FRpcKind: TAMQPRpcKind;
    FRpcMethodPayload: TBytes;
    FRpcMessage: TAMQPGetResult;
    FRpcError: string;
    // --- montagem de conteúdo (escrita só pela thread de leitura) ---
    FAsmState: (asIdle, asHeader, asBody);
    FAsmMethodId: TAMQPMethodId;
    FAsmGetOk: TAMQPBasicGetOk;
    FAsmDeliver: TAMQPBasicDeliver;
    FAsmProps: TAMQPBasicProperties;
    FAsmBody: TBytes;
    FAsmRemaining: UInt64;
    // --- consumers ---
    FConsumers: TDictionary<string, TAMQPConsumerCallback>;
    FConsumersLock: TCriticalSection;
    FConsumerCounter: Integer;
    FInFlight: Integer; // callbacks em execução no pool (atômico)
    // --- topologia gravada para reconexão ---
    FRecovery: TList<TAMQPRecoveryAction>;
    FRecoveryLock: TCriticalSection;
    procedure AddRecovery(const APayload: TBytes; AAwaitReply: Boolean;
      AIsConsume: Boolean = False; const AConsumerTag: string = '';
      const ACallback: TAMQPConsumerCallback = nil);
    /// Remove a gravação de recovery de um consumer (ao cancelá-lo).
    procedure RemoveConsumerRecovery(const AConsumerTag: string);
    /// Reabre o canal e replaya a topologia gravada (chamado na reconexão).
    procedure Recover;
    procedure Open;
    /// Envia um método e aguarda a resposta (payload do método de resposta).
    function CallRpc(const ARequest: TBytes): TBytes;
    // sinalizadores chamados pela thread de leitura:
    procedure SignalMethod(const APayload: TBytes);
    procedure SignalMessage(const AMessage: TAMQPGetResult);
    procedure SignalError(const AMessage: string);
    procedure CompleteContent;
    /// Despacha uma entrega para o callback do consumer, no thread pool.
    procedure DispatchDelivery(const ADeliver: TAMQPBasicDeliver;
      const AProps: TAMQPBasicProperties; const ABody: TBytes);
    /// Aguarda os callbacks em voo terminarem (usado ao fechar o canal).
    procedure DrainInFlight;
    /// Trata um frame entregue pela thread de leitura (roda NA thread de leitura).
    procedure HandleFrame(const AFrame: TAMQPFrame);
  public
    constructor Create(AConnection: TAMQPConnection; AChannelId: Word);
    destructor Destroy; override;

    procedure DeclareExchange(const ADeclare: TAMQPExchangeDeclare);
    function DeclareQueue(const ADeclare: TAMQPQueueDeclare): TAMQPQueueDeclareOk;
    procedure BindQueue(const ABind: TAMQPQueueBind);

    /// Publica ABody com as propriedades AProps. Fire-and-forget (sem confirms).
    procedure Publish(const AExchange, ARoutingKey: string; const ABody: TBytes;
      const AProps: TAMQPBasicProperties; AMandatory: Boolean = False);
    /// Conveniência: publica um texto UTF-8 como content-type text/plain.
    procedure PublishText(const AExchange, ARoutingKey, AText: string;
      APersistent: Boolean = True);

    /// Busca uma mensagem da fila. Result.Found=False se estava vazia.
    function BasicGet(const AQueue: string; ANoAck: Boolean = True): TAMQPGetResult;
    procedure Ack(ADeliveryTag: UInt64; AMultiple: Boolean = False);
    procedure Nack(ADeliveryTag: UInt64; ARequeue: Boolean = True;
      AMultiple: Boolean = False);

    /// Limita quantas mensagens não confirmadas o servidor entrega por vez
    /// (prefetch). Útil para dividir a carga entre os callbacks concorrentes.
    procedure Qos(APrefetchCount: Word; AGlobal: Boolean = False);
    /// Inicia o consumo da fila. Cada mensagem chega em ACallback, despachado
    /// num thread do pool. Devolve o consumer-tag (use em Cancel). Com
    /// ANoAck=False (padrão), confirme com AChannel.Ack no callback.
    function Consume(const AQueue: string; const ACallback: TAMQPConsumerCallback;
      ANoAck: Boolean = False; AExclusive: Boolean = False): string;
    /// Cancela um consumer pelo tag.
    procedure Cancel(const AConsumerTag: string);

    procedure Close(AReplyCode: Word = 200; const AReplyText: string = 'OK');

    property ChannelId: Word read FChannelId;
    property IsOpen: Boolean read FIsOpen;
  end;

  TAMQPReaderThread = class(TThread)
  private
    FConnection: TAMQPConnection;
  protected
    procedure Execute; override;
  public
    constructor Create(AConnection: TAMQPConnection);
  end;

  { Thread de heartbeat: acorda periodicamente (espera interrompível via TEvent,
    NÃO TTimer) para enviar heartbeat quando o envio está ocioso e para detectar
    conexão morta (nenhum frame recebido em 2x o intervalo negociado). }
  TAMQPHeartbeatThread = class(TThread)
  private
    FConnection: TAMQPConnection;
  protected
    procedure Execute; override;
  public
    constructor Create(AConnection: TAMQPConnection);
  end;

  TAMQPConnection = class
  private
    FParams: TAMQPConnectionParams;
    FSocket: TSocket;
    FStream: TAMQPSocketStream;
    FIsOpen: Boolean;
    FNegotiated: TAMQPConnectionTune;
    FNextChannel: Word;
    FWriteLock: TCriticalSection;
    FChannelsLock: TCriticalSection;
    FChannels: TDictionary<Word, TAMQPChannel>;
    FReadThread: TAMQPReaderThread;
    FCloseOkEvent: TEvent;
    // --- heartbeat ---
    FHeartbeatThread: TAMQPHeartbeatThread;
    FHbStopEvent: TEvent;
    FLastWriteTick: UInt64; // atualizado a cada frame enviado
    FLastReadTick: UInt64;  // atualizado a cada frame recebido
    // --- reconexão ---
    FDeliberateClose: Boolean; // True quando o usuário fechou (não reconectar)
    FReconnecting: Boolean;
    FOnDisconnect: TAMQPConnectionEvent;
    FOnReconnect: TAMQPConnectionEvent;
    FOnReconnectFailed: TAMQPConnectionEvent;
    // --- envio (serializado por FWriteLock) ---
    procedure SendFrameNoLock(AFrameType: Byte; AChannel: Word; const APayload: TBytes);
    procedure SendFrame(AFrameType: Byte; AChannel: Word; const APayload: TBytes);
    procedure SendMethod(AChannel: Word; const APayload: TBytes);
    // --- leitura síncrona, só durante o handshake ---
    function NextFrame: TAMQPFrame;
    function NextMethodOn(AExpectChannel: Word; out AId: TAMQPMethodId): TAMQPReader;
    function ExpectMethod(AExpectChannel, AClassId, AMethodId: Word): TAMQPReader;
    function BuildClientProperties: TAMQPFieldTable;
    procedure Handshake;
    procedure EstablishConnection;
    procedure CloseSocketStream;
    // --- threads ---
    procedure StartReadThread;
    procedure StopReadThread;
    procedure StartHeartbeatThread;
    procedure StopHeartbeatThread;
    procedure HeartbeatTick;
    procedure DispatchFrame(const AFrame: TAMQPFrame);
    procedure HandleConnectionFrame(const AFrame: TAMQPFrame);
    procedure ReadThreadFinished(const AError: string);
    procedure UnregisterChannel(AChannelId: Word);
    // --- reconexão ---
    procedure RunReconnect;
    procedure RecoverAllChannels;
    procedure DrainAllChannels;
    procedure WaitReconnectStopped;
  public
    constructor Create(const AParams: TAMQPConnectionParams);
    destructor Destroy; override;

    /// Conecta o socket e executa o handshake. Levanta EAMQPConnection em falha.
    procedure Open;
    /// Abre um novo canal (já aberto). O chamador é dono e deve liberá-lo.
    function CreateChannel: TAMQPChannel;
    /// Envia Connection.Close, aguarda Close-Ok e fecha o socket.
    procedure Close(AReplyCode: Word = 200; const AReplyText: string = 'Goodbye');
    /// Fecha o socket abruptamente para simular queda de rede (uso em testes).
    procedure DropConnectionForTest;

    property IsOpen: Boolean read FIsOpen;
    property NegotiatedTune: TAMQPConnectionTune read FNegotiated;
    /// Disparado (em thread interna) quando a conexão cai e a reconexão inicia.
    property OnDisconnect: TAMQPConnectionEvent read FOnDisconnect write FOnDisconnect;
    /// Disparado após reconectar e restaurar a topologia com sucesso.
    property OnReconnect: TAMQPConnectionEvent read FOnReconnect write FOnReconnect;
    /// Disparado quando a reconexão esgota as tentativas.
    property OnReconnectFailed: TAMQPConnectionEvent read FOnReconnectFailed write FOnReconnectFailed;
  end;

implementation

uses
  System.Threading,
  AMQP.Channel.Methods;

const
  AMQP_RPC_TIMEOUT_MS = 30000; // tempo máximo aguardando resposta de RPC

{ TAMQPGetResult }

function TAMQPGetResult.BodyAsText: string;
begin
  Result := TEncoding.UTF8.GetString(Body);
end;

{ TAMQPDelivery }

function TAMQPDelivery.BodyAsText: string;
begin
  Result := TEncoding.UTF8.GetString(Body);
end;

{ TAMQPSocketStream }

constructor TAMQPSocketStream.Create(ASocket: TSocket);
begin
  inherited Create;
  FSocket := ASocket;
end;

function TAMQPSocketStream.Read(var Buffer; Count: Longint): Longint;
begin
  Result := FSocket.Receive(Buffer, Count);
end;

function TAMQPSocketStream.Write(const Buffer; Count: Longint): Longint;
begin
  Result := FSocket.Send(Buffer, Count);
end;

function TAMQPSocketStream.Seek(const Offset: Int64; Origin: TSeekOrigin): Int64;
begin
  raise EAMQPConnection.Create('TAMQPSocketStream não suporta Seek');
end;

{ TAMQPConnectionParams }

class function TAMQPConnectionParams.Localhost: TAMQPConnectionParams;
begin
  Result.Host := 'localhost';
  Result.Port := 5672;
  Result.VirtualHost := '/';
  Result.User := 'guest';
  Result.Password := 'guest';
  Result.ChannelMax := 2047;
  Result.FrameMax := 131072;
  Result.Heartbeat := 60;
  Result.AutoReconnect := False;
  Result.ReconnectDelayMs := 2000;
  Result.MaxReconnectAttempts := 0; // infinitas
  Result.ConnectionName := '';
end;

{ TAMQPReaderThread }

constructor TAMQPReaderThread.Create(AConnection: TAMQPConnection);
begin
  FConnection := AConnection;
  FreeOnTerminate := False;
  inherited Create(False);
end;

procedure TAMQPReaderThread.Execute;
var
  LFrame: TAMQPFrame;
begin
  try
    while not Terminated do
    begin
      LFrame := TAMQPFrame.ReadFrom(FConnection.FStream);
      TInterlocked.Exchange(FConnection.FLastReadTick,
        TThread.GetTickCount64); // atômico p/ a thread de heartbeat
      FConnection.DispatchFrame(LFrame);
    end;
    FConnection.ReadThreadFinished('');
  except
    on E: Exception do
      // fim de stream (socket fechado) ou erro de protocolo: encerra a thread.
      FConnection.ReadThreadFinished(E.Message);
  end;
end;

{ TAMQPHeartbeatThread }

constructor TAMQPHeartbeatThread.Create(AConnection: TAMQPConnection);
begin
  FConnection := AConnection;
  FreeOnTerminate := False;
  inherited Create(False);
end;

procedure TAMQPHeartbeatThread.Execute;
var
  LWaitMs: Cardinal;
begin
  // Acorda a cada metade do intervalo (mínimo 1s); a espera é interrompível
  // via FHbStopEvent (não usamos TTimer).
  LWaitMs := (Cardinal(FConnection.FNegotiated.Heartbeat) * 1000) div 2;
  if LWaitMs < 1000 then
    LWaitMs := 1000;
  while not Terminated do
  begin
    if FConnection.FHbStopEvent.WaitFor(LWaitMs) = wrSignaled then
      Break; // parada solicitada
    if Terminated then
      Break;
    FConnection.HeartbeatTick;
  end;
end;

{ TAMQPConnection }

constructor TAMQPConnection.Create(const AParams: TAMQPConnectionParams);
begin
  inherited Create;
  FParams := AParams;
  FWriteLock := TCriticalSection.Create;
  FChannelsLock := TCriticalSection.Create;
  FChannels := TDictionary<Word, TAMQPChannel>.Create;
  FCloseOkEvent := TEvent.Create(nil, True, False, '');
  FHbStopEvent := TEvent.Create(nil, True, False, '');
end;

destructor TAMQPConnection.Destroy;
var
  LChan: TAMQPChannel;
begin
  // Close é idempotente: aborta reconexão, para threads e fecha o socket.
  try
    Close;
  except
  end;
  // Libera canais que o usuário não liberou (cada Free chama UnregisterChannel,
  // que remove do dicionário; por isso iteramos sobre uma cópia).
  if Assigned(FChannels) then
  begin
    for LChan in FChannels.Values.ToArray do
      LChan.Free;
    FChannels.Free;
  end;
  FChannelsLock.Free;
  FWriteLock.Free;
  FCloseOkEvent.Free;
  FHbStopEvent.Free;
  inherited;
end;

procedure TAMQPConnection.SendFrameNoLock(AFrameType: Byte; AChannel: Word;
  const APayload: TBytes);
var
  LFrame: TAMQPFrame;
begin
  // Assume FWriteLock já adquirido (envio de grupo de frames, ex.: Publish).
  if not Assigned(FStream) then
    raise EAMQPConnection.Create('conexão indisponível no momento (reconectando?)');
  LFrame := TAMQPFrame.Create(AFrameType, AChannel, APayload);
  LFrame.WriteTo(FStream);
  // Atômico: lido pela thread de heartbeat; no Win32 um store de 64 bits não é
  // atômico e poderia ser "torn" (ver TInterlocked em HeartbeatTick).
  TInterlocked.Exchange(FLastWriteTick, TThread.GetTickCount64);
end;

procedure TAMQPConnection.SendFrame(AFrameType: Byte; AChannel: Word;
  const APayload: TBytes);
begin
  FWriteLock.Enter;
  try
    SendFrameNoLock(AFrameType, AChannel, APayload);
  finally
    FWriteLock.Leave;
  end;
end;

procedure TAMQPConnection.SendMethod(AChannel: Word; const APayload: TBytes);
begin
  SendFrame(AMQP_FRAME_METHOD, AChannel, APayload);
end;

{ --- Leitura síncrona (só no handshake) --- }

function TAMQPConnection.NextFrame: TAMQPFrame;
begin
  repeat
    Result := TAMQPFrame.ReadFrom(FStream);
  until not Result.IsHeartbeat;
end;

function TAMQPConnection.NextMethodOn(AExpectChannel: Word;
  out AId: TAMQPMethodId): TAMQPReader;
var
  LFrame: TAMQPFrame;
  LConnClose: TAMQPConnectionClose;
begin
  LFrame := NextFrame;
  if not LFrame.IsMethod then
    raise EAMQPConnection.CreateFmt(
      'frame inesperado (tipo %d, canal %d)', [LFrame.FrameType, LFrame.Channel]);

  Result := TAMQPReader.Create(LFrame.Payload);
  try
    AId := ReadMethodHeader(Result);
    if AId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE) then
    begin
      LConnClose := DecodeClose(Result);
      raise EAMQPConnection.CreateFmt('conexão recusada pelo servidor: %d %s',
        [LConnClose.ReplyCode, LConnClose.ReplyText]);
    end;
    if LFrame.Channel <> AExpectChannel then
      raise EAMQPConnection.CreateFmt('resposta em canal inesperado: %d (esperava %d)',
        [LFrame.Channel, AExpectChannel]);
  except
    Result.Free;
    raise;
  end;
end;

function TAMQPConnection.ExpectMethod(AExpectChannel, AClassId,
  AMethodId: Word): TAMQPReader;
var
  LId: TAMQPMethodId;
begin
  Result := NextMethodOn(AExpectChannel, LId);
  try
    if not LId.Matches(AClassId, AMethodId) then
      raise EAMQPConnection.CreateFmt(
        'resposta inesperada: método %d/%d (esperava %d/%d)',
        [LId.ClassId, LId.MethodId, AClassId, AMethodId]);
  except
    Result.Free;
    raise;
  end;
end;

procedure TAMQPConnection.Handshake;
var
  LReader: TAMQPReader;
  LStart: TAMQPConnectionStart;
  LServerTune: TAMQPConnectionTune;
  LProps: TAMQPFieldTable;
begin
  WriteProtocolHeader(FStream);

  LReader := ExpectMethod(AMQP_CHANNEL_CONNECTION, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START);
  try
    LStart := DecodeStart(LReader);
  finally
    LReader.Free;
  end;

  try
    if not LStart.SupportsMechanism(AMQP_AUTH_PLAIN) then
      raise EAMQPConnection.CreateFmt(
        'servidor não oferece o mecanismo %s (oferece: %s)',
        [AMQP_AUTH_PLAIN, LStart.Mechanisms]);

    LProps := BuildClientProperties;
    try
      SendMethod(AMQP_CHANNEL_CONNECTION, BuildStartOk(LProps, AMQP_AUTH_PLAIN,
        PlainAuthResponse(FParams.User, FParams.Password), AMQP_LOCALE_DEFAULT));
    finally
      LProps.Free;
    end;
  finally
    LStart.ServerProperties.Free;
  end;

  LReader := ExpectMethod(AMQP_CHANNEL_CONNECTION, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE);
  try
    LServerTune := DecodeTune(LReader);
  finally
    LReader.Free;
  end;

  FNegotiated := NegotiateTune(LServerTune,
    FParams.ChannelMax, FParams.FrameMax, FParams.Heartbeat);
  SendMethod(AMQP_CHANNEL_CONNECTION, BuildTuneOk(FNegotiated));

  SendMethod(AMQP_CHANNEL_CONNECTION, BuildOpen(FParams.VirtualHost));
  LReader := ExpectMethod(AMQP_CHANNEL_CONNECTION, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN_OK);
  try
    DecodeOpenOk(LReader);
  finally
    LReader.Free;
  end;
end;

function TAMQPConnection.BuildClientProperties: TAMQPFieldTable;
begin
  Result := DefaultClientProperties;
  if FParams.ConnectionName <> '' then
    Result.Put('connection_name', FParams.ConnectionName);
end;

procedure TAMQPConnection.EstablishConnection;
begin
  // Cria socket, faz o handshake (síncrono, inline) e sobe as threads.
  // Usado tanto no Open inicial quanto na reconexão.
  FSocket := TSocket.Create(TSocketType.TCP);
  FSocket.Connect(FParams.Host, '', '', FParams.Port);
  FStream := TAMQPSocketStream.Create(FSocket);

  Handshake; // antes de qualquer thread

  FLastWriteTick := TThread.GetTickCount64;
  FLastReadTick := FLastWriteTick;
  StartReadThread;     // a partir daqui, só a thread lê o socket
  StartHeartbeatThread;
  FIsOpen := True;
end;

procedure TAMQPConnection.CloseSocketStream;
begin
  if Assigned(FSocket) then
    try
      FSocket.Close;
    except
    end;
  FreeAndNil(FStream);
  FreeAndNil(FSocket);
end;

procedure TAMQPConnection.Open;
begin
  if FIsOpen then
    raise EAMQPConnection.Create('conexão já está aberta');
  EstablishConnection;
end;

procedure TAMQPConnection.DropConnectionForTest;
begin
  if Assigned(FSocket) then
    try
      FSocket.Close; // a thread de leitura vai perceber e disparar a reconexão
    except
    end;
end;

{ --- Thread de leitura --- }

procedure TAMQPConnection.StartReadThread;
begin
  FReadThread := TAMQPReaderThread.Create(Self);
end;

procedure TAMQPConnection.StopReadThread;
begin
  if Assigned(FReadThread) then
  begin
    FReadThread.Terminate;
    if Assigned(FSocket) then
      try
        FSocket.Close; // desbloqueia o ReadFrom da thread
      except
      end;
    FReadThread.WaitFor;
    FreeAndNil(FReadThread);
  end;
end;

{ --- Thread de heartbeat --- }

procedure TAMQPConnection.StartHeartbeatThread;
begin
  if FNegotiated.Heartbeat = 0 then
    Exit; // heartbeat desabilitado na negociação
  FHbStopEvent.ResetEvent;
  FHeartbeatThread := TAMQPHeartbeatThread.Create(Self);
end;

procedure TAMQPConnection.StopHeartbeatThread;
begin
  if Assigned(FHeartbeatThread) then
  begin
    FHeartbeatThread.Terminate;
    FHbStopEvent.SetEvent; // interrompe a espera imediatamente
    FHeartbeatThread.WaitFor;
    FreeAndNil(FHeartbeatThread);
  end;
end;

procedure TAMQPConnection.HeartbeatTick;
var
  LIntervalMs: UInt64;
  LNow, LLastRead, LLastWrite: UInt64;
begin
  LIntervalMs := UInt64(FNegotiated.Heartbeat) * 1000;
  if LIntervalMs = 0 then
    Exit;
  LNow := TThread.GetTickCount64;
  // Leituras atômicas (os ticks são escritos por outras threads; no Win32 um
  // load de 64 bits pode ser "torn" e produzir um delta absurdo).
  LLastRead := TInterlocked.Read(FLastReadTick);
  LLastWrite := TInterlocked.Read(FLastWriteTick);

  // Conexão morta: nenhum frame recebido em 2x o intervalo (o servidor também
  // manda heartbeats). Fecha o socket para desbloquear a thread de leitura.
  if (LNow - LLastRead) > (2 * LIntervalMs) then
  begin
    if Assigned(FSocket) then
      try
        FSocket.Close;
      except
      end;
    Exit;
  end;

  // Envio ocioso há >= metade do intervalo: manda um heartbeat.
  if (LNow - LLastWrite) >= (LIntervalMs div 2) then
    try
      SendFrame(AMQP_FRAME_HEARTBEAT, AMQP_CHANNEL_CONNECTION, nil);
    except
      // erro de escrita: a thread de leitura vai perceber o socket caído
    end;
end;

procedure TAMQPConnection.ReadThreadFinished(const AError: string);
var
  LChannels: TArray<TAMQPChannel>;
  LChan: TAMQPChannel;
  LMsg: string;
begin
  FIsOpen := False;
  // Acorda quem estiver esperando Close-Ok e qualquer RPC pendente nos canais.
  FCloseOkEvent.SetEvent;
  if AError <> '' then
    LMsg := 'conexão encerrada: ' + AError
  else
    LMsg := 'conexão encerrada';
  FChannelsLock.Enter;
  try
    LChannels := FChannels.Values.ToArray;
  finally
    FChannelsLock.Leave;
  end;
  for LChan in LChannels do
    LChan.SignalError(LMsg);

  // Queda inesperada: dispara a reconexão (numa thread própria, pois esta é a
  // thread de leitura que está terminando).
  if FParams.AutoReconnect and (not FDeliberateClose) and (not FReconnecting) then
  begin
    FReconnecting := True;
    TThread.CreateAnonymousThread(
      procedure
      begin
        RunReconnect;
      end).Start;
  end;
end;

procedure TAMQPConnection.DrainAllChannels;
var
  LChannels: TArray<TAMQPChannel>;
  LChan: TAMQPChannel;
begin
  FChannelsLock.Enter;
  try
    LChannels := FChannels.Values.ToArray;
  finally
    FChannelsLock.Leave;
  end;
  for LChan in LChannels do
    LChan.DrainInFlight;
end;

procedure TAMQPConnection.RecoverAllChannels;
var
  LChannels: TArray<TAMQPChannel>;
  LChan: TAMQPChannel;
begin
  FChannelsLock.Enter;
  try
    LChannels := FChannels.Values.ToArray;
  finally
    FChannelsLock.Leave;
  end;
  for LChan in LChannels do
    LChan.Recover;
end;

procedure TAMQPConnection.WaitReconnectStopped;
var
  LWaited: Integer;
begin
  // Assume FDeliberateClose já True. Espera a thread de reconexão encerrar para
  // evitar corrida no teardown do socket/threads.
  LWaited := 0;
  while FReconnecting and (LWaited < 12000) do
  begin
    TThread.Sleep(20);
    Inc(LWaited, 20);
  end;
end;

procedure TAMQPConnection.RunReconnect;
var
  LAttempt: Integer;
  LDelay: Cardinal;
begin
  // Roda numa thread anônima dedicada; FReconnecting já está True.
  try
    StopHeartbeatThread;
    StopReadThread;    // aguarda a thread de leitura antiga terminar
    DrainAllChannels;  // callbacks antigos terminam (acks falham no socket morto)
    CloseSocketStream;

    if Assigned(FOnDisconnect) then
      try FOnDisconnect(Self); except end;

    LDelay := FParams.ReconnectDelayMs;
    if LDelay = 0 then
      LDelay := 2000;

    LAttempt := 0;
    while not FDeliberateClose do
    begin
      TThread.Sleep(LDelay);
      if FDeliberateClose then
        Break;
      Inc(LAttempt);
      try
        EstablishConnection; // novo socket + handshake + threads (FIsOpen := True)
        RecoverAllChannels;  // reabre canais e replaya a topologia gravada
        if Assigned(FOnReconnect) then
          try FOnReconnect(Self); except end;
        Exit; // sucesso
      except
        // tentativa falhou: limpa o estado parcial e tenta de novo
        FIsOpen := False;
        StopHeartbeatThread;
        StopReadThread;
        CloseSocketStream;
        if (FParams.MaxReconnectAttempts > 0) and
           (LAttempt >= FParams.MaxReconnectAttempts) then
        begin
          if Assigned(FOnReconnectFailed) then
            try FOnReconnectFailed(Self); except end;
          Break;
        end;
      end;
    end;
  finally
    FReconnecting := False;
  end;
end;

procedure TAMQPConnection.DispatchFrame(const AFrame: TAMQPFrame);
var
  LChan: TAMQPChannel;
begin
  if AFrame.IsHeartbeat then
    Exit; // heartbeat tratado no item 4

  if AFrame.Channel = AMQP_CHANNEL_CONNECTION then
  begin
    HandleConnectionFrame(AFrame);
    Exit;
  end;

  FChannelsLock.Enter;
  try
    if not FChannels.TryGetValue(AFrame.Channel, LChan) then
      LChan := nil;
    if Assigned(LChan) then
      LChan.HandleFrame(AFrame);
  finally
    FChannelsLock.Leave;
  end;
end;

procedure TAMQPConnection.HandleConnectionFrame(const AFrame: TAMQPFrame);
var
  LReader: TAMQPReader;
  LId: TAMQPMethodId;
  LClose: TAMQPConnectionClose;
begin
  if not AFrame.IsMethod then
    Exit;
  LReader := TAMQPReader.Create(AFrame.Payload);
  try
    LId := ReadMethodHeader(LReader);
    if LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE_OK) then
    begin
      FCloseOkEvent.SetEvent;
    end
    else if LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE) then
    begin
      LClose := DecodeClose(LReader);
      FIsOpen := False;
      SendMethod(AMQP_CHANNEL_CONNECTION, BuildCloseOk);
      FCloseOkEvent.SetEvent;
      // (canais serão sinalizados quando a thread encerrar)
    end;
  finally
    LReader.Free;
  end;
end;

procedure TAMQPConnection.UnregisterChannel(AChannelId: Word);
begin
  FChannelsLock.Enter;
  try
    FChannels.Remove(AChannelId);
  finally
    FChannelsLock.Leave;
  end;
end;

function TAMQPConnection.CreateChannel: TAMQPChannel;
var
  LChan: TAMQPChannel;
begin
  if not FIsOpen then
    raise EAMQPConnection.Create('conexão não está aberta');

  // Alocação do id + inserção no dicionário sob o mesmo lock (evita duas threads
  // gerarem o mesmo channel-id e o segundo Add levantar/vazar o canal).
  FChannelsLock.Enter;
  try
    if (FNegotiated.ChannelMax > 0) and (FNextChannel >= FNegotiated.ChannelMax) then
      raise EAMQPConnection.CreateFmt(
        'limite de canais atingido (%d); reuso de canais ainda não implementado',
        [FNegotiated.ChannelMax]);
    Inc(FNextChannel);
    LChan := TAMQPChannel.Create(Self, FNextChannel);
    try
      FChannels.Add(LChan.ChannelId, LChan);
    except
      LChan.Free;
      raise;
    end;
  finally
    FChannelsLock.Leave;
  end;

  try
    LChan.Open; // RPC: usa a thread de leitura já rodando (fora do lock)
  except
    UnregisterChannel(LChan.ChannelId);
    LChan.Free;
    raise;
  end;
  Result := LChan;
end;

procedure TAMQPConnection.Close(AReplyCode: Word; const AReplyText: string);
var
  LClose: TAMQPConnectionClose;
begin
  // Idempotente. Sinaliza fechamento deliberado e aguarda qualquer reconexão em
  // curso encerrar antes de mexer no socket/threads (evita corrida).
  FDeliberateClose := True;
  WaitReconnectStopped;

  if FIsOpen then
  begin
    FIsOpen := False;
    StopHeartbeatThread; // não enviar heartbeats durante o fechamento
    LClose.ReplyCode := AReplyCode;
    LClose.ReplyText := AReplyText;
    LClose.ClassId := 0;
    LClose.MethodId := 0;
    try
      FCloseOkEvent.ResetEvent;
      SendMethod(AMQP_CHANNEL_CONNECTION, BuildClose(LClose));
      FCloseOkEvent.WaitFor(3000);
    except
    end;
  end;

  StopHeartbeatThread;
  StopReadThread; // termina a thread e fecha o socket
  CloseSocketStream;
end;

{ TAMQPChannel }

constructor TAMQPChannel.Create(AConnection: TAMQPConnection; AChannelId: Word);
begin
  inherited Create;
  FConnection := AConnection;
  FChannelId := AChannelId;
  FRpcLock := TCriticalSection.Create;
  FRpcEvent := TEvent.Create(nil, True, False, '');
  FConsumers := TDictionary<string, TAMQPConsumerCallback>.Create;
  FConsumersLock := TCriticalSection.Create;
  FRecovery := TList<TAMQPRecoveryAction>.Create;
  FRecoveryLock := TCriticalSection.Create;
  FAsmState := asIdle;
end;

destructor TAMQPChannel.Destroy;
begin
  if FIsOpen and (not FClosed) and FConnection.IsOpen then
    try
      Close;
    except
    end;
  DrainInFlight; // garante que nenhum callback ainda usa este canal
  FRecovery.Free;
  FRecoveryLock.Free;
  FConsumers.Free;
  FConsumersLock.Free;
  FRpcEvent.Free;
  FRpcLock.Free;
  inherited;
end;

procedure TAMQPChannel.AddRecovery(const APayload: TBytes; AAwaitReply: Boolean;
  AIsConsume: Boolean; const AConsumerTag: string;
  const ACallback: TAMQPConsumerCallback);
var
  LAction: TAMQPRecoveryAction;
begin
  LAction.Payload := APayload;
  LAction.AwaitReply := AAwaitReply;
  LAction.IsConsume := AIsConsume;
  LAction.ConsumerTag := AConsumerTag;
  LAction.Callback := ACallback;
  FRecoveryLock.Enter;
  try
    FRecovery.Add(LAction);
  finally
    FRecoveryLock.Leave;
  end;
end;

procedure TAMQPChannel.RemoveConsumerRecovery(const AConsumerTag: string);
var
  I: Integer;
begin
  FRecoveryLock.Enter;
  try
    for I := FRecovery.Count - 1 downto 0 do
      if FRecovery[I].IsConsume and (FRecovery[I].ConsumerTag = AConsumerTag) then
        FRecovery.Delete(I);
  finally
    FRecoveryLock.Leave;
  end;
end;

procedure TAMQPChannel.Recover;
var
  LActions: TArray<TAMQPRecoveryAction>;
  LAction: TAMQPRecoveryAction;
begin
  // Roda na thread de reconexão. Segura FRpcLock durante todo o replay para que
  // RPCs do usuário no canal esperem a recuperação terminar. FRpcLock é
  // recursivo (critical section), então CallRpc interno funciona.
  FRpcLock.Enter;
  try
    FClosed := False;
    FAsmState := asIdle;
    Open; // Channel.Open no novo socket (define FIsOpen := True)

    FConsumersLock.Enter;
    try
      FConsumers.Clear; // registros da sessão antiga; serão re-adicionados
    finally
      FConsumersLock.Leave;
    end;

    FRecoveryLock.Enter;
    try
      LActions := FRecovery.ToArray;
    finally
      FRecoveryLock.Leave;
    end;

    for LAction in LActions do
    begin
      if LAction.IsConsume then
      begin
        FConsumersLock.Enter;
        try
          FConsumers.AddOrSetValue(LAction.ConsumerTag, LAction.Callback);
        finally
          FConsumersLock.Leave;
        end;
      end;
      if LAction.AwaitReply then
        CallRpc(LAction.Payload)
      else
        FConnection.SendMethod(FChannelId, LAction.Payload);
    end;
  finally
    FRpcLock.Leave;
  end;
end;

function TAMQPChannel.CallRpc(const ARequest: TBytes): TBytes;
begin
  FRpcLock.Enter;
  try
    if FClosed then
      raise EAMQPChannel.Create('canal fechado');
    FRpcKind := rkNone;
    FRpcError := '';
    FRpcEvent.ResetEvent;
    FConnection.SendMethod(FChannelId, ARequest);
    if FRpcEvent.WaitFor(AMQP_RPC_TIMEOUT_MS) <> wrSignaled then
      raise EAMQPChannel.Create('timeout aguardando resposta do servidor');
    case FRpcKind of
      rkMethod:
        Result := FRpcMethodPayload;
      rkError:
        raise EAMQPChannel.Create(FRpcError);
    else
      raise EAMQPChannel.Create('resposta de RPC inesperada');
    end;
  finally
    FRpcLock.Leave;
  end;
end;

procedure TAMQPChannel.SignalMethod(const APayload: TBytes);
begin
  FRpcMethodPayload := APayload;
  FRpcKind := rkMethod;
  FRpcEvent.SetEvent;
end;

procedure TAMQPChannel.SignalMessage(const AMessage: TAMQPGetResult);
begin
  FRpcMessage := AMessage;
  FRpcKind := rkMessage;
  FRpcEvent.SetEvent;
end;

procedure TAMQPChannel.SignalError(const AMessage: string);
begin
  FClosed := True;
  FIsOpen := False;
  FRpcError := AMessage;
  FRpcKind := rkError;
  FRpcEvent.SetEvent;
end;

procedure TAMQPChannel.Open;
var
  LPayload: TBytes;
  LReader: TAMQPReader;
begin
  LPayload := CallRpc(BuildChannelOpen);
  LReader := TAMQPReader.Create(LPayload);
  try
    ReadMethodHeader(LReader);
    DecodeChannelOpenOk(LReader);
  finally
    LReader.Free;
  end;
  FIsOpen := True;
end;

procedure TAMQPChannel.DeclareExchange(const ADeclare: TAMQPExchangeDeclare);
var
  LPayload: TBytes;
begin
  LPayload := BuildExchangeDeclare(ADeclare);
  if ADeclare.NoWait then
    FConnection.SendMethod(FChannelId, LPayload)
  else
    CallRpc(LPayload); // Declare-Ok (sem args, descartado)
  AddRecovery(LPayload, not ADeclare.NoWait);
end;

function TAMQPChannel.DeclareQueue(const ADeclare: TAMQPQueueDeclare): TAMQPQueueDeclareOk;
var
  LPayload: TBytes;
  LReader: TAMQPReader;
begin
  LPayload := BuildQueueDeclare(ADeclare);
  if ADeclare.NoWait then
  begin
    FConnection.SendMethod(FChannelId, LPayload);
    Result := Default(TAMQPQueueDeclareOk);
    Result.QueueName := ADeclare.QueueName;
  end
  else
  begin
    LReader := TAMQPReader.Create(CallRpc(LPayload));
    try
      ReadMethodHeader(LReader);
      Result := DecodeQueueDeclareOk(LReader);
    finally
      LReader.Free;
    end;
  end;
  AddRecovery(LPayload, not ADeclare.NoWait);
end;

procedure TAMQPChannel.BindQueue(const ABind: TAMQPQueueBind);
var
  LPayload: TBytes;
begin
  LPayload := BuildQueueBind(ABind);
  if ABind.NoWait then
    FConnection.SendMethod(FChannelId, LPayload)
  else
    CallRpc(LPayload); // Bind-Ok (sem args, descartado)
  AddRecovery(LPayload, not ABind.NoWait);
end;

procedure TAMQPChannel.Publish(const AExchange, ARoutingKey: string;
  const ABody: TBytes; const AProps: TAMQPBasicProperties; AMandatory: Boolean);
var
  LMaxBody, LOffset, LLen: Integer;
begin
  if FConnection.FNegotiated.FrameMax = 0 then
    LMaxBody := 131072 - 8
  else
    LMaxBody := Integer(FConnection.FNegotiated.FrameMax) - 8;
  if LMaxBody < 1 then
    LMaxBody := 1;

  // Os frames de uma mensagem (método + header + body) devem sair juntos, sem
  // que frames de outra thread se intercalem — por isso, um único lock.
  FConnection.FWriteLock.Enter;
  try
    FConnection.SendFrameNoLock(AMQP_FRAME_METHOD, FChannelId,
      BuildBasicPublish(AExchange, ARoutingKey, AMandatory, False));
    FConnection.SendFrameNoLock(AMQP_FRAME_HEADER, FChannelId,
      BuildContentHeader(UInt64(Length(ABody)), AProps));

    LOffset := 0;
    while LOffset < Length(ABody) do
    begin
      if (Length(ABody) - LOffset) < LMaxBody then
        LLen := Length(ABody) - LOffset
      else
        LLen := LMaxBody;
      FConnection.SendFrameNoLock(AMQP_FRAME_BODY, FChannelId,
        Copy(ABody, LOffset, LLen));
      Inc(LOffset, LLen);
    end;
  finally
    FConnection.FWriteLock.Leave;
  end;
end;

procedure TAMQPChannel.PublishText(const AExchange, ARoutingKey, AText: string;
  APersistent: Boolean);
var
  LProps: TAMQPBasicProperties;
begin
  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('text/plain');
  if APersistent then
    LProps.SetPersistent;
  Publish(AExchange, ARoutingKey, TEncoding.UTF8.GetBytes(AText), LProps);
end;

function TAMQPChannel.BasicGet(const AQueue: string; ANoAck: Boolean): TAMQPGetResult;
begin
  FRpcLock.Enter;
  try
    if FClosed then
      raise EAMQPChannel.Create('canal fechado');
    FRpcKind := rkNone;
    FRpcError := '';
    FRpcEvent.ResetEvent;
    FConnection.SendMethod(FChannelId, BuildBasicGet(AQueue, ANoAck));
    if FRpcEvent.WaitFor(AMQP_RPC_TIMEOUT_MS) <> wrSignaled then
      raise EAMQPChannel.Create('timeout aguardando resposta do servidor');
    case FRpcKind of
      rkMessage:
        Result := FRpcMessage;      // Get-Ok (Found=True veio da montagem)
      rkMethod:
        begin                       // Get-Empty
          Result := Default(TAMQPGetResult);
          Result.Found := False;
        end;
      rkError:
        raise EAMQPChannel.Create(FRpcError);
    else
      raise EAMQPChannel.Create('resposta de RPC inesperada');
    end;
  finally
    FRpcLock.Leave;
  end;
end;

procedure TAMQPChannel.Ack(ADeliveryTag: UInt64; AMultiple: Boolean);
begin
  FConnection.SendMethod(FChannelId, BuildBasicAck(ADeliveryTag, AMultiple));
end;

procedure TAMQPChannel.Nack(ADeliveryTag: UInt64; ARequeue, AMultiple: Boolean);
begin
  FConnection.SendMethod(FChannelId, BuildBasicNack(ADeliveryTag, AMultiple, ARequeue));
end;

procedure TAMQPChannel.Qos(APrefetchCount: Word; AGlobal: Boolean);
var
  LPayload: TBytes;
begin
  LPayload := BuildBasicQos(APrefetchCount, AGlobal);
  CallRpc(LPayload); // Qos-Ok (sem args)
  AddRecovery(LPayload, True);
end;

function TAMQPChannel.Consume(const AQueue: string;
  const ACallback: TAMQPConsumerCallback; ANoAck, AExclusive: Boolean): string;
var
  LTag: string;
  LConsume: TAMQPBasicConsume;
  LPayload: TBytes;
begin
  // Geramos o consumer-tag no cliente e registramos o callback ANTES de enviar,
  // para não perder deliveries que cheguem entre o Consume-Ok e o registro.
  LTag := Format('ctag-%d-%d', [FChannelId, TInterlocked.Increment(FConsumerCounter)]);
  FConsumersLock.Enter;
  try
    FConsumers.AddOrSetValue(LTag, ACallback);
  finally
    FConsumersLock.Leave;
  end;

  LConsume := TAMQPBasicConsume.Create(AQueue, LTag, ANoAck);
  LConsume.Exclusive := AExclusive;
  LPayload := BuildBasicConsume(LConsume);
  try
    CallRpc(LPayload); // Consume-Ok (devolve o mesmo tag)
  except
    FConsumersLock.Enter;
    try
      FConsumers.Remove(LTag);
    finally
      FConsumersLock.Leave;
    end;
    raise;
  end;
  // Grava para replay após reconexão (mesmo tag e callback).
  AddRecovery(LPayload, True, True, LTag, ACallback);
  Result := LTag;
end;

procedure TAMQPChannel.Cancel(const AConsumerTag: string);
begin
  // O cleanup local roda mesmo se CallRpc falhar (canal caiu / timeout): senão o
  // consumer ficaria em FRecovery e seria ressuscitado numa eventual reconexão.
  try
    CallRpc(BuildBasicCancel(AConsumerTag, False)); // Cancel-Ok
  finally
    FConsumersLock.Enter;
    try
      FConsumers.Remove(AConsumerTag);
    finally
      FConsumersLock.Leave;
    end;
    RemoveConsumerRecovery(AConsumerTag);
  end;
end;

procedure TAMQPChannel.DispatchDelivery(const ADeliver: TAMQPBasicDeliver;
  const AProps: TAMQPBasicProperties; const ABody: TBytes);
var
  LCallback: TAMQPConsumerCallback;
  LDelivery: TAMQPDelivery;
begin
  FConsumersLock.Enter;
  try
    if not FConsumers.TryGetValue(ADeliver.ConsumerTag, LCallback) then
      LCallback := nil;
  finally
    FConsumersLock.Leave;
  end;

  LDelivery := Default(TAMQPDelivery);
  LDelivery.ConsumerTag := ADeliver.ConsumerTag;
  LDelivery.DeliveryTag := ADeliver.DeliveryTag;
  LDelivery.Redelivered := ADeliver.Redelivered;
  LDelivery.Exchange := ADeliver.Exchange;
  LDelivery.RoutingKey := ADeliver.RoutingKey;
  LDelivery.Properties := AProps;
  LDelivery.Body := ABody;

  if not Assigned(LCallback) then
  begin
    // sem consumer (cancelado): libera eventuais Headers e descarta
    if LDelivery.Properties.Has(bpHeaders) and Assigned(LDelivery.Properties.Headers) then
      LDelivery.Properties.Headers.Free;
    Exit;
  end;

  // Despacha para o pool nativo; a thread de leitura NÃO roda o callback.
  TInterlocked.Increment(FInFlight);
  TTask.Run(
    procedure
    begin
      try
        LCallback(Self, LDelivery);
      finally
        if LDelivery.Properties.Has(bpHeaders) and Assigned(LDelivery.Properties.Headers) then
          LDelivery.Properties.Headers.Free;
        TInterlocked.Decrement(FInFlight);
      end;
    end);
end;

procedure TAMQPChannel.DrainInFlight;
begin
  // Espera SEM timeout: liberar o canal com um callback ainda em execução seria
  // use-after-free (o TTask capturou Self e ainda mexe em FInFlight/FConnection).
  // Um callback bem-comportado sempre termina — mesmo que faça IO de 5s+ (o caso
  // de uso alvo). Não chame Close de dentro do próprio callback (auto-espera).
  while TInterlocked.CompareExchange(FInFlight, 0, 0) > 0 do
    TThread.Sleep(10);
end;

procedure TAMQPChannel.Close(AReplyCode: Word; const AReplyText: string);
var
  LClose: TAMQPCloseInfo;
begin
  if FClosed or (not FIsOpen) then
    Exit;

  LClose.ReplyCode := AReplyCode;
  LClose.ReplyText := AReplyText;
  LClose.ClassId := 0;
  LClose.MethodId := 0;
  try
    CallRpc(BuildChannelClose(LClose)); // aguarda Channel.Close-Ok
  except
    // se falhar (conexão caiu etc.), segue fechando localmente
  end;
  FClosed := True;
  FIsOpen := False;
  // Após o Close-Ok o servidor não entrega mais; espera callbacks em voo
  // terminarem antes de o objeto poder ser liberado.
  DrainInFlight;
  FConnection.UnregisterChannel(FChannelId);
end;

{ TAMQPChannel — recepção (roda na thread de leitura) }

procedure TAMQPChannel.CompleteContent;
var
  LMsg: TAMQPGetResult;
begin
  FAsmState := asIdle;
  if FAsmMethodId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_OK) then
  begin
    LMsg := Default(TAMQPGetResult);
    LMsg.Found := True;
    LMsg.DeliveryTag := FAsmGetOk.DeliveryTag;
    LMsg.Redelivered := FAsmGetOk.Redelivered;
    LMsg.Exchange := FAsmGetOk.Exchange;
    LMsg.RoutingKey := FAsmGetOk.RoutingKey;
    LMsg.MessageCount := FAsmGetOk.MessageCount;
    LMsg.Properties := FAsmProps;
    LMsg.Body := FAsmBody;
    SignalMessage(LMsg);
  end
  else if FAsmMethodId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_DELIVER) then
    DispatchDelivery(FAsmDeliver, FAsmProps, FAsmBody)
  else
  begin
    // Basic.Return (publish mandatory não roteado) ou outro: descartamos o
    // conteúdo. Como ninguém assume a posse das Properties aqui, liberamos a
    // tabela de Headers se ela veio (senão vazaria a cada Return).
    if FAsmProps.Has(bpHeaders) and Assigned(FAsmProps.Headers) then
    begin
      FAsmProps.Headers.Free;
      FAsmProps.Headers := nil;
    end;
  end;
  FAsmBody := nil;
end;

procedure TAMQPChannel.HandleFrame(const AFrame: TAMQPFrame);
var
  LReader: TAMQPReader;
  LId: TAMQPMethodId;
  LClose: TAMQPCloseInfo;
  LHeader: TAMQPContentHeader;
  LTag: string;
begin
  case FAsmState of
    asHeader:
      begin
        if AFrame.FrameType <> AMQP_FRAME_HEADER then
          Exit; // frame fora de ordem: ignora (protocolo)
        LReader := TAMQPReader.Create(AFrame.Payload);
        try
          LHeader := DecodeContentHeader(LReader);
        finally
          LReader.Free;
        end;
        FAsmProps := LHeader.Properties;
        FAsmBody := nil;
        FAsmRemaining := LHeader.BodySize;
        if FAsmRemaining = 0 then
          CompleteContent
        else
          FAsmState := asBody;
        Exit;
      end;

    asBody:
      begin
        if AFrame.FrameType <> AMQP_FRAME_BODY then
          Exit;
        FAsmBody := FAsmBody + AFrame.Payload;
        if UInt64(Length(AFrame.Payload)) >= FAsmRemaining then
          FAsmRemaining := 0
        else
          Dec(FAsmRemaining, Length(AFrame.Payload));
        if FAsmRemaining = 0 then
          CompleteContent;
        Exit;
      end;
  end;

  // asIdle: espera um frame de método.
  if not AFrame.IsMethod then
    Exit;

  LReader := TAMQPReader.Create(AFrame.Payload);
  try
    LId := ReadMethodHeader(LReader);

    if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_OK) then
    begin
      FAsmMethodId := LId;
      FAsmGetOk := DecodeBasicGetOk(LReader);
      FAsmState := asHeader;
    end
    else if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_DELIVER) then
    begin
      FAsmMethodId := LId;
      FAsmDeliver := DecodeBasicDeliver(LReader);
      FAsmState := asHeader; // conteúdo vem nos próximos frames
    end
    else if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_RETURN) then
    begin
      // publish mandatory não roteado: guarda e drena o conteúdo (descartado).
      FAsmMethodId := LId;
      FAsmState := asHeader;
    end
    else if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_CANCEL) then
    begin
      // servidor cancelou o consumer (ex.: fila removida).
      LTag := DecodeBasicCancel(LReader);
      FConsumersLock.Enter;
      try
        FConsumers.Remove(LTag);
      finally
        FConsumersLock.Leave;
      end;
      RemoveConsumerRecovery(LTag);
    end
    else if LId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE) then
    begin
      LClose := DecodeChannelClose(LReader);
      FConnection.SendMethod(FChannelId, BuildChannelCloseOk);
      SignalError(Format('canal %d fechado pelo servidor: %d %s',
        [FChannelId, LClose.ReplyCode, LClose.ReplyText]));
    end
    else
    begin
      // resposta de RPC (Open-Ok, Declare-Ok, Bind-Ok, Get-Empty, Close-Ok, ...)
      SignalMethod(AFrame.Payload);
    end;
  finally
    LReader.Free;
  end;
end;

end.
