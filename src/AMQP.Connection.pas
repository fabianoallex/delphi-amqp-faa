unit AMQP.Connection;

{ Conexão AMQP 0-9-1 sobre socket TCP e canais (item 2 do roadmap).

  TAMQPConnection: abre o socket, faz o handshake (protocol-header, Start/
  Start-Ok, Tune/Tune-Ok, Open/Open-Ok) e cria canais.

  TAMQPChannel: sobre um canal (nº > 0) declara exchange/queue, faz bind, publica
  (Basic.Publish + content header + body frames) e busca mensagens (Basic.Get +
  Ack/Nack).

  Modelo de concorrência: por ora tudo é síncrono (request/response na thread do
  chamador). A thread de leitura dedicada com demultiplexação por canal e
  despacho de consumers vem nos itens 3/4.

  Usa System.Net.Socket.TSocket (RTL pura, sem VCL). Heartbeat (item 4) ainda
  não existe; use por período curto ou proponha Heartbeat=0 nos parâmetros. }

interface

uses
  System.SysUtils,
  System.Classes,
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

  { Canal de dados. Criado via TAMQPConnection.CreateChannel (que já o abre).
    O chamador é dono do canal e deve liberá-lo (Free). }
  TAMQPChannel = class
  private
    FConnection: TAMQPConnection;
    FChannelId: Word;
    FIsOpen: Boolean;
    procedure Open;
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

    procedure Close(AReplyCode: Word = 200; const AReplyText: string = 'OK');

    property ChannelId: Word read FChannelId;
    property IsOpen: Boolean read FIsOpen;
  end;

  TAMQPConnection = class
  private
    FParams: TAMQPConnectionParams;
    FSocket: TSocket;
    FStream: TAMQPSocketStream;
    FIsOpen: Boolean;
    FNegotiated: TAMQPConnectionTune;
    FNextChannel: Word;
    procedure SendFrame(AFrameType: Byte; AChannel: Word; const APayload: TBytes);
    procedure SendMethod(AChannel: Word; const APayload: TBytes);
    /// Próximo frame do socket, pulando heartbeats.
    function NextFrame: TAMQPFrame;
    /// Próximo frame de método no canal AExpectChannel; devolve reader após o
    /// cabeçalho e preenche AId. Trata Connection.Close (levanta EAMQPConnection)
    /// e Channel.Close (responde Close-Ok e levanta EAMQPChannel). Chamador libera.
    function NextMethodOn(AExpectChannel: Word; out AId: TAMQPMethodId): TAMQPReader;
    /// Como NextMethodOn, mas exige o método (classe/id) esperado.
    function ExpectMethod(AExpectChannel, AClassId, AMethodId: Word): TAMQPReader;
    /// Lê content header + body frames de um conteúdo no canal AExpectChannel.
    function ReadContent(AExpectChannel: Word;
      out AProps: TAMQPBasicProperties): TBytes;
    procedure Handshake;
  public
    constructor Create(const AParams: TAMQPConnectionParams);
    destructor Destroy; override;

    /// Conecta o socket e executa o handshake. Levanta EAMQPConnection em falha.
    procedure Open;
    /// Abre um novo canal (já aberto). O chamador é dono e deve liberá-lo.
    function CreateChannel: TAMQPChannel;
    /// Envia Connection.Close, aguarda Close-Ok e fecha o socket.
    procedure Close(AReplyCode: Word = 200; const AReplyText: string = 'Goodbye');

    property IsOpen: Boolean read FIsOpen;
    property NegotiatedTune: TAMQPConnectionTune read FNegotiated;
  end;

implementation

uses
  AMQP.Channel.Methods;

{ TAMQPGetResult }

function TAMQPGetResult.BodyAsText: string;
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
end;

{ TAMQPConnection }

constructor TAMQPConnection.Create(const AParams: TAMQPConnectionParams);
begin
  inherited Create;
  FParams := AParams;
end;

destructor TAMQPConnection.Destroy;
begin
  if FIsOpen then
    try
      Close;
    except
      // fechando de qualquer forma; ignora erro de rede no encerramento
    end;
  FStream.Free;
  if Assigned(FSocket) then
  begin
    try
      FSocket.Close;
    except
    end;
    FSocket.Free;
  end;
  inherited;
end;

procedure TAMQPConnection.SendFrame(AFrameType: Byte; AChannel: Word;
  const APayload: TBytes);
var
  LFrame: TAMQPFrame;
begin
  LFrame := TAMQPFrame.Create(AFrameType, AChannel, APayload);
  LFrame.WriteTo(FStream);
end;

procedure TAMQPConnection.SendMethod(AChannel: Word; const APayload: TBytes);
begin
  SendFrame(AMQP_FRAME_METHOD, AChannel, APayload);
end;

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
  LChanClose: TAMQPCloseInfo;
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
      FIsOpen := False;
      raise EAMQPConnection.CreateFmt('conexão fechada pelo servidor: %d %s',
        [LConnClose.ReplyCode, LConnClose.ReplyText]);
    end;

    if AId.Matches(AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE) then
    begin
      LChanClose := DecodeChannelClose(Result);
      SendMethod(LFrame.Channel, BuildChannelCloseOk); // educadamente
      raise EAMQPChannel.CreateFmt('canal %d fechado pelo servidor: %d %s',
        [LFrame.Channel, LChanClose.ReplyCode, LChanClose.ReplyText]);
    end;

    if LFrame.Channel <> AExpectChannel then
      raise EAMQPConnection.CreateFmt(
        'resposta em canal inesperado: %d (esperava %d)',
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

function TAMQPConnection.ReadContent(AExpectChannel: Word;
  out AProps: TAMQPBasicProperties): TBytes;
var
  LFrame: TAMQPFrame;
  LReader: TAMQPReader;
  LHeader: TAMQPContentHeader;
  LRemaining: UInt64;
begin
  // Content header (tipo 2)
  LFrame := NextFrame;
  if (LFrame.FrameType <> AMQP_FRAME_HEADER) or (LFrame.Channel <> AExpectChannel) then
    raise EAMQPConnection.CreateFmt(
      'esperava content header no canal %d (veio tipo %d, canal %d)',
      [AExpectChannel, LFrame.FrameType, LFrame.Channel]);

  LReader := TAMQPReader.Create(LFrame.Payload);
  try
    LHeader := DecodeContentHeader(LReader);
  finally
    LReader.Free;
  end;
  AProps := LHeader.Properties;

  // Body frames (tipo 3) até somar body-size.
  Result := nil;
  LRemaining := LHeader.BodySize;
  while LRemaining > 0 do
  begin
    LFrame := NextFrame;
    if (LFrame.FrameType <> AMQP_FRAME_BODY) or (LFrame.Channel <> AExpectChannel) then
      raise EAMQPConnection.CreateFmt(
        'esperava content body no canal %d (veio tipo %d, canal %d)',
        [AExpectChannel, LFrame.FrameType, LFrame.Channel]);
    Result := Result + LFrame.Payload;
    if UInt64(Length(LFrame.Payload)) >= LRemaining then
      LRemaining := 0
    else
      Dec(LRemaining, Length(LFrame.Payload));
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

  // Connection.Start
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

    LProps := DefaultClientProperties;
    try
      SendMethod(AMQP_CHANNEL_CONNECTION, BuildStartOk(LProps, AMQP_AUTH_PLAIN,
        PlainAuthResponse(FParams.User, FParams.Password), AMQP_LOCALE_DEFAULT));
    finally
      LProps.Free;
    end;
  finally
    LStart.ServerProperties.Free;
  end;

  // Connection.Tune
  LReader := ExpectMethod(AMQP_CHANNEL_CONNECTION, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE);
  try
    LServerTune := DecodeTune(LReader);
  finally
    LReader.Free;
  end;

  FNegotiated := NegotiateTune(LServerTune,
    FParams.ChannelMax, FParams.FrameMax, FParams.Heartbeat);
  SendMethod(AMQP_CHANNEL_CONNECTION, BuildTuneOk(FNegotiated));

  // Connection.Open
  SendMethod(AMQP_CHANNEL_CONNECTION, BuildOpen(FParams.VirtualHost));
  LReader := ExpectMethod(AMQP_CHANNEL_CONNECTION, AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN_OK);
  try
    DecodeOpenOk(LReader);
  finally
    LReader.Free;
  end;

  FIsOpen := True;
end;

procedure TAMQPConnection.Open;
begin
  if FIsOpen then
    raise EAMQPConnection.Create('conexão já está aberta');

  FSocket := TSocket.Create(TSocketType.TCP);
  FSocket.Connect(FParams.Host, '', '', FParams.Port);
  FStream := TAMQPSocketStream.Create(FSocket);

  Handshake;
end;

function TAMQPConnection.CreateChannel: TAMQPChannel;
begin
  if not FIsOpen then
    raise EAMQPConnection.Create('conexão não está aberta');
  Inc(FNextChannel);
  Result := TAMQPChannel.Create(Self, FNextChannel);
  try
    Result.Open;
  except
    Result.Free;
    raise;
  end;
end;

procedure TAMQPConnection.Close(AReplyCode: Word; const AReplyText: string);
var
  LClose: TAMQPConnectionClose;
  LReader: TAMQPReader;
  LId: TAMQPMethodId;
begin
  if not FIsOpen then
    Exit;
  FIsOpen := False;

  LClose.ReplyCode := AReplyCode;
  LClose.ReplyText := AReplyText;
  LClose.ClassId := 0;
  LClose.MethodId := 0;
  SendMethod(AMQP_CHANNEL_CONNECTION, BuildClose(LClose));

  // Aguarda Close-Ok (ignora o conteúdo).
  LReader := NextMethodOn(AMQP_CHANNEL_CONNECTION, LId);
  LReader.Free;

  FSocket.Close;
end;

{ TAMQPChannel }

constructor TAMQPChannel.Create(AConnection: TAMQPConnection; AChannelId: Word);
begin
  inherited Create;
  FConnection := AConnection;
  FChannelId := AChannelId;
end;

destructor TAMQPChannel.Destroy;
begin
  if FIsOpen and FConnection.IsOpen then
    try
      Close;
    except
      // ignora erro de rede ao encerrar
    end;
  inherited;
end;

procedure TAMQPChannel.Open;
var
  LReader: TAMQPReader;
begin
  FConnection.SendMethod(FChannelId, BuildChannelOpen);
  LReader := FConnection.ExpectMethod(FChannelId, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_OPEN_OK);
  try
    DecodeChannelOpenOk(LReader);
  finally
    LReader.Free;
  end;
  FIsOpen := True;
end;

procedure TAMQPChannel.DeclareExchange(const ADeclare: TAMQPExchangeDeclare);
var
  LReader: TAMQPReader;
begin
  FConnection.SendMethod(FChannelId, BuildExchangeDeclare(ADeclare));
  if ADeclare.NoWait then
    Exit;
  LReader := FConnection.ExpectMethod(FChannelId, AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_DECLARE_OK);
  try
    DecodeExchangeDeclareOk(LReader);
  finally
    LReader.Free;
  end;
end;

function TAMQPChannel.DeclareQueue(const ADeclare: TAMQPQueueDeclare): TAMQPQueueDeclareOk;
var
  LReader: TAMQPReader;
begin
  FConnection.SendMethod(FChannelId, BuildQueueDeclare(ADeclare));
  if ADeclare.NoWait then
  begin
    Result := Default(TAMQPQueueDeclareOk);
    Result.QueueName := ADeclare.QueueName;
    Exit;
  end;
  LReader := FConnection.ExpectMethod(FChannelId, AMQP_CLASS_QUEUE, AMQP_QUEUE_DECLARE_OK);
  try
    Result := DecodeQueueDeclareOk(LReader);
  finally
    LReader.Free;
  end;
end;

procedure TAMQPChannel.BindQueue(const ABind: TAMQPQueueBind);
var
  LReader: TAMQPReader;
begin
  FConnection.SendMethod(FChannelId, BuildQueueBind(ABind));
  if ABind.NoWait then
    Exit;
  LReader := FConnection.ExpectMethod(FChannelId, AMQP_CLASS_QUEUE, AMQP_QUEUE_BIND_OK);
  try
    DecodeQueueBindOk(LReader);
  finally
    LReader.Free;
  end;
end;

procedure TAMQPChannel.Publish(const AExchange, ARoutingKey: string;
  const ABody: TBytes; const AProps: TAMQPBasicProperties; AMandatory: Boolean);
var
  LMaxBody, LOffset, LLen: Integer;
begin
  // 1) frame de método Basic.Publish
  FConnection.SendMethod(FChannelId,
    BuildBasicPublish(AExchange, ARoutingKey, AMandatory, False));

  // 2) frame de content header
  FConnection.SendFrame(AMQP_FRAME_HEADER, FChannelId,
    BuildContentHeader(UInt64(Length(ABody)), AProps));

  // 3) frames de corpo, respeitando o frame-max negociado (7 header + 1 end = 8).
  if FConnection.FNegotiated.FrameMax = 0 then
    LMaxBody := 131072 - 8
  else
    LMaxBody := Integer(FConnection.FNegotiated.FrameMax) - 8;
  if LMaxBody < 1 then
    LMaxBody := 1;

  LOffset := 0;
  while LOffset < Length(ABody) do
  begin
    if (Length(ABody) - LOffset) < LMaxBody then
      LLen := Length(ABody) - LOffset
    else
      LLen := LMaxBody;
    FConnection.SendFrame(AMQP_FRAME_BODY, FChannelId, Copy(ABody, LOffset, LLen));
    Inc(LOffset, LLen);
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
var
  LReader: TAMQPReader;
  LId: TAMQPMethodId;
  LGetOk: TAMQPBasicGetOk;
begin
  Result := Default(TAMQPGetResult);
  FConnection.SendMethod(FChannelId, BuildBasicGet(AQueue, ANoAck));

  LReader := FConnection.NextMethodOn(FChannelId, LId);
  try
    if LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_EMPTY) then
    begin
      Result.Found := False;
      Exit;
    end;
    if not LId.Matches(AMQP_CLASS_BASIC, AMQP_BASIC_GET_OK) then
      raise EAMQPChannel.CreateFmt('esperava Get-Ok/Get-Empty, veio %d/%d',
        [LId.ClassId, LId.MethodId]);
    LGetOk := DecodeBasicGetOk(LReader);
  finally
    LReader.Free;
  end;

  Result.Found := True;
  Result.DeliveryTag := LGetOk.DeliveryTag;
  Result.Redelivered := LGetOk.Redelivered;
  Result.Exchange := LGetOk.Exchange;
  Result.RoutingKey := LGetOk.RoutingKey;
  Result.MessageCount := LGetOk.MessageCount;
  Result.Body := FConnection.ReadContent(FChannelId, Result.Properties);
end;

procedure TAMQPChannel.Ack(ADeliveryTag: UInt64; AMultiple: Boolean);
begin
  FConnection.SendMethod(FChannelId, BuildBasicAck(ADeliveryTag, AMultiple));
end;

procedure TAMQPChannel.Nack(ADeliveryTag: UInt64; ARequeue, AMultiple: Boolean);
begin
  FConnection.SendMethod(FChannelId, BuildBasicNack(ADeliveryTag, AMultiple, ARequeue));
end;

procedure TAMQPChannel.Close(AReplyCode: Word; const AReplyText: string);
var
  LClose: TAMQPCloseInfo;
  LReader: TAMQPReader;
begin
  if not FIsOpen then
    Exit;
  FIsOpen := False;

  LClose.ReplyCode := AReplyCode;
  LClose.ReplyText := AReplyText;
  LClose.ClassId := 0;
  LClose.MethodId := 0;
  FConnection.SendMethod(FChannelId, BuildChannelClose(LClose));

  LReader := FConnection.ExpectMethod(FChannelId, AMQP_CLASS_CHANNEL, AMQP_CHANNEL_CLOSE_OK);
  LReader.Free;
end;

end.
