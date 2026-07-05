unit AMQP.Connection;

{ Conexão AMQP 0-9-1 sobre socket TCP: abre o socket, executa o handshake
  (protocol-header, Start/Start-Ok, Tune/Tune-Ok, Open/Open-Ok) e depois pode
  ser fechada com Connection.Close/Close-Ok.

  Usa System.Net.Socket.TSocket (RTL pura, sem VCL — funciona em console/serviço)
  envolvido num TStream (TAMQPSocketStream) para reaproveitar a leitura/escrita
  de frames de AMQP.Frame.

  Todo o tráfego do handshake é no canal 0 (Connection.*). Canais de dados vêm
  depois no roadmap. Heartbeat (item 4) ainda não está implementado: se o
  servidor negociar um intervalo > 0, use a conexão por período curto ou
  proponha Heartbeat=0 nos parâmetros até a thread de heartbeat existir. }

interface

uses
  System.SysUtils,
  System.Classes,
  System.Net.Socket,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method,
  AMQP.Frame,
  AMQP.Connection.Methods;

type
  EAMQPConnection = class(Exception);

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

  TAMQPConnection = class
  private
    FParams: TAMQPConnectionParams;
    FSocket: TSocket;
    FStream: TAMQPSocketStream;
    FIsOpen: Boolean;
    FNegotiated: TAMQPConnectionTune;
    procedure SendMethod(const APayload: TBytes);
    /// Lê o próximo frame de método no canal 0 e devolve um reader posicionado
    /// após o cabeçalho; preenche AId. O chamador libera o reader.
    function NextMethod(out AId: TAMQPMethodId): TAMQPReader;
    /// Como NextMethod, mas exige o método esperado; se vier Connection.Close,
    /// levanta EAMQPConnection com o motivo do servidor.
    function ExpectMethod(AClassId, AMethodId: Word): TAMQPReader;
    procedure Handshake;
  public
    constructor Create(const AParams: TAMQPConnectionParams);
    destructor Destroy; override;

    /// Conecta o socket e executa o handshake. Levanta EAMQPConnection em falha.
    procedure Open;
    /// Envia Connection.Close, aguarda Close-Ok e fecha o socket.
    procedure Close(AReplyCode: Word = 200; const AReplyText: string = 'Goodbye');

    property IsOpen: Boolean read FIsOpen;
    /// Valores de tune negociados (channel-max, frame-max, heartbeat), válidos
    /// após Open.
    property NegotiatedTune: TAMQPConnectionTune read FNegotiated;
  end;

implementation

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

procedure TAMQPConnection.SendMethod(const APayload: TBytes);
var
  LFrame: TAMQPFrame;
begin
  LFrame := TAMQPFrame.Create(AMQP_FRAME_METHOD, AMQP_CHANNEL_CONNECTION, APayload);
  LFrame.WriteTo(FStream);
end;

function TAMQPConnection.NextMethod(out AId: TAMQPMethodId): TAMQPReader;
var
  LFrame: TAMQPFrame;
begin
  // Durante o handshake ignoramos heartbeats que por acaso cheguem.
  repeat
    LFrame := TAMQPFrame.ReadFrom(FStream);
  until not LFrame.IsHeartbeat;

  if not LFrame.IsMethod then
    raise EAMQPConnection.CreateFmt(
      'frame inesperado (tipo %d) durante o handshake', [LFrame.FrameType]);

  Result := TAMQPReader.Create(LFrame.Payload);
  try
    AId := ReadMethodHeader(Result);
  except
    Result.Free;
    raise;
  end;
end;

function TAMQPConnection.ExpectMethod(AClassId, AMethodId: Word): TAMQPReader;
var
  LId: TAMQPMethodId;
  LClose: TAMQPConnectionClose;
begin
  Result := NextMethod(LId);
  try
    if LId.Matches(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_CLOSE) then
    begin
      LClose := DecodeClose(Result);
      raise EAMQPConnection.CreateFmt(
        'servidor recusou a conexão: %d %s', [LClose.ReplyCode, LClose.ReplyText]);
    end;
    if not LId.Matches(AClassId, AMethodId) then
      raise EAMQPConnection.CreateFmt(
        'esperado método %d/%d, veio %d/%d',
        [AClassId, AMethodId, LId.ClassId, LId.MethodId]);
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

  // Connection.Start
  LReader := ExpectMethod(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_START);
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
      SendMethod(BuildStartOk(LProps, AMQP_AUTH_PLAIN,
        PlainAuthResponse(FParams.User, FParams.Password), AMQP_LOCALE_DEFAULT));
    finally
      LProps.Free;
    end;
  finally
    LStart.ServerProperties.Free;
  end;

  // Connection.Tune
  LReader := ExpectMethod(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_TUNE);
  try
    LServerTune := DecodeTune(LReader);
  finally
    LReader.Free;
  end;

  FNegotiated := NegotiateTune(LServerTune,
    FParams.ChannelMax, FParams.FrameMax, FParams.Heartbeat);
  SendMethod(BuildTuneOk(FNegotiated));

  // Connection.Open
  SendMethod(BuildOpen(FParams.VirtualHost));
  LReader := ExpectMethod(AMQP_CLASS_CONNECTION, AMQP_CONNECTION_OPEN_OK);
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
  SendMethod(BuildClose(LClose));

  // Aguarda Close-Ok (ignora o conteúdo).
  LReader := NextMethod(LId);
  LReader.Free;

  FSocket.Close;
end;

end.
