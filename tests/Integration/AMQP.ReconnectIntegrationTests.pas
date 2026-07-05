unit AMQP.ReconnectIntegrationTests;

{ Integração de reconexão automática — precisa de RabbitMQ em localhost:5672.
  Simula queda (fecha o socket), aguarda a auto-reconexão + recuperação de
  topologia (redeclara a fila e re-consome) e confere que o consumer volta a
  receber mensagens publicadas por uma conexão de controle separada. }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  System.SyncObjs,
  System.Generics.Collections,
  AMQP.Connection,
  AMQP.Queue.Methods;

type
  [TestFixture]
  TAMQPReconnectIntegrationTests = class
  private
    FConsumerConn: TAMQPConnection;
    FConsumerChan: TAMQPChannel;
    FControlConn: TAMQPConnection;
    FControlChan: TAMQPChannel;
    FReceived: TThreadList<string>;
    FReconnected: Integer;
    FQueue: string;
    function ReceivedContains(const AText: string): Boolean;
    procedure WaitReceived(const AText: string; ATimeoutMs: Integer);
    procedure WaitReconnected(ATimeoutMs: Integer);
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure Reconecta_E_ContinuaConsumindo;
  end;

implementation

procedure TAMQPReconnectIntegrationTests.Setup;
var
  LParams: TAMQPConnectionParams;
  LDecl: TAMQPQueueDeclare;
begin
  FReceived := TThreadList<string>.Create;
  FReconnected := 0;
  FQueue := 'test-recon-' + IntToStr(TThread.GetTickCount64);

  // Conexão de controle (sem auto-reconnect) só para publicar.
  FControlConn := TAMQPConnection.Create(TAMQPConnectionParams.Localhost);
  FControlConn.Open;
  FControlChan := FControlConn.CreateChannel;

  // Conexão consumidora com auto-reconexão.
  LParams := TAMQPConnectionParams.Localhost;
  LParams.AutoReconnect := True;
  LParams.ReconnectDelayMs := 500;
  LParams.ConnectionName := 'delphi-amqp-faa-recon-test';
  FConsumerConn := TAMQPConnection.Create(LParams);
  FConsumerConn.OnReconnect :=
    procedure(AConnection: TAMQPConnection)
    begin
      TInterlocked.Exchange(FReconnected, 1);
    end;
  FConsumerConn.Open;
  FConsumerChan := FConsumerConn.CreateChannel;

  // Fila nomeada, auto-delete (some quando o consumidor cai; a recuperação a
  // redeclara). Declarada no canal consumidor -> gravada para recuperação.
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.QueueName := FQueue;
  LDecl.AutoDelete := True;
  FConsumerChan.DeclareQueue(LDecl);

  FConsumerChan.Consume(FQueue,
    procedure(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery)
    begin
      FReceived.Add(ADelivery.BodyAsText);
      AChannel.Ack(ADelivery.DeliveryTag);
    end);
end;

procedure TAMQPReconnectIntegrationTests.TearDown;
begin
  FConsumerChan.Free; // cancela o consumidor -> fila auto-delete some
  FConsumerConn.Free;
  FControlChan.Free;
  FControlConn.Free;
  FReceived.Free;
end;

function TAMQPReconnectIntegrationTests.ReceivedContains(const AText: string): Boolean;
var
  LList: TList<string>;
begin
  LList := FReceived.LockList;
  try
    Result := LList.IndexOf(AText) >= 0;
  finally
    FReceived.UnlockList;
  end;
end;

procedure TAMQPReconnectIntegrationTests.WaitReceived(const AText: string;
  ATimeoutMs: Integer);
var
  LWaited: Integer;
begin
  LWaited := 0;
  while (not ReceivedContains(AText)) and (LWaited < ATimeoutMs) do
  begin
    TThread.Sleep(50);
    Inc(LWaited, 50);
  end;
end;

procedure TAMQPReconnectIntegrationTests.WaitReconnected(ATimeoutMs: Integer);
var
  LWaited: Integer;
begin
  LWaited := 0;
  while (TInterlocked.CompareExchange(FReconnected, 0, 0) = 0) and
        (LWaited < ATimeoutMs) do
  begin
    TThread.Sleep(50);
    Inc(LWaited, 50);
  end;
end;

procedure TAMQPReconnectIntegrationTests.Reconecta_E_ContinuaConsumindo;
begin
  // 1) Publica antes da queda e confirma o consumo.
  FControlChan.PublishText('', FQueue, 'antes-da-queda');
  WaitReceived('antes-da-queda', 5000);
  Assert.IsTrue(ReceivedContains('antes-da-queda'), 'deveria consumir antes da queda');

  // 2) Simula a queda de rede.
  FConsumerConn.DropConnectionForTest;

  // 3) Aguarda a auto-reconexão + recuperação (OnReconnect dispara após
  //    redeclarar a fila e re-consumir).
  WaitReconnected(15000);
  Assert.AreEqual(1, TInterlocked.CompareExchange(FReconnected, 0, 0),
    'deveria ter reconectado');
  Assert.IsTrue(FConsumerConn.IsOpen, 'conexão deveria estar aberta após reconectar');

  // 4) Publica depois da recuperação e confirma que o consumo voltou.
  FControlChan.PublishText('', FQueue, 'depois-da-recuperacao');
  WaitReceived('depois-da-recuperacao', 5000);
  Assert.IsTrue(ReceivedContains('depois-da-recuperacao'),
    'deveria voltar a consumir após a reconexão');
end;

initialization
  TDUnitX.RegisterTestFixture(TAMQPReconnectIntegrationTests);

end.
