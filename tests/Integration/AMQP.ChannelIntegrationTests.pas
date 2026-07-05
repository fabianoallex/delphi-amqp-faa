unit AMQP.ChannelIntegrationTests;

{ Integração de canais/publish/get — precisa de RabbitMQ em localhost:5672.
  Sobe com: docker compose -f docker/docker-compose.yml up -d }

interface

uses
  DUnitX.TestFramework,
  System.SysUtils,
  System.Classes,
  AMQP.Connection,
  AMQP.Queue.Methods,
  AMQP.Basic.Methods;

type
  [TestFixture]
  TAMQPChannelIntegrationTests = class
  private
    FConn: TAMQPConnection;
    FChan: TAMQPChannel;
    function DeclareTempQueue: string;
    function GetWithRetry(const AQueue: string): TAMQPGetResult;
  public
    [Setup]    procedure Setup;
    [TearDown] procedure TearDown;

    [Test] procedure PublicaEBuscaComPropriedades;
    [Test] procedure PublishText_EBusca;
    [Test] procedure GetEmpty_QuandoFilaVazia;
    [Test] procedure DeclareQueue_RetornaNomeGerado;
  end;

implementation

procedure TAMQPChannelIntegrationTests.Setup;
begin
  FConn := TAMQPConnection.Create(TAMQPConnectionParams.Localhost);
  FConn.Open;
  FChan := FConn.CreateChannel;
end;

procedure TAMQPChannelIntegrationTests.TearDown;
begin
  FChan.Free;
  FConn.Free;
end;

function TAMQPChannelIntegrationTests.DeclareTempQueue: string;
var
  LDecl: TAMQPQueueDeclare;
begin
  LDecl := Default(TAMQPQueueDeclare);
  LDecl.Exclusive := True;   // some no fechamento da conexão
  LDecl.AutoDelete := True;
  Result := FChan.DeclareQueue(LDecl).QueueName; // '' => nome gerado pelo servidor
end;

function TAMQPChannelIntegrationTests.GetWithRetry(const AQueue: string): TAMQPGetResult;
var
  I: Integer;
begin
  // A entrega após publish é assíncrona; tenta por até ~0,5s.
  for I := 1 to 20 do
  begin
    Result := FChan.BasicGet(AQueue, True);
    if Result.Found then
      Exit;
    TThread.Sleep(25);
  end;
end;

procedure TAMQPChannelIntegrationTests.PublicaEBuscaComPropriedades;
var
  LQueue: string;
  LProps: TAMQPBasicProperties;
  LResult: TAMQPGetResult;
begin
  LQueue := DeclareTempQueue;

  LProps := TAMQPBasicProperties.Empty;
  LProps.SetContentType('application/json');
  LProps.SetPersistent;
  LProps.SetCorrelationId('corr-99');
  LProps.SetMessageId('msg-77');

  // Exchange padrão ('') roteia pela routing-key = nome da fila.
  FChan.Publish('', LQueue, TEncoding.UTF8.GetBytes('{"chave":"NFe123"}'), LProps);

  LResult := GetWithRetry(LQueue);
  Assert.IsTrue(LResult.Found, 'mensagem deveria ter sido entregue');
  Assert.AreEqual('{"chave":"NFe123"}', LResult.BodyAsText, 'corpo');
  Assert.AreEqual('application/json', LResult.Properties.ContentType, 'content-type');
  Assert.AreEqual('corr-99', LResult.Properties.CorrelationId, 'correlation-id');
  Assert.AreEqual('msg-77', LResult.Properties.MessageId, 'message-id');
  Assert.AreEqual(2, Integer(LResult.Properties.DeliveryMode), 'delivery-mode persistente');
end;

procedure TAMQPChannelIntegrationTests.PublishText_EBusca;
var
  LQueue: string;
  LResult: TAMQPGetResult;
begin
  LQueue := DeclareTempQueue;
  FChan.PublishText('', LQueue, 'olá mundo NFe');

  LResult := GetWithRetry(LQueue);
  Assert.IsTrue(LResult.Found);
  Assert.AreEqual('olá mundo NFe', LResult.BodyAsText);
  Assert.AreEqual('text/plain', LResult.Properties.ContentType);
end;

procedure TAMQPChannelIntegrationTests.GetEmpty_QuandoFilaVazia;
var
  LQueue: string;
  LResult: TAMQPGetResult;
begin
  LQueue := DeclareTempQueue;
  LResult := FChan.BasicGet(LQueue, True);
  Assert.IsFalse(LResult.Found, 'fila recém-criada deveria estar vazia');
end;

procedure TAMQPChannelIntegrationTests.DeclareQueue_RetornaNomeGerado;
var
  LQueue: string;
begin
  LQueue := DeclareTempQueue;
  Assert.IsTrue(LQueue <> '', 'servidor deveria gerar um nome de fila');
end;

initialization
  TDUnitX.RegisterTestFixture(TAMQPChannelIntegrationTests);

end.
