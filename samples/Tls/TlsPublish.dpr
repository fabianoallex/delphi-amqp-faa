program TlsPublish;

{ Sample: conexão AMQP sobre TLS (amqps://) usando o SChannel nativo do Windows.

  Requer um RabbitMQ com listener TLS na porta 5671. Para subir um broker de dev
  (a partir da pasta docker/, após gerar os certificados — ver
  docker/docker-compose.tls.yml):

    docker compose -f docker-compose.yml -f docker-compose.tls.yml up -d

  LocalhostTls usa TlsVerifyPeer=False, aceitando o certificado self-signed do
  broker de dev. Em produção, prefira UseTls=True com TlsVerifyPeer=True (padrão),
  que valida o certificado pela cadeia de confiança do Windows. }

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  AMQP.Connection,
  AMQP.Queue.Methods;

const
  QUEUE_NAME = 'amqps-teste';

procedure Main;
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
  LChannel: TAMQPChannel;
  LMsg: TAMQPGetResult;
begin
  LParams := TAMQPConnectionParams.LocalhostTls; // 5671, TLS, validação off (dev)
  // Produção (validação ligada):
  //   LParams := TAMQPConnectionParams.Localhost;
  //   LParams.Host := 'broker.exemplo.com';
  //   LParams.Port := 5671;
  //   LParams.UseTls := True;   // TlsVerifyPeer=True por padrão

  LConn := TAMQPConnection.Create(LParams);
  try
    LConn.Open; // handshake TLS seguido do handshake AMQP
    Writeln('Conectado sobre TLS.');

    LChannel := LConn.CreateChannel;
    try
      LChannel.DeclareQueue(TAMQPQueueDeclare.Create(QUEUE_NAME, True));
      LChannel.PublishText('', QUEUE_NAME, 'olá sobre TLS');
      Writeln('Publicado em ', QUEUE_NAME, '.');

      // Busca a própria mensagem de volta, só para provar o round-trip cifrado.
      LMsg := LChannel.BasicGet(QUEUE_NAME, True {no-ack});
      if LMsg.Found then
        Writeln('Recebido de volta: ', LMsg.BodyAsText)
      else
        Writeln('Fila vazia (nada recebido).');
    finally
      LChannel.Free;
    end;
  finally
    LConn.Free;
  end;
end;

begin
  try
    Main;
  except
    on E: Exception do
      Writeln('Erro: ', E.ClassName, ': ', E.Message);
  end;
end.
