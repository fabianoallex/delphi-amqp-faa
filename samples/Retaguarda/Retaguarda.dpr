program Retaguarda;

{ Cenario: PDV -> autorizador -> retaguarda. O autorizador (sample
  AutorizadorSim) publica o retorno de cada nota numa fila; a retaguarda
  consome e "busca o XML" (aqui, simulado com um Sleep aleatorio), como se
  varios PDVs estivessem aguardando resposta ao mesmo tempo.

  Diferenca em relacao ao mesmo cenario com outras libs AMQP para Delphi:
  o Channel.Consume desta lib ja despacha cada entrega para o thread pool
  nativo (TTask, dentro de TAMQPChannel.DispatchDelivery) - o callback
  abaixo roda concorrente para mensagens diferentes sem nenhum TTask.Run
  manual, e a thread de leitura nunca fica bloqueada esperando
  ProcessarChave terminar. }

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils,
  System.SyncObjs,
  System.Classes,
  System.Generics.Collections,
  AMQP.Connection,
  AMQP.Queue.Methods;

const
  QUEUE_NAME = 'sefaz-respostas';

var
  NotasProntas: TDictionary<string, string>;
  Lock: TCriticalSection;

// Escreve no console protegido pelo mesmo lock do dicionario - varias
// threads do pool chamando Writeln ao mesmo tempo, sem isso, embaralham a
// saida (TCriticalSection do Delphi e reentrante na mesma thread).
procedure Log(const S: string);
begin
  Lock.Enter;
  try
    Writeln(S);
  finally
    Lock.Leave;
  end;
end;

// Simula a busca do XML (API ou banco). Ja roda numa thread do pool.
procedure ProcessarChave(const Chave: string);
var
  Xml: string;
  AtrasoMs: Integer;
begin
  AtrasoMs := 300 + Random(1200);
  Log(Format('[worker %d] iniciando busca do XML da nota %s (~%dms)',
    [TThread.CurrentThread.ThreadID, Chave, AtrasoMs]));
  Sleep(AtrasoMs);

  Xml := Format('<xml da nota %s>', [Chave]);

  Lock.Enter;
  try
    NotasProntas.AddOrSetValue(Chave, Xml);
  finally
    Lock.Leave;
  end;

  Log(Format('[worker %d] nota %s pronta', [TThread.CurrentThread.ThreadID, Chave]));
end;

procedure Main;
var
  LParams: TAMQPConnectionParams;
  LConn: TAMQPConnection;
  LChannel: TAMQPChannel;
  LConsumerTag: string;
  Linha: string;
  Par: TPair<string, string>;
begin
  NotasProntas := TDictionary<string, string>.Create;
  Lock := TCriticalSection.Create;
  try
    LParams := TAMQPConnectionParams.Localhost;
    LConn := TAMQPConnection.Create(LParams);
    try
      LConn.Open;
      LChannel := LConn.CreateChannel;
      try
        LChannel.DeclareQueue(TAMQPQueueDeclare.Create(QUEUE_NAME, True));
        LChannel.Qos(2); // prefetch: limita mensagens nao confirmadas em voo

        // ANoAck=False (padrao do Consume): so confirmamos apos processar -
        // garantia "pelo menos uma vez" de fabrica, sem passo extra.
        LConsumerTag := LChannel.Consume(QUEUE_NAME,
          procedure(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery)
          var
            Chave: string;
          begin
            Chave := ADelivery.BodyAsText;
            Log('[Retaguarda] retorno recebido da fila: ' + Chave);
            try
              ProcessarChave(Chave);
              AChannel.Ack(ADelivery.DeliveryTag);
            except
              AChannel.Nack(ADelivery.DeliveryTag, True); // requeue em erro
            end;
          end);

        Writeln('[*] Aguardando retornos na fila "', QUEUE_NAME, '".');
        Writeln('[*] Pressione ENTER a qualquer momento pra ver o status (ou digite "sair" + ENTER pra fechar).');

        repeat
          Readln(Linha);
          if SameText(Trim(Linha), 'sair') then
            Break;

          Lock.Enter;
          try
            Writeln('--- status atual ---');
            if NotasProntas.Count = 0 then
              Writeln('(nenhuma nota pronta ainda)')
            else
              for Par in NotasProntas do
                Writeln('  ', Par.Key, ' -> ', Par.Value);
          finally
            Lock.Leave;
          end;
        until False;

        LChannel.Cancel(LConsumerTag);
      finally
        LChannel.Free;
      end;
    finally
      LConn.Free;
    end;
  finally
    Lock.Free;
    NotasProntas.Free;
  end;
end;

begin
  Randomize;
  try
    Main;
  except
    on E: Exception do
      Writeln('Erro: ', E.Message);
  end;
end.
