unit uRetaguardaMain;

{ Tela única: campos de conexão editáveis + fila/prefetch, botão
  Conectar/Desconectar que declara a fila, seta o Qos e chama Channel.Consume.
  O callback do consumer roda no thread pool (despacho nativo da lib) e só
  toca a ListView/label via TThread.Queue — o dicionário FItems só é acessado
  a partir do procedimento marshalled na thread principal, então não precisa
  de lock próprio. Mesmo "processamento" simulado do sample console (Sleep
  aleatório fazendo o papel de busca do XML).

  Modo "Confirmação manual": em vez do Sleep+ack automático, a thread do
  consumer fica bloqueada num TEvent por mensagem (FPending, protegido por
  FPendingLock) até o usuário clicar Aceitar/Rejeitar na tela para a nota
  selecionada na ListView. Ao desconectar/fechar, CancelarPendencias libera
  qualquer thread ainda bloqueada (como nack+requeue) antes de cancelar o
  consumer e fechar o canal.

  FEncerrando fecha uma corrida do encerramento: o nack+requeue disparado por
  CancelarPendencias pode fazer o broker reentregar a mensagem ao consumer
  (ainda ativo até o Cancel-Ok), e esse callback novo estacionaria num TEvent
  que ninguém mais vai sinalizar — enquanto o Destroy do canal espera, na
  thread principal, todos os callbacks terminarem (deadlock: UI congelada).
  Com o flag (setado sob FPendingLock antes de acordar os eventos), qualquer
  entrega que chegue depois do início do encerramento sai sem ack/nack — o
  fechamento do canal devolve a mensagem à fila. }

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Classes,
  System.SyncObjs, System.Generics.Collections, Vcl.Graphics, Vcl.Controls,
  Vcl.Forms, Vcl.Dialogs, Vcl.StdCtrls, Vcl.ComCtrls,
  AMQP.Connection, AMQP.Queue.Methods;

type
  TPendingApproval = class
    Event: TEvent;
    Aceitar: Boolean;
    Requeue: Boolean;
  end;

  TfrmRetaguarda = class(TForm)
    gbConexao: TGroupBox;
    lblHost: TLabel;
    edtHost: TEdit;
    lblPort: TLabel;
    edtPort: TEdit;
    lblVHost: TLabel;
    edtVHost: TEdit;
    lblUser: TLabel;
    edtUser: TEdit;
    lblPassword: TLabel;
    edtPassword: TEdit;
    chkUseTls: TCheckBox;
    chkTlsVerifyPeer: TCheckBox;
    btnConectar: TButton;
    lblStatus: TLabel;
    gbConsumo: TGroupBox;
    lblQueue: TLabel;
    edtQueue: TEdit;
    lblPrefetch: TLabel;
    edtPrefetch: TEdit;
    chkManual: TCheckBox;
    gbAprovacao: TGroupBox;
    btnAceitar: TButton;
    btnRejeitar: TButton;
    chkRequeue: TCheckBox;
    lvNotas: TListView;
    lblContagem: TLabel;
    btnLimparLog: TButton;
    mmoLog: TMemo;
    Button1: TButton;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure FormDestroy(Sender: TObject);
    procedure btnConectarClick(Sender: TObject);
    procedure btnLimparLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
    procedure Button1Click(Sender: TObject);
    procedure btnAceitarClick(Sender: TObject);
    procedure btnRejeitarClick(Sender: TObject);
  private
    FConn: TAMQPConnection;
    FChannel: TAMQPChannel;
    FConsumerTag: string;
    FItems: TDictionary<string, TListItem>;
    FRecebidas: Integer;
    FProntas: Integer;
    FRejeitadas: Integer;
    FManualMode: Boolean;
    FPending: TDictionary<string, TPendingApproval>;
    FPendingLock: TObject;
    FEncerrando: Boolean; // protegido por FPendingLock
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConectado(AConectado: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure AtualizarContagem;
    procedure NotaRecebida(const AChave, AWorker: string);
    procedure NotaStatus(const AChave, AStatus: string);
    procedure ResolverSelecionada(AAceitar, ARequeue: Boolean);
    procedure CancelarPendencias;
  end;

var
  frmRetaguarda: TfrmRetaguarda;

implementation

{$R *.dfm}

procedure TfrmRetaguarda.FormCreate(Sender: TObject);
begin
  Randomize;
  FItems := TDictionary<string, TListItem>.Create;
  FPending := TDictionary<string, TPendingApproval>.Create;
  FPendingLock := TObject.Create;
  SetConectado(False);
end;

procedure TfrmRetaguarda.FormDestroy(Sender: TObject);
begin
  FPendingLock.Free;
  FPending.Free;
  FItems.Free;
end;

procedure TfrmRetaguarda.FormClose(Sender: TObject; var Action: TCloseAction);
begin
  CancelarPendencias;
  if Assigned(FChannel) and (FConsumerTag <> '') then
    try
      FChannel.Cancel(FConsumerTag);
    except
    end;
  FreeAndNil(FChannel);
  FreeAndNil(FConn);
end;

function TfrmRetaguarda.ScrollAtBottom(AHandle: HWND): Boolean;
var
  LInfo: TScrollInfo;
begin
  FillChar(LInfo, SizeOf(LInfo), 0);
  LInfo.cbSize := SizeOf(LInfo);
  LInfo.fMask := SIF_ALL;
  if not GetScrollInfo(AHandle, SB_VERT, LInfo) then
    Exit(True); // sem scrollbar ainda (conteúdo cabe todo) = considera "no fim"
  Result := (LInfo.nPos + Integer(LInfo.nPage)) >= LInfo.nMax;
end;

procedure TfrmRetaguarda.Log(const AMsg: string);
var
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(mmoLog.Handle);
  mmoLog.Lines.Add(FormatDateTime('hh:nn:ss', Now) + '  ' + AMsg);
  if LAtBottom then
    SendMessage(mmoLog.Handle, WM_VSCROLL, SB_BOTTOM, 0);
end;

procedure TfrmRetaguarda.btnLimparLogClick(Sender: TObject);
begin
  mmoLog.Clear;
end;

procedure TfrmRetaguarda.chkUseTlsClick(Sender: TObject);
begin
  chkTlsVerifyPeer.Enabled := chkUseTls.Checked;
  if chkUseTls.Checked then
    edtPort.Text := '5671'
  else
    edtPort.Text := '5672';
end;

function TfrmRetaguarda.BuildParams: TAMQPConnectionParams;
begin
  if chkUseTls.Checked then
    Result := TAMQPConnectionParams.LocalhostTls
  else
    Result := TAMQPConnectionParams.Localhost;
  Result.Host := Trim(edtHost.Text);
  Result.Port := StrToIntDef(Trim(edtPort.Text), Result.Port);
  Result.VirtualHost := edtVHost.Text;
  Result.User := edtUser.Text;
  Result.Password := edtPassword.Text;
  Result.UseTls := chkUseTls.Checked;
  Result.TlsVerifyPeer := chkTlsVerifyPeer.Checked;
end;

procedure TfrmRetaguarda.Button1Click(Sender: TObject);
begin
  lvNotas.Items.Clear;
end;

procedure TfrmRetaguarda.ResolverSelecionada(AAceitar, ARequeue: Boolean);
var
  LChave: string;
  LApproval: TPendingApproval;
begin
  if lvNotas.Selected = nil then
  begin
    Log('Selecione uma nota na lista antes de aceitar/rejeitar.');
    Exit;
  end;
  LChave := lvNotas.Selected.Caption;
  System.TMonitor.Enter(FPendingLock);
  try
    if not FPending.TryGetValue(LChave, LApproval) then
    begin
      Log('Nota "' + LChave + '" não está aguardando aprovação.');
      Exit;
    end;
    LApproval.Aceitar := AAceitar;
    LApproval.Requeue := ARequeue;
  finally
    System.TMonitor.Exit(FPendingLock);
  end;
  LApproval.Event.SetEvent;
end;

procedure TfrmRetaguarda.btnAceitarClick(Sender: TObject);
begin
  ResolverSelecionada(True, False);
end;

procedure TfrmRetaguarda.btnRejeitarClick(Sender: TObject);
begin
  ResolverSelecionada(False, chkRequeue.Checked);
end;

procedure TfrmRetaguarda.CancelarPendencias;
var
  LApproval: TPendingApproval;
begin
  System.TMonitor.Enter(FPendingLock);
  try
    FEncerrando := True; // entregas daqui em diante não estacionam no TEvent
    for LApproval in FPending.Values do
    begin
      LApproval.Aceitar := False;
      LApproval.Requeue := True;
      LApproval.Event.SetEvent;
    end;
  finally
    System.TMonitor.Exit(FPendingLock);
  end;
end;

procedure TfrmRetaguarda.SetConectado(AConectado: Boolean);
begin
  if AConectado then
  begin
    btnConectar.Caption := 'Desconectar';
    lblStatus.Caption := 'Conectado';
    lblStatus.Font.Color := clGreen;
  end
  else
  begin
    btnConectar.Caption := 'Conectar';
    lblStatus.Caption := 'Desconectado';
    lblStatus.Font.Color := clRed;
  end;
  edtHost.Enabled := not AConectado;
  edtPort.Enabled := not AConectado;
  edtVHost.Enabled := not AConectado;
  edtUser.Enabled := not AConectado;
  edtPassword.Enabled := not AConectado;
  chkUseTls.Enabled := not AConectado;
  chkTlsVerifyPeer.Enabled := (not AConectado) and chkUseTls.Checked;
  edtQueue.Enabled := not AConectado;
  edtPrefetch.Enabled := not AConectado;
  chkManual.Enabled := not AConectado;
  btnAceitar.Enabled := AConectado;
  btnRejeitar.Enabled := AConectado;
end;

procedure TfrmRetaguarda.AtualizarContagem;
begin
  lblContagem.Caption := Format('Recebidas: %d   |   Prontas: %d   |   Rejeitadas: %d',
    [FRecebidas, FProntas, FRejeitadas]);
end;

procedure TfrmRetaguarda.NotaRecebida(const AChave, AWorker: string);
var
  LItem: TListItem;
  LAtBottom: Boolean;
begin
  LAtBottom := ScrollAtBottom(lvNotas.Handle);
  LItem := lvNotas.Items.Add;
  LItem.Caption := AChave;
  LItem.SubItems.Add('Recebida');
  LItem.SubItems.Add(AWorker);
  LItem.SubItems.Add(FormatDateTime('hh:nn:ss', Now));
  LItem.SubItems.Add('');
  FItems.AddOrSetValue(AChave, LItem);
  Inc(FRecebidas);
  AtualizarContagem;
  if LAtBottom then
    LItem.MakeVisible(False);
end;

procedure TfrmRetaguarda.NotaStatus(const AChave, AStatus: string);
var
  LItem: TListItem;
begin
  if not FItems.TryGetValue(AChave, LItem) then
    Exit;
  LItem.SubItems[0] := AStatus;
  if AStatus = 'Pronta' then
  begin
    LItem.SubItems[3] := FormatDateTime('hh:nn:ss', Now);
    Inc(FProntas);
    AtualizarContagem;
  end
  else if (AStatus = 'Rejeitada') or (AStatus = 'Rejeitada (requeue)') then
  begin
    LItem.SubItems[3] := FormatDateTime('hh:nn:ss', Now);
    Inc(FRejeitadas);
    AtualizarContagem;
  end;
end;

procedure TfrmRetaguarda.btnConectarClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
  LPrefetch: Integer;
  LQueue: string;
  LMsg: string;
begin
  if Assigned(FConn) then
  begin
    try
      CancelarPendencias;
      if (FChannel <> nil) and (FConsumerTag <> '') then
        FChannel.Cancel(FConsumerTag);
      FConsumerTag := '';
      FreeAndNil(FChannel);
      FreeAndNil(FConn);
      Log('Desconectado.');
    except
      on E: Exception do
        Log('Erro ao desconectar: ' + E.Message);
    end;
    SetConectado(False);
    Exit;
  end;

  LParams := BuildParams;
  LQueue := Trim(edtQueue.Text);
  LPrefetch := StrToIntDef(Trim(edtPrefetch.Text), 10);
  try
    FConn := TAMQPConnection.Create(LParams);
    FConn.Open;

    FChannel := FConn.CreateChannel;
    FChannel.DeclareQueue(TAMQPQueueDeclare.Create(LQueue, True));
    FChannel.Qos(LPrefetch);
    FManualMode := chkManual.Checked;
    // Sem lock: nenhum callback vivo aqui (o canal anterior foi drenado no Free).
    FEncerrando := False;

    FConsumerTag := FChannel.Consume(
      LQueue,
      procedure(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery)
      var
        LChave, LWorker: string;
        LDelay: Integer;
        LApproval: TPendingApproval;
      begin
        LChave := ADelivery.BodyAsText;
        LWorker := Format('%d', [TThread.CurrentThread.ThreadID]);

        TThread.Queue(
          nil,
          procedure
          begin
            NotaRecebida(LChave, LWorker);
          end
        );

        if FManualMode then
        begin
          System.TMonitor.Enter(FPendingLock);
          try
            if FEncerrando then
              LApproval := nil // CancelarPendencias já passou; ninguém acordaria o TEvent
            else
            begin
              LApproval := TPendingApproval.Create;
              LApproval.Event := TEvent.Create(nil, True, False, '');
              FPending.Add(LChave, LApproval);
            end;
          finally
            System.TMonitor.Exit(FPendingLock);
          end;

          if LApproval = nil then
          begin
            // Sai sem ack/nack: o fechamento do canal devolve a mensagem à
            // fila. Nack aqui reiniciaria o ciclo de redelivery imediato.
            TThread.Queue(nil, procedure begin NotaStatus(LChave, 'Devolvida (desconexão)'); end);
            Exit;
          end;

          TThread.Queue(
            nil,
            procedure
            begin
              NotaStatus(LChave, 'Aguardando aprovação');
            end
          );

          LApproval.Event.WaitFor(INFINITE);

          System.TMonitor.Enter(FPendingLock);
          try
            FPending.Remove(LChave);
          finally
            System.TMonitor.Exit(FPendingLock);
          end;

          try
            if LApproval.Aceitar then
            begin
              AChannel.Ack(ADelivery.DeliveryTag);
              TThread.Queue(nil, procedure begin NotaStatus(LChave, 'Pronta'); end);
            end
            else
            begin
              AChannel.Nack(ADelivery.DeliveryTag, LApproval.Requeue);
              if LApproval.Requeue then
                TThread.Queue(nil, procedure begin NotaStatus(LChave, 'Rejeitada (requeue)'); end)
              else
                TThread.Queue(nil, procedure begin NotaStatus(LChave, 'Rejeitada'); end);
            end;
          finally
            LApproval.Event.Free;
            LApproval.Free;
          end;
          Exit;
        end;

        LDelay := 300 + Random(2200);
        TThread.Queue(
          nil,
          procedure
          begin
            NotaStatus(LChave, 'Processando');
          end
        );

        Sleep(LDelay);

        try
          TThread.Queue(
            nil,
            procedure
            begin
              NotaStatus(LChave, 'Pronta');
            end
          );

          AChannel.Ack(ADelivery.DeliveryTag);
        except
          AChannel.Nack(ADelivery.DeliveryTag, True);
          TThread.Queue(
            nil,
            procedure
            begin
              NotaStatus(LChave, 'Erro');
            end
          );
        end;
      end
    );

    LMsg := Format('Conectado a %s:%d%s. Consumindo "%s" (prefetch %d).',
      [LParams.Host, LParams.Port, LParams.VirtualHost, LQueue, LPrefetch]);
    if FManualMode then
      LMsg := LMsg + ' Confirmação manual ativada.';
    Log(LMsg);

    SetConectado(True);
  except
    on E: Exception do
    begin
      Log('Falha ao conectar: ' + E.Message);
      FreeAndNil(FChannel);
      FreeAndNil(FConn);
      SetConectado(False);
    end;
  end;
end;

end.
