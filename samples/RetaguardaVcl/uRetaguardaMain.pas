unit uRetaguardaMain;

{ Tela única: campos de conexão editáveis + fila/prefetch, botão
  Conectar/Desconectar que declara a fila, seta o Qos e chama Channel.Consume.
  O callback do consumer roda no thread pool (despacho nativo da lib) e só
  toca a ListView/label via TThread.Queue — o dicionário FItems só é acessado
  a partir do procedimento marshalled na thread principal, então não precisa
  de lock próprio. Mesmo "processamento" simulado do sample console (Sleep
  aleatório fazendo o papel de busca do XML). }

interface

uses
  Winapi.Windows, Winapi.Messages, System.SysUtils, System.Classes,
  System.Generics.Collections, Vcl.Graphics, Vcl.Controls, Vcl.Forms,
  Vcl.Dialogs, Vcl.StdCtrls, Vcl.ComCtrls,
  AMQP.Connection, AMQP.Queue.Methods;

type
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
    lvNotas: TListView;
    lblContagem: TLabel;
    btnLimparLog: TButton;
    mmoLog: TMemo;
    procedure FormCreate(Sender: TObject);
    procedure FormClose(Sender: TObject; var Action: TCloseAction);
    procedure FormDestroy(Sender: TObject);
    procedure btnConectarClick(Sender: TObject);
    procedure btnLimparLogClick(Sender: TObject);
    procedure chkUseTlsClick(Sender: TObject);
  private
    FConn: TAMQPConnection;
    FChannel: TAMQPChannel;
    FConsumerTag: string;
    FItems: TDictionary<string, TListItem>;
    FRecebidas: Integer;
    FProntas: Integer;
    function ScrollAtBottom(AHandle: HWND): Boolean;
    procedure Log(const AMsg: string);
    procedure SetConectado(AConectado: Boolean);
    function BuildParams: TAMQPConnectionParams;
    procedure AtualizarContagem;
    procedure NotaRecebida(const AChave, AWorker: string);
    procedure NotaStatus(const AChave, AStatus: string);
  end;

var
  frmRetaguarda: TfrmRetaguarda;

implementation

{$R *.dfm}

procedure TfrmRetaguarda.FormCreate(Sender: TObject);
begin
  Randomize;
  FItems := TDictionary<string, TListItem>.Create;
  SetConectado(False);
end;

procedure TfrmRetaguarda.FormDestroy(Sender: TObject);
begin
  FItems.Free;
end;

procedure TfrmRetaguarda.FormClose(Sender: TObject; var Action: TCloseAction);
begin
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
end;

procedure TfrmRetaguarda.AtualizarContagem;
begin
  lblContagem.Caption := Format('Recebidas: %d   |   Prontas: %d', [FRecebidas, FProntas]);
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
  end;
end;

procedure TfrmRetaguarda.btnConectarClick(Sender: TObject);
var
  LParams: TAMQPConnectionParams;
  LPrefetch: Integer;
  LQueue: string;
begin
  if Assigned(FConn) then
  begin
    try
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
    FConsumerTag := FChannel.Consume(LQueue,
      procedure(AChannel: TAMQPChannel; const ADelivery: TAMQPDelivery)
      var
        LChave, LWorker: string;
        LDelay: Integer;
      begin
        LChave := ADelivery.BodyAsText;
        LWorker := Format('%d', [TThread.CurrentThread.ThreadID]);
        TThread.Queue(nil,
          procedure
          begin
            NotaRecebida(LChave, LWorker);
          end);

        LDelay := 300 + Random(1200);
        TThread.Queue(nil,
          procedure
          begin
            NotaStatus(LChave, 'Processando');
          end);
        Sleep(LDelay);

        try
          TThread.Queue(nil,
            procedure
            begin
              NotaStatus(LChave, 'Pronta');
            end);
          AChannel.Ack(ADelivery.DeliveryTag);
        except
          AChannel.Nack(ADelivery.DeliveryTag, True);
          TThread.Queue(nil,
            procedure
            begin
              NotaStatus(LChave, 'Erro');
            end);
        end;
      end);
    Log(Format('Conectado a %s:%d%s. Consumindo "%s" (prefetch %d).',
      [LParams.Host, LParams.Port, LParams.VirtualHost, LQueue, LPrefetch]));
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
