program AutorizadorSimVcl;

{ Versão VCL do sample AutorizadorSim: mesma simulação (publica N retornos de
  nota na fila "sefaz-respostas"), agora com os parâmetros de conexão
  editáveis na tela e um log visual, pra dar uma dinâmica melhor de testar
  junto com o RetaguardaVcl — basta abrir os dois executáveis lado a lado.
  Companheiro do sample RetaguardaVcl (fluxo PDV -> autorizador -> retaguarda). }

uses
  Vcl.Forms,
  uAutorizadorMain in 'uAutorizadorMain.pas' {frmAutorizador};

{$R *.res}

begin
  ReportMemoryLeaksOnShutdown := True;

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmAutorizador, frmAutorizador);
  Application.Run;
end.
