program RetaguardaVcl;

{ Versão VCL do sample Retaguarda: consome a fila "sefaz-respostas" e mostra
  o status de cada nota (Recebida -> Processando -> Pronta) numa lista ao
  vivo, em vez do log rolante do console — dá pra ver de relance quantas
  notas chegaram e quantas já foram processadas, com vários workers do
  thread pool rodando em paralelo (mesmo despacho nativo do Channel.Consume,
  sem TTask.Run manual). Companheiro do sample AutorizadorSimVcl (fluxo PDV
  -> autorizador -> retaguarda). }

uses
  Vcl.Forms,
  uRetaguardaMain in 'uRetaguardaMain.pas' {frmRetaguarda};

{$R *.res}

begin
  ReportMemoryLeaksOnShutdown := True;

  Application.Initialize;
  Application.MainFormOnTaskbar := True;
  Application.CreateForm(TfrmRetaguarda, frmRetaguarda);
  Application.Run;
end.
