object frmRetaguarda: TfrmRetaguarda
  Left = 0
  Top = 0
  Caption = 'Retaguarda (VCL)'
  ClientHeight = 640
  ClientWidth = 560
  Color = clBtnFace
  Font.Charset = DEFAULT_CHARSET
  Font.Color = clWindowText
  Font.Height = -12
  Font.Name = 'Segoe UI'
  Font.Style = []
  OnCreate = FormCreate
  OnClose = FormClose
  OnDestroy = FormDestroy
  Position = poScreenCenter
  TextHeight = 15
  object gbConexao: TGroupBox
    Left = 8
    Top = 8
    Width = 540
    Height = 170
    Caption = ' Conexão '
    TabOrder = 0
    object lblHost: TLabel
      Left = 16
      Top = 28
      Width = 29
      Height = 15
      Caption = 'Host:'
    end
    object lblPort: TLabel
      Left = 270
      Top = 28
      Width = 33
      Height = 15
      Caption = 'Porta:'
    end
    object lblVHost: TLabel
      Left = 16
      Top = 57
      Width = 39
      Height = 15
      Caption = 'VHost:'
    end
    object lblUser: TLabel
      Left = 270
      Top = 57
      Width = 45
      Height = 15
      Caption = 'Usuário:'
    end
    object lblPassword: TLabel
      Left = 16
      Top = 86
      Width = 34
      Height = 15
      Caption = 'Senha:'
    end
    object lblStatus: TLabel
      Left = 140
      Top = 145
      Width = 84
      Height = 15
      Caption = 'Desconectado'
      Font.Charset = DEFAULT_CHARSET
      Font.Color = clRed
      Font.Height = -12
      Font.Name = 'Segoe UI'
      Font.Style = [fsBold]
      ParentFont = False
    end
    object edtHost: TEdit
      Left = 90
      Top = 24
      Width = 150
      Height = 23
      TabOrder = 0
      Text = 'localhost'
    end
    object edtPort: TEdit
      Left = 320
      Top = 24
      Width = 60
      Height = 23
      TabOrder = 1
      Text = '5671'
    end
    object edtVHost: TEdit
      Left = 90
      Top = 53
      Width = 150
      Height = 23
      TabOrder = 2
      Text = '/'
    end
    object edtUser: TEdit
      Left = 330
      Top = 53
      Width = 130
      Height = 23
      TabOrder = 3
      Text = 'guest'
    end
    object edtPassword: TEdit
      Left = 90
      Top = 82
      Width = 150
      Height = 23
      PasswordChar = '*'
      TabOrder = 4
      Text = 'guest'
    end
    object chkUseTls: TCheckBox
      Left = 270
      Top = 85
      Width = 190
      Height = 17
      Caption = 'Usar TLS (amqps)'
      Checked = True
      State = cbChecked
      TabOrder = 5
      OnClick = chkUseTlsClick
    end
    object chkTlsVerifyPeer: TCheckBox
      Left = 270
      Top = 108
      Width = 190
      Height = 17
      Caption = 'Validar certificado do broker'
      TabOrder = 6
    end
    object btnConectar: TButton
      Left = 16
      Top = 140
      Width = 110
      Height = 25
      Caption = 'Conectar'
      TabOrder = 7
      OnClick = btnConectarClick
    end
  end
  object gbConsumo: TGroupBox
    Left = 8
    Top = 186
    Width = 540
    Height = 60
    Caption = ' Consumo '
    TabOrder = 1
    object lblQueue: TLabel
      Left = 16
      Top = 26
      Width = 24
      Height = 15
      Caption = 'Fila:'
    end
    object lblPrefetch: TLabel
      Left = 310
      Top = 26
      Width = 53
      Height = 15
      Caption = 'Prefetch:'
    end
    object edtQueue: TEdit
      Left = 90
      Top = 22
      Width = 200
      Height = 23
      TabOrder = 0
      Text = 'sefaz-respostas'
    end
    object edtPrefetch: TEdit
      Left = 390
      Top = 22
      Width = 60
      Height = 23
      TabOrder = 1
      Text = '10'
    end
  end
  object lvNotas: TListView
    Left = 8
    Top = 254
    Width = 540
    Height = 280
    Anchors = [akLeft, akTop, akRight, akBottom]
    Columns = <
      item
        Caption = 'Chave'
        Width = 190
      end
      item
        Caption = 'Status'
        Width = 90
      end
      item
        Caption = 'Worker'
        Width = 70
      end
      item
        Caption = 'Recebida'
        Width = 80
      end
      item
        Caption = 'Pronta'
        Width = 80
      end>
    GridLines = True
    ReadOnly = True
    RowSelect = True
    TabOrder = 2
    ViewStyle = vsReport
  end
  object lblContagem: TLabel
    Left = 8
    Top = 541
    Width = 149
    Height = 15
    Anchors = [akLeft, akBottom]
    Caption = 'Recebidas: 0   |   Prontas: 0'
  end
  object btnLimparLog: TButton
    Left = 452
    Top = 537
    Width = 96
    Height = 23
    Anchors = [akRight, akBottom]
    Caption = 'Limpar log'
    TabOrder = 3
    OnClick = btnLimparLogClick
  end
  object mmoLog: TMemo
    Left = 8
    Top = 565
    Width = 540
    Height = 67
    Anchors = [akLeft, akRight, akBottom]
    ReadOnly = True
    ScrollBars = ssVertical
    TabOrder = 4
  end
end
