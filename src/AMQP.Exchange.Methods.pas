unit AMQP.Exchange.Methods;

{ Métodos da classe Exchange (40): declaração e remoção de exchange. }

interface

uses
  System.SysUtils,
  AMQP.Protocol,
  AMQP.Wire,
  AMQP.Method;

const
  AMQP_EXCHANGE_TYPE_DIRECT = 'direct';
  AMQP_EXCHANGE_TYPE_FANOUT = 'fanout';
  AMQP_EXCHANGE_TYPE_TOPIC  = 'topic';
  AMQP_EXCHANGE_TYPE_HEADERS = 'headers';

type
  TAMQPExchangeDeclare = record
    ExchangeName: string;
    ExchangeType: string;
    Passive: Boolean;
    Durable: Boolean;
    AutoDelete: Boolean;
    Internal: Boolean;
    NoWait: Boolean;
    Arguments: TAMQPFieldTable; // pode ser nil (tabela vazia)
    /// Declaração padrão: exchange durável, não passiva, tipo 'direct'.
    class function Create(const AName: string;
      const AType: string = AMQP_EXCHANGE_TYPE_DIRECT;
      ADurable: Boolean = True): TAMQPExchangeDeclare; static;
  end;

  TAMQPExchangeDelete = record
    ExchangeName: string;
    IfUnused: Boolean;
    NoWait: Boolean;
  end;

function BuildExchangeDeclare(const ADeclare: TAMQPExchangeDeclare): TBytes;
procedure DecodeExchangeDeclareOk(const AReader: TAMQPReader);

function BuildExchangeDelete(const ADelete: TAMQPExchangeDelete): TBytes;
procedure DecodeExchangeDeleteOk(const AReader: TAMQPReader);

implementation

{ TAMQPExchangeDeclare }

class function TAMQPExchangeDeclare.Create(const AName, AType: string;
  ADurable: Boolean): TAMQPExchangeDeclare;
begin
  Result := Default(TAMQPExchangeDeclare);
  Result.ExchangeName := AName;
  Result.ExchangeType := AType;
  Result.Durable := ADurable;
end;

function BuildExchangeDeclare(const ADeclare: TAMQPExchangeDeclare): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_DECLARE);
  try
    W.WriteShortUInt(0); // reserved-1 (ticket)
    W.WriteShortStr(ADeclare.ExchangeName);
    W.WriteShortStr(ADeclare.ExchangeType);
    W.WriteBit(ADeclare.Passive);
    W.WriteBit(ADeclare.Durable);
    W.WriteBit(ADeclare.AutoDelete);
    W.WriteBit(ADeclare.Internal);
    W.WriteBit(ADeclare.NoWait);
    W.WriteFieldTable(ADeclare.Arguments);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

procedure DecodeExchangeDeclareOk(const AReader: TAMQPReader);
begin
  // sem argumentos
end;

function BuildExchangeDelete(const ADelete: TAMQPExchangeDelete): TBytes;
var
  W: TAMQPWriter;
begin
  W := BeginMethod(AMQP_CLASS_EXCHANGE, AMQP_EXCHANGE_DELETE);
  try
    W.WriteShortUInt(0); // reserved-1
    W.WriteShortStr(ADelete.ExchangeName);
    W.WriteBit(ADelete.IfUnused);
    W.WriteBit(ADelete.NoWait);
    Result := W.ToBytes;
  finally
    W.Free;
  end;
end;

procedure DecodeExchangeDeleteOk(const AReader: TAMQPReader);
begin
  // sem argumentos
end;

end.
