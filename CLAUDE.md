# delphi-amqp-faa — contexto do projeto

Cliente AMQP 0-9-1 para Delphi, escrito do zero, licença MIT desde o primeiro commit.

## Origem e motivação

Antes deste projeto, várias bibliotecas Delphi AMQP existentes foram avaliadas e testadas contra um broker RabbitMQ real (repositório de estudo separado, não referenciado aqui). Todas tinham problemas sérios: abandono de anos, bugs que impediam handshake com broker moderno, e — crucialmente — ausência de arquivo de licença (o que significa "todos os direitos reservados" por padrão), inviabilizando reuso seguro em algo que se pretende redistribuir ou abrir como OSS.

Decisão: em vez de forkar/corrigir uma dessas bibliotecas, escrever uma implementação nova e limpa.

## Regra inegociável: proveniência do código

**Nunca copiar, adaptar ou se basear em código-fonte de bibliotecas Delphi AMQP existentes** (incluindo qualquer fork/versão do "comotobo", `delphi-amqp` do felipecaputo, `rabbitmq-delphi` do sonadorje, ou qualquer outra lib Delphi AMQP sem licença permissiva clara). Direito autoral protege a expressão do código, não a "quantidade de mudança" — copiar e modificar não torna o código limpo.

Fontes de referência permitidas:
- A especificação AMQP 0-9-1 em si (protocolo/documento, não é copyrightable).
- Arquitetura/abordagem de clients já abertos sob licença permissiva (ex.: clients oficiais Java/.NET da RabbitMQ, Apache-2.0/MIT) — só como inspiração de design e para entender como o protocolo é tipicamente implementado, nunca traduzindo/copiando código deles linha a linha.

Se em algum momento for necessário consultar o código de uma lib sem licença clara para entender um comportamento, trate isso como **conhecimento de protocolo** (ex.: "descobri que X precisa ser codificado como Y"), nunca como fonte de código a transcrever.

## Lições de protocolo já aprendidas (conhecimento, não código)

Da fase de avaliação das libs existentes, ficou claro que estes pontos são fáceis de errar e vale testar com cuidado desde o início:

- **Escrita de frame em `TStream`**: o parâmetro `Buffer` de `TStream.Write` já é untyped/by-reference — não usar `@` antes da variável (ex.: `Stream.Write(Bytes[0], Length(Bytes))`, não `Stream.Write(@Bytes[0], ...)`). Um erro sutil aqui corrompe o frame de forma não óbvia.
- **Negociação de `channel-max` no handshake**: `Connection.Tune-Ok` precisa devolver um `channel-max` válido baseado no que o servidor propôs em `Connection.Tune` (nunca deixar em 0/"sem limite" nem propor valor maior que o do servidor). RabbitMQ 3.13+ rejeita conexão se o valor negociado exceder o máximo que o próprio broker aceita (2047, na configuração padrão) — brokers mais antigos podem não checar isso, então esse tipo de bug só aparece contra versão recente.
- **Mensagens de erro com `Format`/`CreateFmt`**: cuidado pra passar os valores inteiros certos como parâmetro, não os objetos que os encapsulam (fácil de confundir campo com valor).
- **Modelo de concorrência é uma decisão de arquitetura, não um detalhe**: o ideal é uma única thread de leitura fazendo demultiplexação de frames por canal, mas com despacho de callbacks de consumer para um thread pool (nativo, tipo `TTask.Run`) — nunca deixar o callback do usuário bloquear a thread de leitura. Isso deveria ser comportamento de fábrica da lib, não algo que o código consumidor precisa lembrar de fazer manualmente.
- **Heartbeat**: usar uma thread dedicada + `TEvent` (manual-reset) para o loop de espera interrompível, não `TTimer` (dependência de VCL/mensagens, indesejada numa lib que deve funcionar em console/serviço).

## Caso de uso alvo (critério de aceite do MVP)

Fluxo real: PDV → aplicação → autorizador (gera XML da NFe, envia à SEFAZ) → autorizador publica resposta em uma fila → retaguarda consome a fila, busca o XML (API ou banco) usando a chave da mensagem, e responde ao polling do PDV. Múltiplos PDVs podem estar aguardando resposta ao mesmo tempo — a retaguarda precisa processar essas respostas de forma concorrente.

Isso exige da lib: publish/consume confiável, ack manual (at-least-once), e um modelo de consumo que não serialize o processamento de mensagens diferentes.

Demonstrado em `samples/AutorizadorSim` + `samples/Retaguarda` (portados do repositório de estudo `delphi/amqp`, que usava o fork do comotobo — lá o despacho para thread pool era manual via `TTask.Run`; aqui `Channel.Consume` já despacha nativamente, então o callback do sample chama `ProcessarChave` direto).

## Roadmap (MVP, pensado para ~30 dias de dedicação)

1. ~~Framing binário + handshake de conexão (`AMQP0091` header, `Start/Start-Ok/Tune/Tune-Ok/Open/Open-Ok`), validado contra RabbitMQ real via Docker.~~ **Concluído.**
2. ~~Canais + encode/decode de métodos básicos (`exchange.*`, `queue.*`) + `Basic.Publish`.~~ **Concluído.**
3. ~~`Basic.Consume` + ack/nack manual + despacho de callback para thread pool.~~ **Concluído.**
4. ~~Heartbeat (thread própria) + reconexão + polimento/documentação.~~ **Concluído.** Passou por revisão (ultrareview) com correções aplicadas.
5. ~~Adapter para `delphi-api-infra-faa` (`Messaging.Adapters.DelphiAmqpFaa.pas`, implementa `IMessagingFactory`/`IMessageConsumer`/`IMessagePublisher`, registra-se como `'rabbitmq'`).~~ **Concluído.**

MVP completo. **Publisher confirms** (`confirm.select`) implementados: `ConfirmSelect`, `Publish` retorna seq-no, `OnConfirm` (thread pool) + `WaitForConfirm`/`WaitForConfirms`. `Basic.Return` já é tratado (evento `OnBasicReturn`). **TLS (amqps://)** implementado via SChannel nativo do Windows (`src/AMQP.Transport.Tls.pas`, `UseTls`/`TlsVerifyPeer`) — sem OpenSSL/DLLs externas, aditivo ao transporte plain. Próximos passos possíveis (não bloqueiam uso): transações (`tx.*` — descartado por decisão de design, ver README), infra Docker/teste automatizado de TLS, mTLS/client-cert, recuperação de topologia para filas com nome gerado pelo servidor, reenvio automático de publishes não confirmados após reconexão.

## Testes

- **Integração**: `docker/docker-compose.yml` sobe um RabbitMQ real (`rabbitmq:3-management`, guest/guest, portas 5672 e 15672) para validação ponta a ponta — mesma abordagem usada na fase de avaliação.
- **Unitário**: usar DUnitX para o que for testável sem broker (encode/decode de frames e métodos, por exemplo), seguindo a convenção já usada nos projetos irmãos (`delphi-api-infra-faa`).

## Plano B

Se este projeto não atingir o nível de robustez necessário dentro do prazo, a alternativa é usar o fork já corrigido de uma das libs avaliadas (documentado no repositório de estudo, fora deste projeto) — não é obrigatório fazer essa lib funcionar a qualquer custo.

## Convenções gerais

Mesmo padrão dos projetos irmãos (`delphi-api-infra-faa`, `delphi-api-starter`): licença MIT com copyright de Fabiano Arndt, commits em português, sem pushes/commits automáticos sem confirmação explícita do usuário.
