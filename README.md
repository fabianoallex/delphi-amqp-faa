# delphi-amqp-faa

Cliente AMQP 0-9-1 para Delphi, implementado a partir da especificação pública do protocolo.

Status: **em desenvolvimento inicial** (WIP). Ainda não há release nem API estável.

## Uso pretendido

Biblioteca de uso geral, mas o primeiro caso de uso real e critério de aceite é uma integração PDV → autorizador → retaguarda (emissão de NFe), com consumo concorrente de mensagens via thread pool nativo.

## Licença

MIT — ver [LICENSE](LICENSE).
