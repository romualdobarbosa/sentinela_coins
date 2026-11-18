# 0001. Fonte: stream `@trade` da Binance, trade cru

**Status:** aceita (09/2026)

## Contexto

Eu precisava de uma fonte gratuita, sem autenticação e com volume suficiente pra justificar um Kafka. Com dado lento (uma API consultada de minuto em minuto, por exemplo), o Kafka vira enfeite.

## Decisão

Uso o stream `@trade` da Binance pelo endpoint só de market data (`wss://data-stream.binance.vision/stream`), com 5 pares. Escolhi trade cru em vez de kline pronto porque montar os candles é justamente a parte de transformação que eu quero mostrar.

## Consequências

- O WebSocket entrega na casa de 100 trades por segundo somando os 5 pares, o que dá trabalho de verdade pro Kafka.
- A agregação em candles fica comigo, no silver.
- Existe risco de geoblock regional. O plano B era Coinbase ou Kraken, mudando só o parse do producer. Não foi preciso.
