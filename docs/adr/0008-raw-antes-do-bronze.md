# 0008. Camada raw antes do bronze, com dedup na promoção

**Status:** aceita (30/09/2026)

## Contexto

O `consumer_batch` grava o parquet e só depois commita o offset (at-least-once, de propósito). Se ele cair entre as duas coisas, o Kafka reentrega as mensagens e elas são gravadas de novo. Exactly-once na ingestão seria difícil e caro pra esse projeto.

Na rodada de 7 dias, que gravava direto no bronze, isso aconteceu de fato. Auditando o bronze histórico achei **169 trades duplicados** em 66.123.102, todos numa mesma janela na madrugada de 16/09.

## Decisão

O `consumer_batch` grava num raw local, que pode ter duplicata. O `promote_bronze.py` lê o raw, deduplica por `(symbol, trade_id)` e escreve 1 parquet com nome fixo (`data.parquet`) por partição `symbol=/dt=`. A Binance já manda um `trade_id` único por símbolo, então o dedup é exato, sem heurística de preço ou horário.

## Consequências

- A garantia de unicidade fica num ponto só. Bronze, silver e gold não precisam pensar em duplicata.
- A promoção é idempotente, porque rodar de novo sobrescreve o mesmo arquivo com o mesmo resultado.
- De quebra, os milhares de arquivos pequenos do raw viram 1 arquivo por partição.
- O bronze histórico de 7 dias continua no formato antigo, não foi reprocessado.
