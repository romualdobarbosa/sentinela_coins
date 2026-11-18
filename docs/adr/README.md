# Decisões de arquitetura (ADRs)

| # | Decisão |
|---|---|
| [0001](0001-binance-trade-cru.md) | Fonte: stream `@trade` da Binance, trade cru |
| [0002](0002-kafka-3-brokers-kraft.md) | Kafka com 3 brokers em KRaft, RF=3, min ISR=2 |
| [0003](0003-topico-chaveado-por-simbolo.md) | Tópico chaveado por símbolo |
| [0004](0004-dois-consumer-groups.md) | Dois consumers no mesmo tópico, com group.id diferentes |
| [0005](0005-medallion-mais-live.md) | Medallion além do caminho ao vivo |
| [0006](0006-duckdb-em-vez-de-athena.md) | DuckDB pra silver e gold, em vez de Athena |
| [0007](0007-streamlit-le-gold.md) | Streamlit lê só as camadas agregadas |
| [0008](0008-raw-antes-do-bronze.md) | Camada raw antes do bronze, com dedup na promoção |
