# 0002. Kafka com 3 brokers em KRaft

**Status:** aceita (09/2026)

## Contexto

Com 1 broker dá pra ter partição, mas não replicação. Eu queria mostrar replicação de verdade. Kafka gerenciado (MSK) não tem free tier e custaria mais de R$ 3.000 por mês, o que não faz sentido num projeto de portfólio.

## Decisão

Três brokers em modo KRaft (sem Zookeeper), rodando em containers locais via Podman. Replication factor 3 e `min.insync.replicas=2` (no `docker-compose.yml`), com o producer usando `acks=all`.

## Consequências

- Um broker pode cair sem perder escrita, porque ainda sobram 2 réplicas em sync.
- RF=3 exige os 3 brokers no ar pra criar o tópico.
- Custo zero, e montar o cluster na mão mostra mais do que clicar em "criar cluster".
