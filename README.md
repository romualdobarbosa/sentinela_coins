# Crypto Streaming Lakehouse

Pipeline de dados em streaming sobre trades de cripto (Binance), com Kafka (3 brokers,
replicação real) e arquitetura medallion. Dois consumers no mesmo tópico: um batch
(alimenta o lakehouse) e um live (speed layer do dashboard). Arquitetura Lambda.

## Arquitetura

```
Binance WS ──> producer.py ──> Kafka (crypto.trades, 6 part., RF=3)
                                   │
                    ┌──────────────┴───────────────┐
              consumer_batch.py               dashboard/live.py
                    │                               │
                bronze (parquet cru)          painel "ao vivo"
                    │
                silver (candles 1m, DuckDB)
                    │
                gold (métricas diárias, DuckDB)
                    │
                Streamlit (aba Análise)
```

- **Bronze**: trade cru da Binance, parquet particionado por `symbol/dt`. Imutável.
- **Silver**: candles OHLCV de 1min, tipado e agregado (DuckDB).
- **Gold**: métricas de negócio por símbolo/dia — VWAP, volume, variação %, amplitude.

## Stack

Python · confluent-kafka · websockets · Polars · DuckDB · Streamlit · Podman

## Rodando local

Pré: [Podman](https://podman.io/) (com o plugin `podman compose`, via `podman-compose`),
[uv](https://docs.astral.sh/uv/) e Python 3.11+.

```bash
uv sync            # cria o .venv e instala tudo (produção + dev), a partir do uv.lock
cp .env.example .env

make up            # 3 brokers + tópico + kafka-ui (http://localhost:8080)
```

Depois, em terminais separados:

```bash
make producer      # 1. ingestão: Binance -> Kafka
make batch         # 2. Kafka -> bronze (deixa rodando um tempo pra acumular dado)
make pipeline      # 3. bronze -> silver -> gold
make dashboard     # 4. Streamlit (aba Análise + aba Ao vivo)
```

Deixa o producer + batch rodando uns minutos antes do `make pipeline`, senão não tem
dado suficiente pra formar candles.

Parar tudo: `make down`. Se o Kafka bugar no boot: `make reset` (apaga o estado e sobe limpo).

## Fase AWS (provar uso da cloud)

Depois de tudo redondo local, aponta o bronze pro S3:

1. Cria um bucket S3 e credenciais (perfil/keys).
2. No `.env`: `BRONZE_PATH=s3://seu-bucket/bronze`.
3. `uv add s3fs boto3` (Polars grava em `s3://` via s3fs).
4. Roda o `make batch` por um período -> os parquet caem no S3.
5. Silver lê de lá com DuckDB: `INSTALL httpfs; LOAD httpfs;` e `read_parquet('s3://...')`.

Print do bucket + kafka-ui com as partições = evidência pro portfólio. Não precisa
deixar no ar; é vitrine, não produção.

## Desenvolvimento

```bash
uv run ruff check .          # lint
uv run ruff format .         # format
uv run pytest                # testes (lógica pura, não precisa do Kafka no ar)
pre-commit install           # roda ruff automaticamente a cada commit
```

CI no GitHub Actions roda ruff + pytest em cada PR.

## Notas

- 3 brokers KRaft (sem Zookeeper). RF=3 exige os 3 no ar.
- Tópico com 6 partições, chaveado por símbolo (mesmo símbolo -> mesma partição, ordem preservada).
- Consumers com `group.id` distintos -> offsets independentes; ler não apaga.
