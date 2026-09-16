# CLAUDE.md — Contexto do projeto: Crypto Streaming Lakehouse

> Este arquivo é o briefing pro agente. Lê tudo antes de propor plano.
> É um projeto de **portfólio**, não produção. Objetivo duplo: (1) mostrar
> engenharia de dados de verdade (streaming + medallion) e (2) provar uso de AWS.

---

## 1. Quem sou eu (dono do projeto)

- Analista de dados evoluindo pra **data engineering / IA**.
- Base técnica sólida — **não sou iniciante**, pode ir direto ao ponto técnico.
- Stack que já domino e quero usar aqui: **Python, SQL, DuckDB, Polars**. Ambiente **Linux (Fedora)**.
- Estilo de trabalho: direto, casual, PT-BR. Faça perguntas quando ambíguo em vez de chutar.

## 2. O que é o projeto

Pipeline de dados em **streaming** sobre trades de criptomoedas da **Binance**,
usando **Kafka** como buffer central e **arquitetura medallion** (bronze/silver/gold),
terminando num **dashboard Streamlit**. Roda **local primeiro**; a AWS entra só no fim,
pra evidência.

## 3. Objetivo e restrições (LEIA — moldam todas as decisões)

- **É vitrine, não produção.** Não precisa de uptime, HA real, nem rodar pra sempre.
- **Custo alvo: ~R$ 0.** Todo o desenvolvimento é local (Docker no Fedora, de graça).
- **AWS só no final e por pouco tempo:** subir a camada **bronze pro S3** por um período,
  tirar print/deixar como evidência, e desligar. Nada fica no ar.
- **Nada de MSK.** Kafka gerenciado da AWS não tem free tier e custa R$ 3.000+/mês. Kafka
  roda em Docker local. Rodar Kafka na mão inclusive mostra mais skill que clicar em "criar MSK".
- Free tier atual da AWS (contas pós-15/07/2025) é **crédito de até US$ 200 / 6 meses**, não
  mais "12 meses grátis". Por isso: desenvolve local, só integra AWS quando estiver redondo,
  pra não queimar o relógio de 6 meses à toa.

## 4. Arquitetura e as decisões por trás dela

```
Binance WS ──> producer ──> Kafka (crypto.trades, 6 partições, RF=3)
                               │
                 ┌─────────────┴──────────────┐
           consumer_batch                 consumer_live (no dashboard)
                 │                              │
             BRONZE (parquet cru)         painel "ao vivo" (não grava)
                 │
             SILVER (candles 1m, DuckDB)
                 │
             GOLD (métricas diárias, DuckDB)
                 │
             Streamlit (aba Análise)
```

Isso é uma **arquitetura Lambda** (speed layer + batch layer saindo do mesmo tópico).

### Decisões tomadas (com o porquê)

1. **Fonte = Binance, stream `@trade` (trade cru).** Free, sem auth, WebSocket que jorra
   dado de verdade — justifica o Kafka (se fosse dado lento, Kafka seria enfeite). Trade cru
   em vez de kline pronto porque a agregação (candles) é justamente onde mostro skill.
   - Endpoint só market-data: `wss://data-stream.binance.vision/stream`.
   - **Risco: geoblock regional.** Se não conectar, plano B é Coinbase/Kraken (mesma arquitetura,
     muda só o parse do producer).

2. **Kafka com 3 brokers em KRaft (sem Zookeeper).** Escolhi 3 pra mostrar **replicação real
   (RF=3, min ISR=2)**. Com 1 broker só dava pra ter partição mas não replicação.

3. **Tópico chaveado por símbolo.** Mesmo símbolo → mesma partição → ordem preservada por par.
   6 partições.

4. **Dois consumers no mesmo tópico, com `group.id` diferentes** (offsets independentes; no Kafka,
   ler não apaga):
   - `consumer_batch`: alimenta o lakehouse. Batch, pode atrasar, tudo bem.
   - `consumer_live`: speed layer do dashboard, lê o "agora" (`auto.offset.reset=latest`,
     group efêmero), **não grava nada**.
   - Motivo de manter o live: sem ele, o projeto vira "cron batendo em API" e o destaque
     "streaming/tempo real" some. O live é o que prova visualmente que é streaming.

5. **Por que medallion e não streamar direto pro dashboard?**
   - Live vai direto do Kafka pro painel (resolve o "agora").
   - Mas só o live não dá **histórico**, **reprocessamento** nem **dashboard analítico rápido**.
   - Bronze cru imutável = fonte da verdade + permite recalcular silver/gold sem re-ingerir.
   - Gold pré-agregado = dashboard só faz SELECT, voa.
   - Ou seja: os dois caminhos coexistem, um cobre a fraqueza do outro.

6. **DuckDB pra silver/gold, não Athena.** Mais barato (R$ 0), já domino, lê parquet (e S3 via
   httpfs) direto. Athena fica como "sei usar também" se quiser mencionar.

7. **Streamlit só lê o GOLD** na aba analítica (batch). A aba "ao vivo" usa o consumer live.

### Camadas medallion

- **Bronze**: trade cru da Binance, parquet particionado `symbol=<>/dt=<>`. Imutável, zero transformação.
- **Silver**: candles OHLCV 1min por símbolo (tipado, agregado). DuckDB.
- **Gold**: métricas de negócio por símbolo/dia — VWAP, volume, variação %, amplitude. DuckDB.

## 5. Stack

Python 3.11+ · confluent-kafka · websockets · Polars · DuckDB · Streamlit · Docker Compose.
Fase AWS: S3 (bronze) + boto3/s3fs; DuckDB lê `s3://` via httpfs.

## 6. Estado atual do código

Já existe um **esqueleto funcional** (gerado, sintaxe validada, **mas não rodado de ponta a ponta**):

```
├── docker-compose.yml        # 3 brokers KRaft + init do tópico + kafka-ui (:8080)
├── Makefile                  # up/down/producer/batch/silver/gold/pipeline/dashboard
├── requirements.txt
├── .env.example
├── README.md
├── src/
│   ├── config.py             # tudo parametrizável por .env
│   ├── producer.py           # Binance WS -> Kafka (com reconexão)
│   ├── consumer_batch.py     # Kafka -> bronze (commit manual pós-gravação)
│   └── transform/
│       ├── silver.py         # bronze -> candles 1m
│       └── gold.py           # silver -> métricas diárias
└── dashboard/
    ├── app.py                # Streamlit: aba Análise (gold) + aba Ao vivo
    └── live.py               # consumer live (group próprio)
```

**Pontos que podem precisar de ajuste no primeiro run** (o agente deve verificar):
- Boot do Kafka: controller leva ~20-30s a eleger antes do tópico ser criado (`kafka-init` espera).
  Se bugar, `make reset` (down -v) e sobe limpo.
- API de `group_by` do Polars pode variar conforme versão — checar iteração em `consumer_batch.flush`.
- Geoblock da Binance (ver decisão 1).
- Imagens/tags do compose (cp-kafka 7.7.1, kafbat/kafka-ui) — validar que puxam.

## 7. Ordem de construção sugerida

1. `make up` → confirmar 3 brokers de pé + tópico criado (ver no kafka-ui).
2. `make producer` + `make batch` → confirmar parquet caindo no bronze.
3. `make pipeline` (silver + gold) → confirmar candles e métricas.
4. `make dashboard` → validar aba Análise e aba Ao vivo.
5. Só depois de tudo redondo local: **fase AWS** — trocar `BRONZE_PATH` por `s3://...`,
   rodar o batch por um período, guardar evidência, desligar.

## 8. Como quero trabalhar com você (agente)

- **Entre em plan mode primeiro.** Proponha o plano, valide comigo antes de sair codando.
- Priorize **fazer rodar de ponta a ponta local** antes de qualquer refino.
- Respostas diretas, PT-BR, pode assumir base técnica sólida.
- Quando algo for ambíguo (versão de lib, escolha de config), **pergunte**, não chute.
- Não adicione complexidade "de produção" (auth, k8s, CI pesado) sem eu pedir — é portfólio.
