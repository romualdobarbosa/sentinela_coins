# 0007. Streamlit lê só as camadas agregadas

**Status:** aceita (09/2026)

## Contexto

O dashboard tem duas abas. A Visão Geral mostra o resumo do dia e um bloco ao vivo. A Análise mostra os candles e o histórico diário.

## Decisão

Tudo que é histórico vem das camadas agregadas (gold e, pros candles do gráfico, silver), nunca do bronze. O bloco ao vivo da Visão Geral usa o consumer live da ADR 0004.

## Consequências

- A aba de análise abre rápido, porque o trabalho pesado já foi feito no batch.
- Pra publicar uma demo sem Kafka no ar, bastou congelar um snapshot em `dashboard/demo_data/` e ligar `DASHBOARD_MODE=demo`.
