# 0007. Streamlit lê só o gold na aba de análise

**Status:** aceita (09/2026)

## Contexto

O dashboard tem duas abas com fontes diferentes.

## Decisão

A aba Análise lê só o gold (e os candles do silver pro gráfico), nunca o bronze. A aba Ao vivo usa o consumer live da ADR 0004.

## Consequências

- A aba de análise abre rápido, porque o trabalho pesado já foi feito no batch.
- Pra publicar uma demo sem Kafka no ar, bastou congelar um snapshot em `dashboard/demo_data/` e ligar `DASHBOARD_MODE=demo`.
