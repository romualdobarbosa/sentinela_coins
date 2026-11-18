# 0005. Medallion além do caminho ao vivo

**Status:** aceita (09/2026)

## Contexto

Dava pra streamar direto do Kafka pro dashboard. Isso resolve o agora, mas não dá histórico, reprocessamento nem consulta analítica rápida.

## Decisão

Os dois caminhos coexistem. O live vai do Kafka pro painel. O batch passa por raw, bronze, silver e gold.

- Bronze cru e imutável é a fonte da verdade e permite recalcular silver e gold sem reingerir.
- Gold pré-agregado deixa a aba de análise só fazendo SELECT.

## Consequências

Um caminho cobre a fraqueza do outro. O preço é manter dois fluxos de leitura do mesmo tópico.
