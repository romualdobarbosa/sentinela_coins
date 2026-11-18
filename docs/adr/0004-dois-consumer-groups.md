# 0004. Dois consumers no mesmo tópico, com group.id diferentes

**Status:** aceita (09/2026)

## Contexto

O mesmo fluxo de trades alimenta duas coisas com necessidades opostas. O lakehouse pode atrasar, mas não pode perder nada. O painel ao vivo só quer o agora.

## Decisão

Dois consumers, cada um no seu consumer group, com offsets independentes (no Kafka, ler não apaga a mensagem).

- `consumer_batch` (group `bronze-writer`) grava no raw e commita o offset só depois de gravar.
- O consumer live do dashboard usa group efêmero com `auto.offset.reset=latest` e não grava nada.

## Consequências

- É uma arquitetura Lambda, com speed layer e batch layer saindo do mesmo tópico.
- Sem o live, o projeto viraria um cron batendo em API. É ele que mostra visualmente que é streaming.
