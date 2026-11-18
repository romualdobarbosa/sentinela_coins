# 0003. Tópico chaveado por símbolo

**Status:** aceita (09/2026)

## Contexto

O Kafka só garante ordem dentro de uma partição. Os candles precisam dos trades de cada par em ordem.

## Decisão

O tópico `crypto.trades` tem 6 partições e a chave da mensagem é o símbolo. Mesmo símbolo sempre vai pra mesma partição.

## Consequências

- A ordem fica preservada por par, que é o que importa.
- Com 5 pares e 6 partições, a distribuição não é uniforme (uma partição pode ficar vazia, outra pode receber dois pares). Pra esse volume, tanto faz.
