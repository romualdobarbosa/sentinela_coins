# 0006. DuckDB pra silver e gold, em vez de Athena

**Status:** aceita (09/2026)

## Contexto

Silver e gold são agregações sobre parquet. Na fase AWS, o bronze também ficou no S3.

## Decisão

DuckDB, local. Ele lê parquet direto e também lê `s3://` pelo `httpfs`.

## Consequências

- Custo zero, e é uma ferramenta que eu já domino.
- O mesmo código roda contra o bronze local ou no S3, trocando só o `BRONZE_PATH`.
- Ler milhares de arquivos pequenos do S3 é lento (cerca de 9 min contra 2 s local no bronze antigo). Ver a ADR 0008.
- O DuckDB não diferencia maiúscula de minúscula no nome de coluna, e o JSON da Binance tem `t` (trade id) e `T` (trade time). Isso gerou um bug real, contado no README.
