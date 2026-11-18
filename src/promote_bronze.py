"""Promove RAW -> BRONZE: dedup por (symbol, trade_id) e consolida em 1 parquet/partição.

- RAW é landing zone: consumer_batch pode gravar o mesmo trade duas vezes se cair entre
  o flush e o commit do offset (at-least-once). Aqui é o único lugar que garante que o
  bronze não tem duplicata -- silver/gold, depois disso, não precisam pensar nisso.
- Chave de dedup: (s, t) -- "s" é o símbolo, "t" é o trade_id que a própria Binance manda
  no stream @trade, único por símbolo. Não precisa de heurística por preço/qty/tempo.
- Nome de arquivo fixo por partição (não tem timestamp) e linhas ordenadas por trade_id --
  rodar de novo sobrescreve com o mesmo arquivo, então a promoção é idempotente mesmo sem
  limpar o raw.
- Lê tudo o raw acumulado a cada execução (sem incremental): simples e correto, custa só
  reprocessar volume que já processou antes -- tradeoff aceitável pro tamanho do projeto.
"""

from pathlib import Path

import polars as pl

from config import AWS_ACCESS_KEY_ID, AWS_REGION, AWS_SECRET_ACCESS_KEY, BRONZE_PATH, RAW_PATH


def promote() -> None:
    raw_files = list(Path(RAW_PATH).rglob("*.parquet"))
    if not raw_files:
        print(f"[promote] nada em {RAW_PATH}, nada a fazer")
        return

    df = pl.read_parquet(raw_files)
    before = df.height
    df = df.unique(subset=["s", "t"], keep="first")
    print(f"[promote] {before} trades no raw -> {df.height} únicos por (symbol, trade_id)")

    is_s3 = BRONZE_PATH.startswith("s3://")
    fs = None
    if is_s3:
        import s3fs

        fs = s3fs.S3FileSystem(
            key=AWS_ACCESS_KEY_ID,
            secret=AWS_SECRET_ACCESS_KEY,
            client_kwargs={"region_name": AWS_REGION},
        )

    for (symbol, dt), part in df.group_by(["s", "dt"]):
        # unique() não preserva ordem: sem o sort, as linhas saem embaralhadas, o parquet
        # comprime ~3x pior (t, E e trade_time deixam de ser quase sequenciais) e duas
        # promoções do mesmo raw gravam arquivos com ordem diferente.
        part = part.sort("t")
        rel = f"symbol={symbol}/dt={dt}/data.parquet"
        if is_s3:
            with fs.open(f"{BRONZE_PATH.rstrip('/')}/{rel}", "wb") as f:
                part.write_parquet(f)
        else:
            out_path = Path(BRONZE_PATH) / rel
            out_path.parent.mkdir(parents=True, exist_ok=True)
            part.write_parquet(out_path)

    print(f"[promote] bronze atualizado em {BRONZE_PATH}")


if __name__ == "__main__":
    promote()
