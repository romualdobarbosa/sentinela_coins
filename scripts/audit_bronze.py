"""Audita o bronze: conta trades duplicados por (symbol, trade_id).

Feito pra medir o bronze histórico (rodada de 7 dias, 16/09 a 23/09/2026), que foi gravado
direto pelo consumer_batch antes de existir a camada raw + promote_bronze.py. Se der zero,
a rodada não duplicou; se der mais que zero, é o caso que o raw + dedup resolve.

Lê com Polars (case-sensitive). No DuckDB as colunas `t` (trade id) e `T` (trade time) do
JSON da Binance colidem, então aqui a chave é explicitamente `s` + `t`.

Uso: make audit-bronze
"""

from pathlib import Path

import polars as pl

from config import BRONZE_PATH


def audit(bronze_path: str) -> pl.DataFrame:
    """Uma linha por símbolo: trades, únicos, duplicados e janela de tempo."""
    lf = pl.scan_parquet(f"{bronze_path}/**/*.parquet", hive_partitioning=True)
    return (
        lf.group_by("s")
        .agg(
            pl.len().alias("trades"),
            pl.col("t").n_unique().alias("unicos"),
            pl.col("trade_time").min().alias("primeiro"),
            pl.col("trade_time").max().alias("ultimo"),
        )
        .with_columns((pl.col("trades") - pl.col("unicos")).alias("duplicados"))
        .sort("s")
        .collect()
    )


def main() -> None:
    n_files = sum(1 for _ in Path(BRONZE_PATH).rglob("*.parquet"))
    size_mb = sum(p.stat().st_size for p in Path(BRONZE_PATH).rglob("*.parquet")) / 1e6
    df = audit(BRONZE_PATH)

    trades = df["trades"].sum()
    dups = df["duplicados"].sum()
    inicio, fim = df["primeiro"].min(), df["ultimo"].max()
    segundos = (fim - inicio).total_seconds()

    with pl.Config(tbl_rows=-1, tbl_cols=-1, tbl_width_chars=120):
        print(df)
    print(f"arquivos parquet:  {n_files} ({size_mb:.1f} MB)")
    print(f"janela:            {inicio} -> {fim}")
    print(f"trades:            {trades}")
    print(f"duplicados:        {dups} ({dups / trades:.6%})")
    print(f"média:             {trades / segundos:.1f} trades/s (janela inteira, com as paradas)")


if __name__ == "__main__":
    main()
