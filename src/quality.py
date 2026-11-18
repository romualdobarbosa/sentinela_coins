"""DATA QUALITY: checks sobre o silver e o gold já gravados.

Dois níveis:
- erro: regra que nunca pode quebrar (preço <= 0, low > high, chave duplicada...).
  Se algum falhar, o processo sai com código 1 e o `make pipeline` para.
- aviso: gap de ingestão. O silver preenche minutos sem trade com candle flat
  (forward-fill, trades = 0), então um buraco longo some do dado. Aqui ele volta a
  aparecer, com começo, fim e duração, mas não quebra o pipeline: o gap é real.

Uso: make dq   (também roda no fim do make pipeline)
"""

import sys
from dataclasses import dataclass

import duckdb

from config import GOLD_PATH, SILVER_PATH

# Minutos seguidos sem trade a partir dos quais conta como gap de ingestão. BTC e ETH têm
# trade todo segundo; 5 min parado é falha da ingestão, não mercado parado.
GAP_MIN_MINUTES = 5

# (nome, camada, WHERE que seleciona as linhas que violam a regra)
SILVER_RULES = [
    ("preco_positivo", "open <= 0 OR high <= 0 OR low <= 0 OR close <= 0"),
    ("low_menor_que_high", "low > high"),
    ("open_dentro_do_range", "open < low OR open > high"),
    ("close_dentro_do_range", "close < low OR close > high"),
    ("volume_nao_negativo", "volume < 0 OR quote_volume < 0 OR trades < 0"),
]
GOLD_RULES = [
    ("vwap_positivo", "vwap IS NULL OR vwap <= 0"),
    ("no_maximo_1440_candles_por_dia", "n_candles > 1440"),
]


@dataclass
class CheckResult:
    layer: str
    name: str
    violations: int


@dataclass
class Gap:
    symbol: str
    start: object
    end: object
    minutes: int


def _rule_checks(con, source: str, layer: str, rules) -> list[CheckResult]:
    return [
        CheckResult(
            layer,
            name,
            con.execute(f"SELECT count(*) FROM read_parquet('{source}') WHERE {where}").fetchone()[
                0
            ],
        )
        for name, where in rules
    ]


def _unique_check(con, source: str, layer: str, keys: str) -> CheckResult:
    dup = con.execute(
        f"SELECT count(*) - count(DISTINCT ({keys})) FROM read_parquet('{source}')"
    ).fetchone()[0]
    return CheckResult(layer, f"chave_unica({keys})", dup)


def run_checks(silver_path: str, gold_path: str) -> list[CheckResult]:
    con = duckdb.connect()
    silver = f"{silver_path}/candles_1m.parquet"
    gold = f"{gold_path}/daily_metrics.parquet"
    return [
        *_rule_checks(con, silver, "silver", SILVER_RULES),
        _unique_check(con, silver, "silver", "symbol, minute"),
        *_rule_checks(con, gold, "gold", GOLD_RULES),
        _unique_check(con, gold, "gold", "symbol, dt"),
    ]


def find_gaps(silver_path: str, min_minutes: int = GAP_MIN_MINUTES) -> list[Gap]:
    """Sequências de minutos consecutivos com trades = 0 (ilhas), por símbolo."""
    rows = duckdb.sql(
        f"""
        WITH empty AS (
            SELECT
                symbol,
                minute,
                -- minutos consecutivos dão o mesmo valor aqui (técnica de gaps and islands)
                minute - to_minutes(
                    CAST(row_number() OVER (PARTITION BY symbol ORDER BY minute) AS BIGINT)
                ) AS island
            FROM read_parquet('{silver_path}/candles_1m.parquet')
            WHERE trades = 0
        )
        SELECT symbol, min(minute) AS start, max(minute) AS "end", count(*) AS minutes
        FROM empty
        GROUP BY symbol, island
        HAVING count(*) >= {min_minutes}
        ORDER BY symbol, start
        """
    ).fetchall()
    return [Gap(*r) for r in rows]


def main() -> None:
    results = run_checks(SILVER_PATH, GOLD_PATH)
    for r in results:
        status = "ok  " if r.violations == 0 else "ERRO"
        print(f"[dq] {status} {r.layer:6} {r.name:40} {r.violations} violações")

    gaps = find_gaps(SILVER_PATH)
    for g in gaps:
        print(f"[dq] aviso gap {g.symbol} {g.start} -> {g.end} ({g.minutes} min sem trade)")
    print(f"[dq] {len(gaps)} gaps de ingestão com >= {GAP_MIN_MINUTES} min")

    failed = [r for r in results if r.violations]
    if failed:
        sys.exit(f"[dq] {len(failed)} check(s) de erro falharam")
    print("[dq] todos os checks de erro passaram")


if __name__ == "__main__":
    main()
