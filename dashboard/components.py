"""Componentes visuais reutilizáveis do dashboard (KPI cards e gráficos Plotly)."""

import plotly.graph_objects as go
import polars as pl
import streamlit as st
from plotly.subplots import make_subplots
from streamlit.delta_generator import DeltaGenerator
from theme import (
    BORDER,
    CARD_BG,
    DOWN,
    UP,
    apply_dark_layout,
    display_name,
    format_compact,
    format_price,
)


def kpi_card(
    column: DeltaGenerator,
    symbol: str,
    price: float,
    change_pct: float | None,
    volume: float | None,
) -> None:
    delta_color = UP if (change_pct or 0) >= 0 else DOWN
    delta_html = (
        f'<span style="color:{delta_color}; font-size:0.9rem;">{change_pct:+.2f}%</span>'
        if change_pct is not None
        else ""
    )
    volume_html = (
        f'<div style="font-size:0.75rem; opacity:0.6;">vol {format_compact(volume)}</div>'
        if volume is not None
        else ""
    )
    card_html = (
        f'<div style="background-color:{CARD_BG}; border:1px solid {BORDER}; '
        f'border-radius:8px; padding:12px 16px; text-align:center;">'
        f'<div style="font-size:0.85rem; opacity:0.7;">{display_name(symbol)}</div>'
        f'<div style="font-size:1.5rem;">{format_price(price)}</div>'
        f"{delta_html}"
        f"{volume_html}"
        f"</div>"
    )
    with column:
        st.markdown(card_html, unsafe_allow_html=True)


MIN_GAP_MINUTES = 10  # trecho flat menor que isso não vira quebra no eixo
VOLUME_CLIP_QUANTILE = 0.99


def _drop_ingestion_gaps(df: pl.DataFrame) -> tuple[pl.DataFrame, list[dict]]:
    """Remove candles flat (trades=0, vindos do forward-fill do silver) e devolve os
    rangebreaks que colapsam os trechos longos no eixo do tempo -- senão a ingestão
    parada vira uma linha reta no gráfico."""
    flat = df.filter(pl.col("trades") == 0)["minute"].to_list()
    if not flat or df.height < 2:
        return df, []
    step = df["minute"].diff().drop_nulls().min()
    runs, start, prev = [], flat[0], flat[0]
    for m in flat[1:]:
        if m - prev > step:
            runs.append((start, prev + step))
            start = m
        prev = m
    runs.append((start, prev + step))
    breaks = [
        {"bounds": [a, b]} for a, b in runs if (b - a).total_seconds() >= MIN_GAP_MINUTES * 60
    ]
    return df.filter(pl.col("trades") > 0), breaks


def price_chart(df: pl.DataFrame, symbol: str) -> go.Figure:
    """Candlestick + volume, eixos sincronizados, estilo trading terminal."""
    df, gap_breaks = _drop_ingestion_gaps(df)
    fig = make_subplots(
        rows=2,
        cols=1,
        shared_xaxes=True,
        row_heights=[0.75, 0.25],
        vertical_spacing=0.03,
    )
    fig.add_trace(
        go.Candlestick(
            x=df["minute"],
            open=df["open"],
            high=df["high"],
            low=df["low"],
            close=df["close"],
            increasing_line_color=UP,
            decreasing_line_color=DOWN,
            name=symbol,
            showlegend=False,
        ),
        row=1,
        col=1,
    )
    bar_colors = [UP if c >= o else DOWN for o, c in zip(df["open"], df["close"], strict=True)]
    fig.add_trace(
        go.Bar(
            x=df["minute"],
            y=df["volume"],
            # borda 1px na mesma cor: com ~10k barras sub-pixel o antialiasing as apaga
            marker={"color": bar_colors, "line": {"color": bar_colors, "width": 1}},
            showlegend=False,
        ),
        row=2,
        col=1,
    )
    fig.update_layout(xaxis_rangeslider_visible=False, xaxis2_rangeslider_visible=False)
    if gap_breaks:
        fig.update_xaxes(rangebreaks=gap_breaks)
    # um spike isolado (ex.: flash crash) achata o resto das barras: corta o eixo no
    # percentil 99; o hover continua mostrando o volume real da barra cortada.
    if not df.is_empty():
        fig.update_yaxes(range=[0, df["volume"].quantile(VOLUME_CLIP_QUANTILE) * 1.2], row=2, col=1)
    return apply_dark_layout(fig, height=480)


def sparkline(prices: list[float]) -> go.Figure:
    color = UP if len(prices) < 2 or prices[-1] >= prices[0] else DOWN
    fig = go.Figure(data=[go.Scatter(y=prices, mode="lines", line={"color": color, "width": 2})])
    fig.update_layout(
        showlegend=False,
        xaxis={"visible": False},
        yaxis={"visible": False},
    )
    return apply_dark_layout(fig, height=80)
