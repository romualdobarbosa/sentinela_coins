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


def price_chart(df: pl.DataFrame, symbol: str) -> go.Figure:
    """Candlestick + volume, eixos sincronizados, estilo trading terminal."""
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
        go.Bar(x=df["minute"], y=df["volume"], marker_color=bar_colors, showlegend=False),
        row=2,
        col=1,
    )
    fig.update_layout(xaxis_rangeslider_visible=False, xaxis2_rangeslider_visible=False)
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
