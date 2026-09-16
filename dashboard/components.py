"""Componentes visuais reutilizáveis do dashboard (KPI cards e gráficos Plotly)."""

import plotly.graph_objects as go
import polars as pl
import streamlit as st
from plotly.subplots import make_subplots
from streamlit.delta_generator import DeltaGenerator
from theme import DOWN, UP, apply_dark_layout, format_price


def kpi_card(
    column: DeltaGenerator,
    symbol: str,
    price: float,
    change_pct: float | None,
    volume: float | None,
) -> None:
    with column:
        st.metric(
            symbol,
            format_price(price),
            delta=f"{change_pct:+.2f}%" if change_pct is not None else None,
        )
        if volume is not None:
            st.caption(f"vol: {volume:,.2f}")


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
