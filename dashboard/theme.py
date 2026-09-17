"""Paleta e helpers de estilo compartilhados pelo dashboard (dark trading terminal)."""

import plotly.graph_objects as go
import streamlit as st

CARD_BG = "#161513"
BORDER = "#3A342B"
UP = "#26a69a"
DOWN = "#ef5350"
TEXT = "#E4DBCA"

SYMBOL_NAMES = {
    "BTCUSDT": "Bitcoin",
    "ETHUSDT": "Ethereum",
    "SOLUSDT": "Solana",
    "BNBUSDT": "BNB",
    "XRPUSDT": "XRP",
}


def display_name(symbol: str) -> str:
    return SYMBOL_NAMES.get(symbol, symbol)


def format_price(value: float) -> str:
    """Precisão adaptativa por magnitude — evita string longa cortando no card."""
    if abs(value) >= 100:
        return f"{value:,.2f}"
    if abs(value) >= 1:
        return f"{value:,.4f}"
    return f"{value:,.6f}"


def format_compact(value: float) -> str:
    """Abrevia volumes grandes (K/M/B) — evita estourar a largura do card."""
    for suffix, threshold in (("B", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(value) >= threshold:
            return f"{value / threshold:,.2f}{suffix}"
    return f"{value:,.2f}"


def inject_css() -> None:
    st.markdown(
        f"""
        <style>
        div[data-testid="stMetric"] {{
            background-color: {CARD_BG};
            border: 1px solid {BORDER};
            border-radius: 8px;
            padding: 12px 16px;
        }}
        div[data-testid="stMetricValue"] {{
            font-size: 1.5rem;
        }}
        h1 {{
            text-align: center;
        }}
        </style>
        """,
        unsafe_allow_html=True,
    )


def apply_dark_layout(fig: go.Figure, *, height: int = 420) -> go.Figure:
    fig.update_layout(
        template="plotly_dark",
        paper_bgcolor=CARD_BG,
        plot_bgcolor=CARD_BG,
        font_color=TEXT,
        height=height,
        margin=dict(l=10, r=10, t=30, b=10),
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
    )
    fig.update_xaxes(gridcolor=BORDER)
    fig.update_yaxes(gridcolor=BORDER)
    return fig
