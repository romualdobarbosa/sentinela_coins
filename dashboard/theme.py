"""Paleta e helpers de estilo compartilhados pelo dashboard (dark trading terminal)."""

import plotly.graph_objects as go
import streamlit as st

CARD_BG = "#161a25"
BORDER = "#262d3d"
UP = "#26a69a"
DOWN = "#ef5350"
TEXT = "#e6e9ef"


def format_price(value: float) -> str:
    """Precisão adaptativa por magnitude — evita string longa cortando no card."""
    if abs(value) >= 100:
        return f"{value:,.2f}"
    if abs(value) >= 1:
        return f"{value:,.4f}"
    return f"{value:,.6f}"


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
