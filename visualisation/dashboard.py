"""
dashboard.py
------------
Interactive Plotly Dash dashboard that reads from PostgreSQL
and visualises cryptocurrency price data.

Run with:
    python visualisation/dashboard.py

Then open: http://localhost:8050
"""

import os
import psycopg2
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output
from dotenv import load_dotenv

load_dotenv()

app = Dash(__name__, title="Crypto ETL Dashboard")


# ─── Database Helper ──────────────────────────────────────────────────────────

def fetch_from_db(query: str) -> pd.DataFrame:
    """Run a SQL query and return results as a DataFrame."""
    conn = psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "crypto_db"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "")
    )
    try:
        df = pd.read_sql_query(query, conn)
        return df
    finally:
        conn.close()


# ─── Layout ───────────────────────────────────────────────────────────────────

app.layout = html.Div([

    html.Div([
        html.H1("🪙 Crypto ETL Pipeline Dashboard", style={"margin": "0"}),
        html.P("Live data from CoinGecko → PostgreSQL pipeline",
               style={"color": "#888", "margin": "4px 0 0 0"}),
    ], style={"padding": "20px 30px", "borderBottom": "1px solid #eee"}),

    html.Div([

        # Coin selector
        html.Div([
            html.Label("Select Coin:", style={"fontWeight": "bold"}),
            dcc.Dropdown(id="coin-dropdown", placeholder="Select a coin...", clearable=False),
        ], style={"width": "280px"}),

        # Refresh interval
        dcc.Interval(id="interval", interval=60 * 1000, n_intervals=0),  # refresh every 60s

    ], style={"display": "flex", "alignItems": "center", "gap": "20px",
              "padding": "16px 30px", "background": "#f9f9f9"}),

    # KPI Cards Row
    html.Div(id="kpi-cards", style={"display": "flex", "gap": "16px", "padding": "20px 30px"}),

    # Charts Row
    html.Div([
        dcc.Graph(id="price-history-chart", style={"flex": 1}),
        dcc.Graph(id="price-change-bar", style={"flex": 1}),
    ], style={"display": "flex", "gap": "16px", "padding": "0 30px 20px"}),

    # Price Distribution
    html.Div([
        dcc.Graph(id="market-cap-pie", style={"flex": 1}),
        dcc.Graph(id="volume-chart", style={"flex": 1}),
    ], style={"display": "flex", "gap": "16px", "padding": "0 30px 30px"}),

], style={"fontFamily": "Inter, sans-serif", "background": "#fff", "minHeight": "100vh"})


# ─── Callbacks ────────────────────────────────────────────────────────────────

@app.callback(
    Output("coin-dropdown", "options"),
    Output("coin-dropdown", "value"),
    Input("interval", "n_intervals")
)
def populate_dropdown(_):
    df = fetch_from_db("SELECT DISTINCT coin_id, coin_name FROM crypto_prices ORDER BY coin_name;")
    options = [{"label": row["coin_name"], "value": row["coin_id"]} for _, row in df.iterrows()]
    default = options[0]["value"] if options else None
    return options, default


@app.callback(
    Output("kpi-cards", "children"),
    Input("coin-dropdown", "value"),
    Input("interval", "n_intervals")
)
def update_kpis(coin_id, _):
    if not coin_id:
        return []
    df = fetch_from_db(f"""
        SELECT * FROM latest_crypto_prices WHERE coin_id = '{coin_id}';
    """)
    if df.empty:
        return [html.P("No data available.")]

    row = df.iloc[0]
    change = row["price_change_percentage_24h"]
    change_color = "#16a34a" if change >= 0 else "#dc2626"

    def kpi_card(label, value, color="#111"):
        return html.Div([
            html.P(label, style={"margin": 0, "color": "#888", "fontSize": "13px"}),
            html.H3(value, style={"margin": "4px 0 0", "color": color}),
        ], style={"background": "#f4f4f4", "borderRadius": "10px",
                  "padding": "16px 20px", "minWidth": "160px"})

    return [
        kpi_card("Current Price", f"${row['current_price_usd']:,.2f}"),
        kpi_card("Market Cap", f"${row['market_cap']:,.0f}"),
        kpi_card("24h Change", f"{change:+.2f}%", color=change_color),
        kpi_card("Last Updated", str(row["fetched_at"])[:16]),
    ]


@app.callback(
    Output("price-history-chart", "figure"),
    Input("coin-dropdown", "value"),
    Input("interval", "n_intervals")
)
def price_history(coin_id, _):
    if not coin_id:
        return go.Figure()
    df = fetch_from_db(f"""
        SELECT fetched_at, current_price_usd
        FROM crypto_prices
        WHERE coin_id = '{coin_id}'
        ORDER BY fetched_at;
    """)
    fig = px.line(df, x="fetched_at", y="current_price_usd",
                  title=f"{coin_id.capitalize()} Price History",
                  labels={"fetched_at": "Time", "current_price_usd": "Price (USD)"})
    fig.update_traces(line_color="#6366f1", line_width=2)
    fig.update_layout(plot_bgcolor="#fff", paper_bgcolor="#fff")
    return fig


@app.callback(
    Output("price-change-bar", "figure"),
    Input("interval", "n_intervals")
)
def price_change_bar(_):
    df = fetch_from_db("SELECT coin_name, price_change_percentage_24h FROM latest_crypto_prices;")
    df = df.sort_values("price_change_percentage_24h")
    df["color"] = df["price_change_percentage_24h"].apply(lambda x: "#16a34a" if x >= 0 else "#dc2626")
    fig = px.bar(df, x="price_change_percentage_24h", y="coin_name",
                 orientation="h", title="24h Price Change (%)",
                 color="price_change_percentage_24h",
                 color_continuous_scale=["#dc2626", "#f9f9f9", "#16a34a"])
    fig.update_layout(plot_bgcolor="#fff", paper_bgcolor="#fff", coloraxis_showscale=False)
    return fig


@app.callback(
    Output("market-cap-pie", "figure"),
    Input("interval", "n_intervals")
)
def market_cap_pie(_):
    df = fetch_from_db("SELECT coin_name, market_cap FROM latest_crypto_prices;")
    fig = px.pie(df, names="coin_name", values="market_cap",
                 title="Market Cap Distribution",
                 color_discrete_sequence=px.colors.qualitative.Pastel)
    fig.update_layout(paper_bgcolor="#fff")
    return fig


@app.callback(
    Output("volume-chart", "figure"),
    Input("interval", "n_intervals")
)
def volume_chart(_):
    df = fetch_from_db("SELECT coin_name, total_volume FROM latest_crypto_prices ORDER BY total_volume DESC;")
    fig = px.bar(df, x="coin_name", y="total_volume", title="24h Trading Volume",
                 color="total_volume", color_continuous_scale="Blues")
    fig.update_layout(plot_bgcolor="#fff", paper_bgcolor="#fff", coloraxis_showscale=False)
    return fig


# ─── Run ─────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=8050)
