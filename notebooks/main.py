# /// script
# requires-python = ">=3.13"
# dependencies = [
#     "marimo",
#     "numpy==2.3.3",
#     "pandas==2.3.3",
#     "yfinance==0.2.66",
# ]
# ///

import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _(mo):
    import sqlite3
    import pandas as pd
    from enums import AssetCategory 

    # Verbindung zur SQLite-Datenbank
    conn = sqlite3.connect("../db/depot.db")
    cat = AssetCategory.from_str("stk") # 0

    # Trades einlesen
    df = pd.read_sql_query("""
        SELECT Description, Symbol, Quantity, TradePrice, ReportDate, TradeDateTime,
               NetCash, FxRateToBase, Euro, IbCommission, OpenCloseIndicator, BuySell, AssetCategory
        FROM Trades
        WHERE AssetCategory = 0
    """, conn)

    # Datumsspalte in datetime umwandeln
    df["TradeDateTime"] = pd.to_datetime(df["TradeDateTime"])

    # Sortieren nach Datum
    df = df.sort_values("TradeDateTime", ascending=[False])

    # Liste der eindeutigen Trade-Daten (als Strings für Auswahl)
    trade_dates = [
        str(d)
        for d in df["TradeDateTime"].dt.strftime("%Y-%m-%d %H:%M:%S").dropna().tolist()
    ]

    # UI-Element für Datumsauswahl
    selected_date = mo.ui.dropdown.from_series(df["TradeDateTime"].sort_values(ascending=[False]), label="Zeitpunkt: ")
    return df, pd, selected_date


@app.cell
def _(mo, selected_date, show_depot):
    result = show_depot()
    mo.vstack(
        [
            mo.md("## Historische Depot Analyse"),
            selected_date,
            mo.md("### Depot zu diesem Zeitpunkt"),
            result["Depot"],
            mo.md("### Trades an diesem Zeitpunkt"),
            result["Trades"],
        ]
    )
    return


@app.cell
def _(df, pd, selected_date):
    def show_depot():
        # Gewähltes Datum als Timestamp
        cutoff = pd.to_datetime(selected_date.value)
        if cutoff == None:
            return {"Depot": None, "Trades": None}
        # Alle Trades bis inkl. gewähltes Datum
        df_up_to = df[df["TradeDateTime"] <= cutoff]

        # Depot berechnen
        depot = df_up_to.groupby(["Symbol"], as_index=False).agg(
            {"Quantity": "sum", "NetCash": "sum", "Euro": "sum"}
        )
        depot = depot[depot["Quantity"] != 0]

        # Trades, die genau zum ausgewählten Zeitpunkt stattfanden
        trades_at_cutoff = df[df["TradeDateTime"] == cutoff]

        return {"Depot": depot, "Trades": trades_at_cutoff}
    return (show_depot,)


@app.cell
def _(mo):
    mo.md(
        r"""
    ### Portfolio Optimierung
    Es folgen alle wichtigen Funktionen zur Darstellung und Optimierung des Portfolios nach dem Estimating Mean Variance Verfahren.
    """
    )
    return


@app.cell
def _(DataFrame, pd, selected_date):
    import numpy as np
    import yfinance as yf
    import warnings


    def download_data(depot: DataFrame):
        warnings.filterwarnings("ignore")
        pd.options.display.float_format = "{:.4%}".format
        date = pd.to_datetime(selected_date.value)

        # Date range
        start = f"{date.year}-01-01"
        end = f"{date.year}-12-30"

        # Tickers of assets
        assets = depot["Symbol"].tolist()
        assets.sort()

        # Downloading data
        data = yf.download(assets, start=start, end=end, auto_adjust=False)
        data = data.loc[:, ("Adj Close", slice(None))]
        data.columns = assets

        Y = data[assets].pct_change().dropna()
        return Y
    return


if __name__ == "__main__":
    app.run()
