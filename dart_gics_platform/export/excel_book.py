import os
import pandas as pd

def _ensure_dir(path):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def build_excel_book(env: dict,
                     fin_path: str,
                     events_path: str,
                     corp_path: str,
                     mcap_path: str,
                     macro_path: str,
                     out_path: str,
                     focus_year: int):
    _ensure_dir(out_path)
    # Load if available (may be empty stubs initially)
    def load_or_empty(p):
        return pd.read_parquet(p) if os.path.exists(p) else pd.DataFrame()

    df_fin = load_or_empty(fin_path)
    df_evt = load_or_empty(events_path)
    df_corp = load_or_empty(corp_path)
    df_mcap = load_or_empty(mcap_path)
    df_macro = load_or_empty(macro_path)

    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        # Dashboard (combined): write simple placeholders now
        pd.DataFrame({
            "KPI": ["M2_YoY","PolicyRate","CPI_YoY","ProfitRate(Sample)"],
            "Value": [None,None,None,None],
            "Year": [focus_year]*4
        }).to_excel(xw, sheet_name="Dashboard_Combined", index=False)

        # Industry overview (empty schema)
        pd.DataFrame(columns=[
            "gics_sector","gics_industry","profit_rate","risk_rate",
            "asset_acq_amt","mcap_top3_share","mcap_top5_share","mcap_top10_share",
            "firm_count"
        ]).to_excel(xw, sheet_name="Industry_Overview", index=False)

        # Top100 companies (empty schema)
        pd.DataFrame(columns=[
            "rank","corp_name","stock_code","gics_sub_industry",
            "net_income","op_margin","ROIC","mcap","share_in_category_pct"
        ]).to_excel(xw, sheet_name="Top100_Companies", index=False)

        # Risk events
        (df_evt if not df_evt.empty else
         pd.DataFrame(columns=["date","corp_name","event_type","sub_type","amount","counterparty","link"])
        ).to_excel(xw, sheet_name="Risk_Events", index=False)

        # Asset transactions
        pd.DataFrame(columns=["date","corp_name","type","amount","target","purpose"]).to_excel(
            xw, sheet_name="Asset_Transactions", index=False
        )

        # Macro panel
        (df_macro if not df_macro.empty else
         pd.DataFrame(columns=["date","M2","PolicyRate","CPI","IP","ConstructionOrders","RetailSales"])
        ).to_excel(xw, sheet_name="Macro_Panel", index=False)

    print(f"[OK] Excel book written: {out_path}")
