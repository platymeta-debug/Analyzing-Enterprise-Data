import os
import pandas as pd
from transform.metrics import (
    compute_profit_rate, compute_risk_rate, compute_asset_acq_amt,
    compute_topk_share, compute_top100_companies
)

def _ensure_dir(path):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

def _load_or_empty(p):
    return pd.read_parquet(p) if os.path.exists(p) else pd.DataFrame()

def build_excel_book(env: dict,
                     fin_path: str,
                     events_path: str,
                     corp_path: str,
                     mcap_path: str,
                     macro_path: str,
                     out_path: str,
                     focus_year: int,
                     gics_level: str = "gics_industry"):
    """
    gics_level: gics_sector | gics_industry_group | gics_industry | gics_sub_industry
    """
    _ensure_dir(out_path)
    df_fin = _load_or_empty(fin_path)
    df_evt = _load_or_empty(events_path)
    df_dim = _load_or_empty(corp_path)
    df_mcap = _load_or_empty(mcap_path)
    df_macro = _load_or_empty(macro_path)

    # ---- Metrics ----
    pr = compute_profit_rate(df_fin, df_dim, gics_level, focus_year)
    rr = compute_risk_rate(df_evt, df_dim, gics_level, focus_year)
    aa = compute_asset_acq_amt(df_evt, df_dim, gics_level, focus_year)
    tk = compute_topk_share(df_mcap, df_dim, gics_level, focus_year, ks=(3,5,10))

    ind = (pr.merge(rr, on=gics_level, how="outer")
             .merge(aa, on=gics_level, how="outer")
             .merge(tk, on=gics_level, how="outer"))

    # Top100 — net_income 기준 (원하면 op_income 등으로 교체 가능)
    top100 = compute_top100_companies(df_fin, df_mcap, df_dim, gics_level, focus_year,
                                      sort_metric="net_income", topn=100)

    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        wb = xw.book

        # ---- Dashboard_Combined ----
        dash = pd.DataFrame({
            "Metric": ["ProfitRate(avg)","RiskRate(avg)","AssetAcqAmt(sum)",
                       "McapTop3(avg)","McapTop5(avg)","McapTop10(avg)","FocusYear"],
            "Value": [
                pr["profit_rate"].mean(skipna=True) if not pr.empty else None,
                rr["risk_rate"].mean(skipna=True) if not rr.empty else None,
                aa["asset_acq_amt"].sum(skipna=True) if not aa.empty else None,
                tk["mcap_top3_share"].mean(skipna=True) if not tk.empty else None,
                tk["mcap_top5_share"].mean(skipna=True) if not tk.empty else None,
                tk["mcap_top10_share"].mean(skipna=True) if not tk.empty else None,
                focus_year
            ]
        })
        dash.to_excel(xw, sheet_name="Dashboard_Combined", index=False)
        ws = xw.sheets["Dashboard_Combined"]

        # 간단 차트: (산업별) ProfitRate, RiskRate, AssetAcqAmt
        # Industry_Overview 작성 후 참조할 거라 우선 작성만 한다.

        # ---- Industry_Overview ----
        ind_cols = [
            gics_level,
            "profit_rate","risk_rate","asset_acq_amt",
            "mcap_top3_share","mcap_top5_share","mcap_top10_share",
            "firms","risk_events","firm_count","total_mcap"
        ]
        for c in ind_cols:
            if c not in ind.columns:
                ind[c] = None
        ind = ind[ind_cols].sort_values("profit_rate", ascending=False)
        ind.to_excel(xw, sheet_name="Industry_Overview", index=False)
        ws_ind = xw.sheets["Industry_Overview"]

        # 차트: ProfitRate / RiskRate / AssetAcqAmt (카테고리별)
        last_row = len(ind) + 1
        # ProfitRate
        ch1 = wb.add_chart({"type":"column"})
        ch1.add_series({
            "name":"Profit Rate",
            "categories": ["Industry_Overview", 1, 0, last_row-1, 0],
            "values":     ["Industry_Overview", 1, 1, last_row-1, 1],
        })
        ch1.set_title({"name":"Profit Rate by Category"})
        ws_ind.insert_chart("J2", ch1)

        # RiskRate
        ch2 = wb.add_chart({"type":"column"})
        ch2.add_series({
            "name":"Risk Rate",
            "categories": ["Industry_Overview", 1, 0, last_row-1, 0],
            "values":     ["Industry_Overview", 1, 2, last_row-1, 2],
        })
        ch2.set_title({"name":"Risk Rate by Category"})
        ws_ind.insert_chart("J18", ch2)

        # AssetAcqAmt
        ch3 = wb.add_chart({"type":"column"})
        ch3.add_series({
            "name":"Asset Acquisition Amount",
            "categories": ["Industry_Overview", 1, 0, last_row-1, 0],
            "values":     ["Industry_Overview", 1, 3, last_row-1, 3],
        })
        ch3.set_title({"name":"Asset Acquisition Amount by Category"})
        ws_ind.insert_chart("J34", ch3)

        # ---- Top100_Companies ----
        top_cols = [
            "rank","corp_name","stock_code","gics_level",
            "net_income","op_income","revenue",
            "mcap_krw","share_in_category_pct","corp_code"
        ]
        for c in top_cols:
            if c not in top100.columns:
                top100[c] = None
        top100 = top100[top_cols]
        top100.to_excel(xw, sheet_name="Top100_Companies", index=False)

        # ---- Risk_Events (원본 그대로) ----
        df_evt_out = df_evt.copy()
        if not df_evt_out.empty:
            # 보기 좋은 컬럼 우선
            keep = ["event_date","corp_code","event_type","sub_type","amount","counterparty","summary","report_nm","rcept_dt"]
            for c in keep:
                if c not in df_evt_out.columns:
                    df_evt_out[c] = None
            df_evt_out = df_evt_out[keep]
        else:
            df_evt_out = pd.DataFrame(columns=["event_date","corp_code","event_type","sub_type","amount","counterparty","summary","report_nm","rcept_dt"])
        df_evt_out.to_excel(xw, sheet_name="Risk_Events", index=False)

        # ---- Asset_Transactions (필요 시 이벤트에서 필터링하여 별도 구성 가능) ----
        # 여기서는 빈 시트 유지
        pd.DataFrame(columns=["date","corp_name","type","amount","target","purpose"]).to_excel(
            xw, sheet_name="Asset_Transactions", index=False
        )

        # ---- Macro_Panel (원본 유지) ----
        if df_macro.empty:
            df_macro = pd.DataFrame(columns=["date","M2","PolicyRate","CPI","IP","ConstructionOrders","RetailSales"])
        df_macro.to_excel(xw, sheet_name="Macro_Panel", index=False)

    print(f"[OK] Excel book written: {out_path}")
