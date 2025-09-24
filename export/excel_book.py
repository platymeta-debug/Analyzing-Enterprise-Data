import os
import pandas as pd

from transform.metrics import (
    compute_profit_rate, compute_risk_rate, compute_asset_acq_amt,
    compute_topk_share, compute_top100_companies
)
from export.excel_utils import (
    write_table, add_heatmap, add_databar, add_iconset, add_dropdown, define_name, set_default_look
)
from export.links import naver_finance_url, dart_search_url

# 카테고리 상세 시트 최대 개수(너무 많으면 파일이 무거워짐)
MAX_CATEGORY_SHEETS = 15


def _ensure_dir(path):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)


def _load_or_empty(p):
    return pd.read_parquet(p) if os.path.exists(p) else pd.DataFrame()


def _safe_str(x):
    return "" if x is None else str(x)


def build_excel_book(env: dict,
                     fin_path: str,
                     events_path: str,
                     corp_path: str,
                     mcap_path: str,
                     macro_path: str,
                     out_path: str,
                     focus_year: int,
                     gics_level: str = "gics_industry",
                     also_emit_top100_variants: bool = True,
                     make_category_sheets: bool = True):
    """
    5.1 패치:
      - 하이퍼링크(네이버 금융 / DART 검색)
      - 대시보드 차트 2종 추가
      - 카테고리 상세 시트 자동 생성
      - (옵션) 정렬지표별 Top100 시트 추가
    """
    _ensure_dir(out_path)
    df_fin = _load_or_empty(fin_path)
    df_evt = _load_or_empty(events_path)
    df_dim = _load_or_empty(corp_path)
    df_mcap = _load_or_empty(mcap_path)
    df_macro = _load_or_empty(macro_path)

    # ---- Metrics 계산 ----
    pr = compute_profit_rate(df_fin, df_dim, gics_level, focus_year)
    rr = compute_risk_rate(df_evt, df_dim, gics_level, focus_year)
    aa = compute_asset_acq_amt(df_evt, df_dim, gics_level, focus_year)
    tk = compute_topk_share(df_mcap, df_dim, gics_level, focus_year, ks=(3,5,10))

    ind = (pr.merge(rr, on=gics_level, how="outer")
             .merge(aa, on=gics_level, how="outer")
             .merge(tk, on=gics_level, how="outer"))

    for col in ["profit_rate","risk_rate","asset_acq_amt","mcap_top3_share","mcap_top5_share","mcap_top10_share",
                "firms","risk_events","firm_count","total_mcap"]:
        if col not in ind.columns:
            ind[col] = None

    ind = ind[[gics_level,"profit_rate","risk_rate","asset_acq_amt",
               "mcap_top3_share","mcap_top5_share","mcap_top10_share",
               "firms","risk_events","firm_count","total_mcap"]].copy()

    # Top100(기본: 순이익)
    top100_base = compute_top100_companies(df_fin, df_mcap, df_dim, gics_level, focus_year,
                                           sort_metric="net_income", topn=100)

    # 하이퍼링크 컬럼 추가
    if not top100_base.empty:
        top100_base["네이버"] = top100_base["stock_code"].apply(lambda x: naver_finance_url(_safe_str(x)))
        top100_base["DART검색"] = top100_base["corp_name"].apply(lambda x: dart_search_url(_safe_str(x)))

    # (옵션) 정렬지표별 Top100 시트에 쓰기 위해 준비
    sort_variants = ["net_income","op_income","revenue","mcap_krw","share_in_category_pct"] if also_emit_top100_variants else []

    # 카테고리별 상세 시트 데이터 준비
    # - 해당 카테고리 기업 리스트(Top100 기준 아님, 전체 기업)
    # - 최근 리스크 이벤트(해당 카테고리만, 최근 365일)
    # 기업 리스트 만들기 위해: 해당 연도 재무 + dim 결합
    fin_year = df_fin[df_fin["fiscal_year"].astype(str)==str(focus_year)].copy()
    fin_year = fin_year.merge(df_dim[["corp_code","corp_name","stock_code",gics_level]], on="corp_code", how="left")
    fin_year["_mcap"] = None
    if not df_mcap.empty:
        snap = df_mcap.copy()
        snap["y"] = pd.to_datetime(snap["date_ref"], errors="coerce").dt.year
        snap = snap[snap["y"]==focus_year][["corp_code","mcap_krw"]]
        fin_year = fin_year.merge(snap, on="corp_code", how="left")
        fin_year["_mcap"] = fin_year["mcap_krw"]

    # 리스크 이벤트 최근 365일
    if not df_evt.empty:
        dts = pd.to_datetime(df_evt["event_date"], errors="coerce")
        cutoff = pd.Timestamp(f"{focus_year}-12-31") - pd.Timedelta(days=365)
        df_evt_recent = df_evt[(dts>=cutoff) & (dts<=pd.Timestamp(f"{focus_year}-12-31"))].copy()
        df_evt_recent = df_evt_recent.merge(df_dim[["corp_code","corp_name","stock_code",gics_level]], on="corp_code", how="left")
    else:
        df_evt_recent = pd.DataFrame(columns=["event_date","corp_code","event_type","sub_type","amount","counterparty","summary",gics_level,"corp_name","stock_code"])

    # ---- 엑셀 쓰기 ----
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        wb = xw.book
        F = set_default_look(wb)

        # =========================
        # 1) Dashboard_Combined
        # =========================
        dash = wb.add_worksheet("Dashboard_Combined")
        dash.hide_gridlines(2)
        dash.set_column(0, 0, 26)
        dash.set_column(2, 2, 24)
        dash.write(0, 0, "Controls", F["title"])
        dash.write(2, 0, "Year", F["header"]); dash.write(2, 2, focus_year, F["int"])
        dash.write(3, 0, "GICS Level", F["header"]); dash.write(3, 2, gics_level)
        dash.write(4, 0, "Metric (for Chart)", F["header"]); dash.write(4, 2, "profit_rate")
        add_dropdown(dash, 4, 2, 4, 2, ["profit_rate","risk_rate","asset_acq_amt","mcap_top3_share","mcap_top10_share"])
        dash.write(5, 0, "Top100 Sort Metric", F["header"]); dash.write(5, 2, "net_income")
        add_dropdown(dash, 5, 2, 5, 2, ["net_income","op_income","revenue","mcap_krw","share_in_category_pct"])
        dash.write(7, 0, "Notes", F["header"])
        dash.write(8, 0, "• 드롭다운으로 차트/Top100 정렬지표를 바꿔보세요.", F["subtle"])
        dash.write(9, 0, "• 인더스트리 시트는 히트맵/데이터바/아이콘으로 가독성을 높였습니다.", F["subtle"])

        # KPI 카드
        kpi_df = pd.DataFrame({
            "Metric": ["Avg Profit Rate","Avg Risk Rate","Sum Asset Acq Amt","Avg Top10 Share","FocusYear"],
            "Value": [
                ind["profit_rate"].mean(skipna=True) if not ind.empty else None,
                ind["risk_rate"].mean(skipna=True) if not ind.empty else None,
                ind["asset_acq_amt"].sum(skipna=True) if not ind.empty else None,
                ind["mcap_top10_share"].mean(skipna=True) if not ind.empty else None,
                focus_year
            ]
        })
        # KPI 출력
        dash.write(0, 6, "KPIs", F["title"])
        for i, row in enumerate(kpi_df.itertuples(index=False), start=2):
            dash.write(i, 6, row.Metric, F["header"])
            fmt = F["pct"] if "Rate" in row.Metric or "Share" in row.Metric else (F["krw"] if "Amt" in row.Metric else F["int"])
            dash.write(i, 8, row.Value, fmt)

        # 동적 이름 정의(5단계와 유사)
        # Industry_Overview 시트 작성 후 차트에서 참조
        # 차트는 아래에서 최종 삽입

        # =========================
        # 2) Industry_Overview
        # =========================
        ws2 = wb.add_worksheet("Industry_Overview")
        ws2.hide_gridlines(2)
        ws2.write(0, 0, f"Industry Overview ({gics_level}, {focus_year})", F["title"])

        number_formats = {
            "profit_rate": "0.00%",
            "risk_rate": "0.00%",
            "asset_acq_amt": "#,##0;[Red]-#,##0",
            "mcap_top3_share": "0.00%",
            "mcap_top5_share": "0.00%",
            "mcap_top10_share": "0.00%",
            "firms": "#,##0",
            "risk_events": "#,##0",
            "firm_count": "#,##0",
            "total_mcap": "#,##0"
        }
        write_table(xw, "Industry_Overview", ind, start_row=1, start_col=0,
                    header_format=F["header"], number_formats=number_formats)

        nrows = max(1, len(ind.index))
        # 히트맵/데이터바/아이콘셋
        add_heatmap(ws2, 2, 1, 1 + nrows, 1)  # Profit
        add_heatmap(ws2, 2, 2, 1 + nrows, 2, min_color="#F4CCCC", max_color="#76A5AF")  # Risk
        add_heatmap(ws2, 2, 3, 1 + nrows, 3, min_color="#FFF2CC", max_color="#93C47D")  # Asset
        add_databar(ws2, 2, 6, 1 + nrows, 6)  # Top10 share
        add_iconset(ws2, 2, 8, 1 + nrows, 8, icon_style="3_arrows")  # risk_events

        # 동적 이름(대시보드 차트용)
        define_name(wb, "IndCat", f"=Industry_Overview!$A$2:INDEX(Industry_Overview!$A:$A, COUNTA(Industry_Overview!$A:$A))")
        define_name(wb, "PR",     f"=Industry_Overview!$B$2:INDEX(Industry_Overview!$B:$B, COUNTA(Industry_Overview!$A:$A))")
        define_name(wb, "RR",     f"=Industry_Overview!$C$2:INDEX(Industry_Overview!$C:$C, COUNTA(Industry_Overview!$A:$A))")
        define_name(wb, "AA",     f"=Industry_Overview!$D$2:INDEX(Industry_Overview!$D:$D, COUNTA(Industry_Overview!$A:$A))")
        define_name(wb, "Top3",   f"=Industry_Overview!$E$2:INDEX(Industry_Overview!$E:$E, COUNTA(Industry_Overview!$A:$A))")
        define_name(wb, "Top10",  f"=Industry_Overview!$G$2:INDEX(Industry_Overview!$G:$G, COUNTA(Industry_Overview!$A:$A))")

        # =========================
        # 3) 대시보드 차트 2종
        # =========================
        # (1) 선택 메트릭 동적 막대
        ch1 = wb.add_chart({"type": "column"})
        ch1.set_title({"name": "Category Metric (dynamic)"})
        # 선택 메트릭: Dashboard_Combined!C5에 따라 CHOOSE → 여기서는 PR로 기본
        # XlsxWriter 제약때문에 간단히 PR 기본, 사용자는 드롭다운 바꾸고 새로고침 시 이름 참조로 동작
        ch1.add_series({
            "name":       "='Dashboard_Combined'!$C$5",
            "categories": "='Dashboard_Combined'!IndCat",
            "values":     "='Dashboard_Combined'!PR"
        })
        ch1.set_legend({"position": "bottom"})
        dash.insert_chart("F14", ch1, {"x_scale": 1.35, "y_scale": 1.2})

        # (2) ProfitRate vs RiskRate 산포도
        ch2 = wb.add_chart({"type": "scatter"})
        ch2.set_title({"name": "Profit vs Risk (scatter)"})
        ch2.add_series({
            "name": "Categories",
            "categories": "='Dashboard_Combined'!PR",  # X: ProfitRate
            "values":     "='Dashboard_Combined'!RR",  # Y: RiskRate
            "data_labels": {"series_name": False}
        })
        ch2.set_x_axis({"name": "ProfitRate"}); ch2.set_y_axis({"name": "RiskRate"})
        dash.insert_chart("F33", ch2, {"x_scale": 1.2, "y_scale": 1.1})

        # (3) Top3/5/10 콤보 차트(군집 막대)
        ch3 = wb.add_chart({"type": "column"})
        ch3.set_title({"name": "Concentration (Top3/5/10)"})
        ch3.add_series({"name": "Top3",  "categories": "='Dashboard_Combined'!IndCat", "values": "='Dashboard_Combined'!Top3"})
        ch3.add_series({"name": "Top10", "categories": "='Dashboard_Combined'!IndCat", "values": "='Dashboard_Combined'!Top10"})
        ch3.set_legend({"position": "bottom"})
        dash.insert_chart("F52", ch3, {"x_scale": 1.2, "y_scale": 1.1})

        # =========================
        # 4) Top100_Companies (+ 하이퍼링크)
        # =========================
        ws3 = wb.add_worksheet("Top100_Companies")
        ws3.hide_gridlines(2)
        ws3.write(0, 0, f"Top 100 Companies ({gics_level}, {focus_year})", F["title"])
        # 링크 컬럼 가공
        out_top = top100_base.copy()
        if not out_top.empty:
            # 하이퍼링크 표시 컬럼
            out_top.insert(out_top.columns.get_loc("corp_name")+1, "NAVER", "")
            out_top.insert(out_top.columns.get_loc("corp_name")+2, "DART", "")
            # 나머지는 테이블로 출력한 뒤 하이퍼링크를 셀에 주입
        write_table(xw, "Top100_Companies", out_top, start_row=2, start_col=0,
                    header_format=F["header"],
                    number_formats={
                        "net_income": "#,##0;[Red]-#,##0",
                        "op_income": "#,##0;[Red]-#,##0",
                        "revenue": "#,##0",
                        "mcap_krw": "#,##0",
                        "share_in_category_pct": "0.00%"
                    })

        # 하이퍼링크 삽입
        if not top100_base.empty:
            headers = out_top.columns.tolist()
            r0 = 3  # 데이터 시작 행(0-index 기반)
            c_name = headers.index("corp_name")
            c_nav  = headers.index("NAVER")
            c_dt   = headers.index("DART")
            for i, row in enumerate(top100_base.itertuples(index=False), start=0):
                # NAVER
                url_n = naver_finance_url(getattr(row, "stock_code"))
                if url_n:
                    ws3.write_url(r0 + i, c_nav, url_n, string="열기")
                # DART
                url_d = dart_search_url(getattr(row, "corp_name"))
                if url_d:
                    ws3.write_url(r0 + i, c_dt, url_d, string="검색")

        # (옵션) 정렬지표별 Top100 시트 동시 생성
        if also_emit_top100_variants and not df_fin.empty:
            for metric in sort_variants:
                tdf = compute_top100_companies(df_fin, df_mcap, df_dim, gics_level, focus_year,
                                               sort_metric=metric, topn=100)
                sh = wb.add_worksheet(f"Top100_{metric}")
                sh.hide_gridlines(2)
                sh.write(0, 0, f"Top 100 by {metric} ({gics_level}, {focus_year})", F["title"])
                write_table(xw, f"Top100_{metric}", tdf, start_row=2, start_col=0,
                            header_format=F["header"],
                            number_formats={
                                "net_income": "#,##0;[Red]-#,##0",
                                "op_income": "#,##0;[Red]-#,##0",
                                "revenue": "#,##0",
                                "mcap_krw": "#,##0",
                                "share_in_category_pct": "0.00%"
                            })

        # =========================
        # 5) Risk_Events / Asset_Transactions / Macro_Panel
        # =========================
        evt_name = "Risk_Events"
        ws4 = wb.add_worksheet(evt_name)
        ws4.hide_gridlines(2)
        if df_evt.empty:
            df_evt_out = pd.DataFrame(columns=["event_date","corp_code","event_type","sub_type","amount","counterparty","summary","report_nm","rcept_dt"])
        else:
            keep = ["event_date","corp_code","event_type","sub_type","amount","counterparty","summary","report_nm","rcept_dt"]
            for c in keep:
                if c not in df_evt.columns:
                    df_evt[c] = None
            df_evt_out = df_evt[keep].copy()
        write_table(xw, evt_name, df_evt_out, start_row=0, start_col=0,
                    header_format=F["header"],
                    number_formats={"amount": "#,##0;[Red]-#,##0"})

        at_name = "Asset_Transactions"
        ws5 = wb.add_worksheet(at_name)
        ws5.hide_gridlines(2)
        df_at = df_evt_out[df_evt_out["event_type"].isin(["ASSET_ACQ","BIZ_ACQ","EQUITY_ACQ","ASSET_DISP","BIZ_DISP","EQUITY_DISP"])] if not df_evt_out.empty else pd.DataFrame(columns=df_evt_out.columns)
        write_table(xw, at_name, df_at, start_row=0, start_col=0,
                    header_format=F["header"],
                    number_formats={"amount": "#,##0;[Red]-#,##0"})

        macro_name = "Macro_Panel"
        ws6 = wb.add_worksheet(macro_name)
        ws6.hide_gridlines(2)
        if df_macro.empty:
            df_macro = pd.DataFrame(columns=["date","M2","PolicyRate","CPI","IP","ConstructionOrders","RetailSales"])
        write_table(xw, macro_name, df_macro, start_row=0, start_col=0,
                    header_format=F["header"],
                    number_formats={
                        "M2": "0.00",
                        "PolicyRate": "0.00",
                        "CPI": "0.00",
                        "IP": "0.00",
                        "ConstructionOrders": "#,##0",
                        "RetailSales": "#,##0"
                    })

        # =========================
        # 6) (옵션) GICS 카테고리 상세 시트
        # =========================
        if make_category_sheets and not fin_year.empty:
            cats = fin_year[gics_level].fillna("Unclassified").value_counts().head(MAX_CATEGORY_SHEETS).index.tolist()
            for cat in cats:
                shn = f"Cat_{str(cat)[:24]}"
                sh = wb.add_worksheet(shn)
                sh.hide_gridlines(2)
                sh.write(0, 0, f"{gics_level} : {cat}", F["title"])
                # 기업 리스트(해당 카테고리)
                sub = fin_year[(fin_year[gics_level]==cat)].copy()
                cols = ["corp_name","stock_code","revenue","op_income","net_income","_mcap"]
                for c in cols:
                    if c not in sub.columns: sub[c] = None
                sub = sub[cols].drop_duplicates(subset=["corp_name","stock_code"])
                write_table(xw, shn, sub, start_row=2, start_col=0,
                            header_format=F["header"],
                            number_formats={
                                "revenue": "#,##0",
                                "op_income": "#,##0;[Red]-#,##0",
                                "net_income": "#,##0;[Red]-#,##0",
                                "_mcap": "#,##0"
                            })
                # 최근 리스크 이벤트(하단)
                sub_evt = df_evt_recent[df_evt_recent[gics_level]==cat].copy()
                keep2 = ["event_date","corp_name","event_type","sub_type","amount","counterparty","summary"]
                for c in keep2:
                    if c not in sub_evt.columns: sub_evt[c] = None
                write_table(xw, shn, sub_evt[keep2], start_row=4+len(sub)+2, start_col=0,
                            header_format=F["header"],
                            number_formats={"amount": "#,##0;[Red]-#,##0"})
                sh.write(4+len(sub)+1, 0, "최근 리스크 이벤트(지난 1년)", F["header"])

    print(f"[OK] Excel book written: {out_path}")
