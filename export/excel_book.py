import os
import pandas as pd

from transform.metrics import (
    compute_profit_rate, compute_risk_rate, compute_asset_acq_amt,
    compute_topk_share, compute_top100_companies
)
from export.excel_utils import (
    write_table, add_heatmap, add_databar, add_iconset, add_dropdown, define_name
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
    5단계 UX 업그레이드 버전
    - Dashboard_Combined: 드롭다운으로 지표/정렬 기준/연도 선택 → 동적 차트
    - Industry_Overview: 히트맵/데이터바/아이콘셋으로 가독성 강화
    - Top100_Companies: 점유율/마진 시각 보조, 드롭다운으로 정렬 지표 변경
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

    # 정돈/결측 0 처리(시각화용)
    for col in ["profit_rate","risk_rate","asset_acq_amt","mcap_top3_share","mcap_top5_share","mcap_top10_share",
                "firms","risk_events","firm_count","total_mcap"]:
        if col not in ind.columns:
            ind[col] = None
    # 보기 좋게 정렬
    ind = ind[[gics_level,"profit_rate","risk_rate","asset_acq_amt",
               "mcap_top3_share","mcap_top5_share","mcap_top10_share",
               "firms","risk_events","firm_count","total_mcap"]].copy()

    # Top100: 순이익 기준 (드롭다운으로 op_income/ revenue 등 정렬지표 바꾸도록 설계)
    base_top100 = compute_top100_companies(df_fin, df_mcap, df_dim, gics_level, focus_year,
                                           sort_metric="net_income", topn=100)

    # ---- 엑셀 쓰기 ----
    with pd.ExcelWriter(out_path, engine="xlsxwriter") as xw:
        wb = xw.book

        # 공통 포맷
        fmt_header = wb.add_format({"bold": True, "bg_color": "#F2F2F2", "border": 1})
        fmt_title = wb.add_format({"bold": True, "font_size": 14})
        fmt_subtle = wb.add_format({"font_color": "#666666"})
        fmt_pct = wb.add_format({"num_format": "0.00%"})
        fmt_int = wb.add_format({"num_format": "#,##0"})
        fmt_krw = wb.add_format({"num_format": "#,##0;[Red]-#,##0"})
        fmt_small = wb.add_format({"font_size": 9, "italic": True, "font_color": "#777777"})

        # =========================
        # 1) Dashboard_Combined
        # =========================
        dash_name = "Dashboard_Combined"
        ws = wb.add_worksheet(dash_name)
        xw.sheets[dash_name] = ws
        ws.hide_gridlines(2)

        # 컨트롤(드롭다운)
        ws.write(0, 0, "Controls", fmt_title)
        ws.write(2, 0, "Year", fmt_header)
        ws.write(2, 2, focus_year, fmt_int)

        ws.write(3, 0, "GICS Level", fmt_header)
        d_gics_levels = ["gics_sector","gics_industry_group","gics_industry","gics_sub_industry"]
        ws.write(3, 2, gics_level)
        # 드롭다운(표면적으로만, 분석은 고정된 gics_level로 만들어짐)
        ws.data_validation(3, 2, 3, 2, {"validate":"list","source": d_gics_levels})

        ws.write(4, 0, "Metric (for Chart)", fmt_header)
        metric_options = ["profit_rate","risk_rate","asset_acq_amt","mcap_top3_share","mcap_top10_share"]
        ws.write(4, 2, metric_options[0])
        ws.data_validation(4, 2, 4, 2, {"validate":"list","source": metric_options})

        ws.write(5, 0, "Top100 Sort Metric", fmt_header)
        sort_options = ["net_income","op_income","revenue","mcap_krw","share_in_category_pct"]
        ws.write(5, 2, sort_options[0])
        ws.data_validation(5, 2, 5, 2, {"validate":"list","source": sort_options})

        ws.write(7, 0, "Notes", fmt_header)
        ws.write(8, 0, "• 드롭다운으로 차트/Top100 정렬지표를 바꿔보세요.", fmt_subtle)
        ws.write(9, 0, "• Industry_Overview에서 값은 조건부서식(히트맵/데이터바)으로 직관적으로 비교됩니다.", fmt_subtle)
        ws.set_column(0, 0, 26)
        ws.set_column(2, 2, 24)

        # KPI 카드 (평균/합계)
        ws.write(0, 6, "KPIs", fmt_title)
        kpi_rows = [
            ("Avg Profit Rate", "=AVERAGE(Industry_Overview!$B$2:INDEX(Industry_Overview!$B:$B, COUNTA(Industry_Overview!$A:$A)))", fmt_pct),
            ("Avg Risk Rate",   "=AVERAGE(Industry_Overview!$C$2:INDEX(Industry_Overview!$C:$C, COUNTA(Industry_Overview!$A:$A)))", fmt_pct),
            ("Sum Asset Acq Amt", "=SUM(Industry_Overview!$D$2:INDEX(Industry_Overview!$D:$D, COUNTA(Industry_Overview!$A:$A)))", fmt_krw),
            ("Avg Top10 Share", "=AVERAGE(Industry_Overview!$G$2:INDEX(Industry_Overview!$G:$G, COUNTA(Industry_Overview!$A:$A)))", fmt_pct),
        ]
        for i, (label, formula, fmt) in enumerate(kpi_rows, start=2):
            ws.write(i, 6, label, fmt_header)
            ws.write_formula(i, 8, formula, fmt)

        # 동적 차트용 Named Range 정의
        # A: 카테고리 이름, B..: 지표 열
        define_name(wb, "IndCat", f"Industry_Overview!$A$2:INDEX(Industry_Overview!$A:$A, COUNTA(Industry_Overview!$A:$A))")
        define_name(wb, "PR",     f"Industry_Overview!$B$2:INDEX(Industry_Overview!$B:$B, COUNTA(Industry_Overview!$A:$A))")
        define_name(wb, "RR",     f"Industry_Overview!$C$2:INDEX(Industry_Overview!$C:$C, COUNTA(Industry_Overview!$A:$A))")
        define_name(wb, "AA",     f"Industry_Overview!$D$2:INDEX(Industry_Overview!$D:$D, COUNTA(Industry_Overview!$A:$A))")
        define_name(wb, "Top3",   f"Industry_Overview!$E$2:INDEX(Industry_Overview!$E:$E, COUNTA(Industry_Overview!$A:$A))")
        define_name(wb, "Top10",  f"Industry_Overview!$G$2:INDEX(Industry_Overview!$G:$G, COUNTA(Industry_Overview!$A:$A))")
        define_name(
            wb,
            "SelectedMetric",
            "CHOOSE(MATCH(Dashboard_Combined!$C$5,{\"profit_rate\",\"risk_rate\",\"asset_acq_amt\",\"mcap_top3_share\",\"mcap_top10_share\"},0),PR,RR,AA,Top3,Top10)"
        )

        # 선택된 메트릭 → 이름으로 맵핑 (LOOKUP)
        ws.write(12, 0, "Selected Metric Series", fmt_header)
        ws.write_formula(12, 2, "=SelectedMetric")

        # 차트: 동적 범위(카테고리 vs 선택 메트릭)
        ch = wb.add_chart({"type": "column"})
        ch.set_title({"name": "Category Metric (dynamic)"})
        # 카테고리/값은 이름 참조
        ch.add_series({
            "name":       "='Dashboard_Combined'!$C$5",
            "categories": "='Dashboard_Combined'!IndCat",
            "values":     "=SelectedMetric",
        })
        ch.set_legend({"position": "bottom"})
        ws.insert_chart("F14", ch, {"x_scale": 1.35, "y_scale": 1.2})

        # =========================
        # 2) Industry_Overview
        # =========================
        ind_name = "Industry_Overview"
        ws2 = wb.add_worksheet(ind_name)
        xw.sheets[ind_name] = ws2
        ws2.hide_gridlines(2)
        ws2.write(0, 0, f"Industry Overview ({gics_level}, {focus_year})", fmt_title)

        # 지표 테이블
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
        write_table(xw, ind_name, ind, start_row=1, start_col=0,
                    header_format=fmt_header, number_formats=number_formats)

        # 조건부서식: 히트맵(Profit/Risk/Asset), 데이터바(Top10_share), 아이콘셋(리스크 이벤트)
        nrows = len(ind.index)
        if nrows == 0:
            nrows = 1  # 보호
        # heatmap: B(profit), C(risk), D(asset)
        add_heatmap(ws2, 2, 1, 1 + nrows, 1)
        add_heatmap(ws2, 2, 2, 1 + nrows, 2, min_color="#F4CCCC", max_color="#76A5AF")  # risk는 다른 팔레트
        add_heatmap(ws2, 2, 3, 1 + nrows, 3, min_color="#FFF2CC", max_color="#93C47D")
        # data bar: G(top10 share)
        add_databar(ws2, 2, 6, 1 + nrows, 6)
        # icon set: H(rms), I(risk_events)
        add_iconset(ws2, 2, 8, 1 + nrows, 8, icon_style="3_arrows")

        ws2.write(2 + nrows + 2, 0, "Tip: 열 필터와 정렬을 이용해 지표 상/하위 업종을 빠르게 파악하세요.", fmt_small)

        # =========================
        # 3) Top100_Companies
        # =========================
        top_name = "Top100_Companies"
        ws3 = wb.add_worksheet(top_name)
        xw.sheets[top_name] = ws3
        ws3.hide_gridlines(2)
        ws3.write(0, 0, f"Top 100 Companies ({gics_level}, {focus_year})", fmt_title)
        ws3.write(1, 0, "Sort By", fmt_header)
        # 드롭다운: Dashboard_Combined의 C6을 참조(사용자는 대시보드에서 선택)
        ws3.write_formula(1, 2, "='Dashboard_Combined'!$C$6")
        add_dropdown(ws3, 1, 2, 1, 2, ["net_income","op_income","revenue","mcap_krw","share_in_category_pct"])
        ws3.write(1, 3, "(대시보드에서 변경하세요)", fmt_subtle)

        # Top100 표 (기본: net_income 정렬)
        number_formats_top = {
            "net_income": "#,##0;[Red]-#,##0",
            "op_income": "#,##0;[Red]-#,##0",
            "revenue": "#,##0",
            "mcap_krw": "#,##0",
            "share_in_category_pct": "0.00%",
        }
        write_table(xw, top_name, base_top100, start_row=3, start_col=0,
                    header_format=fmt_header, number_formats=number_formats_top)

        nrows_top = len(base_top100.index)
        if nrows_top == 0:
            nrows_top = 1

        # 조건부서식: 점유율(share_in_category_pct) 데이터바, 순이익/영업이익 음수 붉은 글씨는 포맷으로 처리됨
        # share_in_category_pct: 열 index 찾아 적용
        headers = base_top100.columns.tolist()
        share_j = headers.index("share_in_category_pct")
        add_databar(ws3, 4, share_j, 3 + nrows_top, share_j)

        ws3.write(4 + nrows_top + 2, 0, "Tip: Sort By 지표를 바꾸면 표 우측 상단의 정렬(엑셀)로 빠르게 재정렬하세요.", fmt_small)

        # =========================
        # 4) Risk_Events / Asset_Transactions / Macro_Panel
        # =========================
        # (2단계/2.5단계, 4단계에서 만들어둔 그대로 출력하되, 테이블 스타일만 통일)
        # Risk_Events
        evt_name = "Risk_Events"
        ws4 = wb.add_worksheet(evt_name)
        xw.sheets[evt_name] = ws4
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
                    header_format=fmt_header,
                    number_formats={"amount": "#,##0;[Red]-#,##0"})

        # Asset_Transactions (이벤트에서 ACQ/DISP 필터링해 보고싶으면 여기서 추가 후 출력)
        at_name = "Asset_Transactions"
        ws5 = wb.add_worksheet(at_name)
        xw.sheets[at_name] = ws5
        ws5.hide_gridlines(2)
        df_at = df_evt_out[df_evt_out["event_type"].isin(["ASSET_ACQ","BIZ_ACQ","EQUITY_ACQ","ASSET_DISP","BIZ_DISP","EQUITY_DISP"])] if not df_evt_out.empty else pd.DataFrame(columns=df_evt_out.columns)
        write_table(xw, at_name, df_at, start_row=0, start_col=0,
                    header_format=fmt_header,
                    number_formats={"amount": "#,##0;[Red]-#,##0"})

        # Macro_Panel
        macro_name = "Macro_Panel"
        ws6 = wb.add_worksheet(macro_name)
        xw.sheets[macro_name] = ws6
        ws6.hide_gridlines(2)
        if df_macro.empty:
            df_macro = pd.DataFrame(columns=["date","M2","PolicyRate","CPI","IP","ConstructionOrders","RetailSales"])
        write_table(xw, macro_name, df_macro, start_row=0, start_col=0,
                    header_format=fmt_header,
                    number_formats={
                        "M2": "0.00",
                        "PolicyRate": "0.00",
                        "CPI": "0.00",
                        "IP": "0.00",
                        "ConstructionOrders": "#,##0",
                        "RetailSales": "#,##0"
                    })

    print(f"[OK] Excel book written: {out_path}")
