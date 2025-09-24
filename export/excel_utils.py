from typing import List, Optional, Dict
import pandas as pd


def write_table(xw, sheet_name: str, df: pd.DataFrame, start_row=0, start_col=0,
                header_format=None, number_formats: Optional[Dict[str, str]] = None,
                autofilter=True, freeze_panes=True, table_style="Table Style Medium 9"):
    """
    DataFrame -> Excel Table
    - number_formats: {"colname": "#,##0;[Red]-#,##0", "colname2": "0.00%"} 처럼 컬럼별 표시형식
    """
    ws = xw.sheets[sheet_name]
    ws.hide_gridlines(2)  # both
    nrows, ncols = len(df.index), len(df.columns)

    # 본문 먼저 쓰기
    ws.write_row(start_row, start_col, df.columns.tolist(), header_format)
    if nrows:
        ws.write_column(start_row + 1, start_col, df.iloc[:, 0].tolist())
        for j in range(1, ncols):
            ws.write_column(start_row + 1, start_col + j, df.iloc[:, j].tolist())

    # 테이블 생성
    end_row, end_col = start_row + nrows, start_col + ncols - 1
    ws.add_table(start_row, start_col, end_row, end_col, {
        "style": table_style,
        "columns": [{"header": c} for c in df.columns],
        "autofilter": autofilter,
    })
    # 숫자 서식
    if number_formats:
        for j, col in enumerate(df.columns):
            fmt = number_formats.get(col)
            if fmt:
                rng = (start_row + 1, start_col + j, end_row, start_col + j)
                ws.set_column(rng[1], rng[3], 14, xw.book.add_format({"num_format": fmt}))
    # 폭 자동 (헤더 길이에 기반해 대략)
    for j, col in enumerate(df.columns):
        width = max(12, min(42, int(max(len(str(col)), 16))))
        ws.set_column(start_col + j, start_col + j, width)

    if freeze_panes:
        ws.freeze_panes(start_row + 1, start_col + 1)


def add_heatmap(ws, first_row, first_col, last_row, last_col, min_color="#FFF2CC", max_color="#63BE7B"):
    ws.conditional_format(first_row, first_col, last_row, last_col, {
        "type": "2_color_scale",
        "min_color": min_color,
        "max_color": max_color
    })


def add_databar(ws, first_row, first_col, last_row, last_col):
    ws.conditional_format(first_row, first_col, last_row, last_col, {
        "type": "data_bar"
    })


def add_iconset(ws, first_row, first_col, last_row, last_col, icon_style="3_traffic_lights"):
    ws.conditional_format(first_row, first_col, last_row, last_col, {
        "type": "icon_set",
        "icon_style": icon_style
    })


def add_dropdown(ws, first_row, first_col, last_row, last_col, options: List[str]):
    ws.data_validation(first_row, first_col, last_row, last_col, {
        "validate": "list",
        "source": options
    })


def define_name(wb, name: str, formula: str):
    # 예: define_name(wb, "SelectedMetric", "Dashboard_Combined!$C$4")
    wb.define_name(f"{name}={formula}")


def set_default_look(wb, *, base_font="맑은 고딕", base_size=10):
    """
    XlsxWriter는 워크북 전역 폰트 설정이 제한적이라, 자주 쓰는 포맷만 생성해서 재사용.
    """
    fmts = {
        "title": wb.add_format({"bold": True, "font_size": 14}),
        "header": wb.add_format({"bold": True, "bg_color": "#F2F2F2", "border": 1}),
        "subtle": wb.add_format({"font_color": "#666666"}),
        "pct": wb.add_format({"num_format": "0.00%"}),
        "int": wb.add_format({"num_format": "#,##0"}),
        "krw": wb.add_format({"num_format": "#,##0;[Red]-#,##0"}),
        "small": wb.add_format({"font_size": 9, "italic": True, "font_color": "#777777"}),
    }
    return fmts
