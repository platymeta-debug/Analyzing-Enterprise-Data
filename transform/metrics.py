import pandas as pd
from typing import List, Tuple

# level: "gics_sector" | "gics_industry_group" | "gics_industry" | "gics_sub_industry"

def compute_profit_rate(df_fin: pd.DataFrame, dim: pd.DataFrame, level: str, year: int):
    base = df_fin[df_fin["fiscal_year"].astype(str)==str(year)].merge(
        dim[["corp_code", level]], on="corp_code", how="left"
    )
    if base.empty:
        return pd.DataFrame(columns=[level,"firms","prof","profit_rate"])
    grp = base.groupby(level, dropna=False).agg(
        firms=("corp_code","nunique"),
        prof=("net_income", lambda s: (pd.to_numeric(s, errors="coerce")>0).sum())
    ).reset_index()
    grp["profit_rate"] = grp["prof"] / grp["firms"].replace({0: None})
    return grp

def compute_risk_rate(df_events: pd.DataFrame, dim: pd.DataFrame, level: str, year: int,
                      risk_types: Tuple[str,...] = ("DEFAULT","OPS_SUSPEND","REHAB","LIQUIDATION","BANK_GROUP")):
    if df_events.empty:
        return pd.DataFrame(columns=[level,"risk_events","risk_rate"])
    df_events = df_events.copy()
    df_events["y"] = pd.to_datetime(df_events["event_date"], errors="coerce").dt.year
    e = df_events[df_events["y"]==year]
    e = e[e["event_type"].isin(risk_types)]
    if e.empty:
        return pd.DataFrame(columns=[level,"risk_events","risk_rate"])
    # 기업 기준으로 중복 제거(한 해 여러 건이면 1로 카운트할지 선택, 여기선 건수 합계)
    e = e.merge(dim[["corp_code", level]], on="corp_code", how="left")
    risk_cnt = e.groupby(level, dropna=False).agg(risk_events=("corp_code","count")).reset_index()
    # 분모(기업 수)
    firm_cnt = dim.groupby(level, dropna=False).agg(firm_count=("corp_code","nunique")).reset_index()
    out = risk_cnt.merge(firm_cnt, on=level, how="left")
    out["risk_rate"] = out["risk_events"] / out["firm_count"].replace({0: None})
    return out

def compute_asset_acq_amt(df_events: pd.DataFrame, dim: pd.DataFrame, level: str, year: int):
    if df_events.empty:
        return pd.DataFrame(columns=[level,"asset_acq_amt"])
    df_events = df_events.copy()
    df_events["y"] = pd.to_datetime(df_events["event_date"], errors="coerce").dt.year
    e = df_events[df_events["y"]==year]
    e = e[e["event_type"].isin(("ASSET_ACQ","BIZ_ACQ","EQUITY_ACQ"))]
    if e.empty:
        return pd.DataFrame(columns=[level,"asset_acq_amt"])
    e["amount"] = pd.to_numeric(e["amount"], errors="coerce")
    e = e.merge(dim[["corp_code", level]], on="corp_code", how="left")
    out = e.groupby(level, dropna=False).agg(asset_acq_amt=("amount","sum")).reset_index()
    return out

def compute_topk_share(df_mcap: pd.DataFrame, dim: pd.DataFrame, level: str, year: int, ks=(3,5,10)):
    if df_mcap.empty:
        cols = [level,"total_mcap"] + [f"mcap_top{k}_share" for k in ks]
        return pd.DataFrame(columns=cols)
    base = df_mcap.copy()
    base["y"] = pd.to_datetime(base["date_ref"], errors="coerce").dt.year
    base = base[base["y"]==year].merge(dim[["corp_code", level]], on="corp_code", how="left")
    cols = [level,"total_mcap"] + [f"mcap_top{k}_share" for k in ks]
    out = []
    for g, gdf in base.groupby(level, dropna=False):
        gdf = gdf.sort_values("mcap_krw", ascending=False)
        total = pd.to_numeric(gdf["mcap_krw"], errors="coerce").sum()
        if total and total > 0:
            shares = pd.to_numeric(gdf["mcap_krw"], errors="coerce") / total
        else:
            shares = pd.Series([0.0]*len(gdf))
        row = {level: g, "total_mcap": total}
        for k in ks:
            row[f"mcap_top{k}_share"] = float(shares.head(k).sum())
        out.append(row)
    return pd.DataFrame(out, columns=cols)

def compute_top100_companies(df_fin: pd.DataFrame, df_mcap: pd.DataFrame, dim: pd.DataFrame,
                             level: str, year: int, sort_metric="net_income", topn=100):
    # 연도 필터 & 결합
    fin = df_fin[df_fin["fiscal_year"].astype(str)==str(year)].copy()
    fin[sort_metric] = pd.to_numeric(fin[sort_metric], errors="coerce")
    # 회사당 1행(연결/별도 중복 제거) — 가장 큰 값 우선
    fin = fin.sort_values(sort_metric, ascending=False).drop_duplicates("corp_code", keep="first")

    # 시총/점유율 계산
    mcap = df_mcap.copy()
    mcap["y"] = pd.to_datetime(mcap["date_ref"], errors="coerce").dt.year
    mcap = mcap[mcap["y"]==year][["corp_code","mcap_krw"]]

    base = (fin.merge(dim[["corp_code","corp_name","stock_code",level]], on="corp_code", how="left")
               .merge(mcap, on="corp_code", how="left"))

    # 카테고리별 시총합으로 점유율 계산
    base["_mcap"] = pd.to_numeric(base["mcap_krw"], errors="coerce")
    total_by_cat = base.groupby(level, dropna=False)["_mcap"].sum().rename("_cat_mcap")
    base = base.merge(total_by_cat, on=level, how="left")
    base["share_in_category_pct"] = (base["_mcap"] / base["_cat_mcap"] * 100).round(2)

    # 정렬/TopN
    base = base.sort_values(sort_metric, ascending=False).head(topn).copy()
    base.insert(0, "rank", range(1, len(base)+1))
    # 선택 컬럼
    out = base[[
        "rank","corp_name","stock_code",level, sort_metric,"op_income","revenue",
        "mcap_krw","share_in_category_pct","corp_code"
    ]].rename(columns={level:"gics_level"})
    return out.reset_index(drop=True)
