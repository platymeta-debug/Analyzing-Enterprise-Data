import pandas as pd

def compute_profit_rate(df_fin: pd.DataFrame, dim: pd.DataFrame, level: str, year: int):
    base = df_fin[df_fin["fiscal_year"].astype(str)==str(year)].merge(
        dim[["corp_code", level]], on="corp_code", how="left"
    )
    if base.empty:
        return pd.DataFrame(columns=[level,"firms","prof","profit_rate"])
    grp = base.groupby(level).agg(
        firms=("corp_code","nunique"),
        prof=("net_income", lambda s: (pd.to_numeric(s, errors="coerce")>0).sum())
    )
    grp["profit_rate"] = grp["prof"]/grp["firms"]
    return grp.reset_index()
