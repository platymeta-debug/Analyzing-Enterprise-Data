import os
import re
import pandas as pd

# 권장 CSV 포맷(헤더):
# corp_code,gics_sector,gics_industry_group,gics_industry,gics_sub_industry
# 예시) 00123456,Information Technology,Semiconductors & Semiconductor Equipment,Semiconductors,Semiconductors

# ---- 한국어 키워드 → GICS 대략 매핑(초기 규칙) ----
# 정확도 향상을 위해 CSV 매핑을 점차 채워나가세요.
_RULES = [
    (r"반도체|파운드리|칩|메모리", ("Information Technology","Semiconductors & Semiconductor Equipment","Semiconductors","Semiconductors")),
    (r"소프트웨어|SaaS|솔루션|플랫폼|클라우드", ("Information Technology","Software & Services","Application Software","Application Software")),
    (r"인터넷|포털|플랫폼|게임", ("Communication Services","Media & Entertainment","Interactive Media & Services","Interactive Media & Services")),
    (r"은행|금융지주|캐피탈|저축은행", ("Financials","Banks","Banks","Banks")),
    (r"보험|손해보험|생명보험", ("Financials","Insurance","Insurance","Insurance")),
    (r"증권|자산운용|투자|브로커", ("Financials","Diversified Financials","Capital Markets","Capital Markets")),
    (r"바이오|제약|의약|헬스케어|의료기기", ("Health Care","Pharmaceuticals, Biotechnology & Life Sciences","Biotechnology","Biotechnology")),
    (r"건설|토목|엔지니어링|플랜트", ("Industrials","Capital Goods","Construction & Engineering","Construction & Engineering")),
    (r"철강|비철|소재|화학|석유화학", ("Materials","Materials","Chemicals","Commodity Chemicals")),
    (r"자동차|부품|전장", ("Consumer Discretionary","Automobiles & Components","Automobile Components","Auto Parts & Equipment")),
    (r"유통|리테일|쇼핑|마켓", ("Consumer Discretionary","Retailing","Internet & Direct Marketing Retail","Internet & Direct Marketing Retail")),
    (r"통신|5G|이동통신", ("Communication Services","Telecommunication Services","Wireless Telecommunication Services","Wireless Telecommunication Services")),
    (r"전력|가스|수도|공기업", ("Utilities","Utilities","Multi-Utilities","Multi-Utilities")),
    (r"항공|해운|물류|택배", ("Industrials","Transportation","Air Freight & Logistics","Air Freight & Logistics")),
    (r"반도체장비|노광|검사장비", ("Information Technology","Semiconductors & Semiconductor Equipment","Semiconductor Equipment","Semiconductor Equipment")),
]

def _rule_guess(name: str):
    if not isinstance(name, str):
        return None
    for pat, gics in _RULES:
        if re.search(pat, name, flags=re.I):
            return gics
    return None

def apply_gics_mapping(corp_parquet: str, out_parquet: str, mapping_csv: str = None):
    if not os.path.exists(corp_parquet):
        raise FileNotFoundError(f"corp master not found: {corp_parquet}")
    df = pd.read_parquet(corp_parquet)

    # 기본 컬럼 보장
    for c in ["corp_code","corp_name","stock_code"]:
        if c not in df.columns:
            df[c] = None

    # 우선 CSV 매핑 적용(있는 경우)
    gics_cols = ["gics_sector","gics_industry_group","gics_industry","gics_sub_industry"]
    for c in gics_cols:
        if c not in df.columns:
            df[c] = None

    if mapping_csv and os.path.exists(mapping_csv):
        mapdf = pd.read_csv(mapping_csv, dtype=str).fillna("")
        use_cols = ["corp_code"] + [c for c in gics_cols if c in mapdf.columns]
        mapdf = mapdf[use_cols].drop_duplicates("corp_code")
        df = df.merge(mapdf, on="corp_code", how="left", suffixes=("", "_map"))
        for c in gics_cols:
            df[c] = df[c+"_map"].where(df[c+"_map"].notna() & (df[c+"_map"]!=""), df[c])
            if c+"_map" in df.columns: df.drop(columns=[c+"_map"], inplace=True)

    # 룰 기반 보정: 비어 있는 기업만 규칙으로 추정
    mask_need = df["gics_sector"].isna() | (df["gics_sector"]=="")
    if mask_need.any():
        for idx in df[mask_need].index:
            nm = df.at[idx, "corp_name"]
            g = _rule_guess(nm)
            if g:
                df.at[idx, "gics_sector"] = g[0]
                df.at[idx, "gics_industry_group"] = g[1]
                df.at[idx, "gics_industry"] = g[2]
                df.at[idx, "gics_sub_industry"] = g[3]

    out_dir = os.path.dirname(out_parquet)
    if out_dir:
        os.makedirs(out_dir, exist_ok=True)
    df.to_parquet(out_parquet, index=False)
    print(f"[OK] GICS mapping applied → {out_parquet} (rows={len(df)})")
