import os
import math
import time
import datetime as dt
from typing import Dict, List, Optional, Tuple

import pandas as pd
import yfinance as yf

from common.dart_client import DartClient

PAGE_COUNT = 100
SLEEP_SEC = 0.15

OUT_COLUMNS = [
    "corp_code", "stock_code", "date_ref",
    "shares_outstanding", "close_px", "ccy",
    "mcap_local", "mcap_krw", "price_source", "ticker_used", "note"
]

# ---- DART: 주식의 총수 현황(stockTotqySttus) 헬퍼 -----------------
# 참고: 회사/연도 기준으로 보통주 발행주식수(유통/자기주식 제외 포함 여부는 공시 항목에 따름)
# 응답 필드 명칭은 회사/시기별로 차이가 있을 수 있어 key 후보를 넓게 잡음.

SHARE_KEYS = [
    # 대표 후보
    "istc_totqy",             # 발행주식총수(보통주)
    "istc_totqy_knd",         # (보통/우선 구분일 수 있음)
    "se_stk_co",              # 보통주식수
    "se_stk_cnt",             # 보통주식수(다른 명칭)
    "tot_co",                 # 총수
]

def _to_int(x) -> Optional[int]:
    if x is None:
        return None
    s = str(x).strip().replace(",", "")
    if s == "" or s == "-":
        return None
    try:
        return int(float(s))
    except:
        return None

def fetch_shares_outstanding(client: DartClient, corp_code: str, year: int) -> Optional[int]:
    """
    stockTotqySttus: 특정 연도의 주식총수 현황을 조회하여 보통주 발행주식수를 추정.
    연말 스냅샷이 목적이므로 해당 연도의 가장 최신(분기/반기/사업) 값을 선택.
    """
    # reprt_code를 신경쓰지 않는 버전: list/endpoints에 따라 여러 로우가 오면 최신을 씀.
    # 일부 시점/회사에서 status=013(없음)일 수 있음 → None 반환
    try:
        j = client.get("stockTotqySttus", {
            "corp_code": corp_code,
            "bsns_year": str(year),
        })
    except Exception:
        return None

    if str(j.get("status","")) != "000":
        return None

    lst = j.get("list", [])
    if not lst:
        return None

    # 최신 보고서(접수일자/보고서구분) 우선 정렬
    df = pd.DataFrame(lst)
    # 안전을 위해 rcept_no 혹은 rcept_dt가 있으면 그것으로 정렬
    sort_cols = [c for c in ["rcept_dt", "rcept_no"] if c in df.columns]
    if sort_cols:
        df = df.sort_values(sort_cols, ascending=True)  # 뒤쪽이 최신일 수 있음
    # 후보 키 중 첫 유효값 찾기(뒤에서부터 찾음 = 최신 우선)
    for _, row in df.iloc[::-1].iterrows():
        for key in SHARE_KEYS:
            if key in row and row[key] is not None:
                val = _to_int(row[key])
                if val and val > 0:
                    return val
    return None

# ---- 가격(yfinance) 수집 -----------------

def _guess_yahoo_ticker(stock_code: str) -> List[str]:
    """
    한국 상장 6자리 종목코드 기준으로 야후 티커 후보를 리턴.
    KOSPI: .KS, KOSDAQ: .KQ
    시장 구분을 모르면 두 가지 모두 시도.
    """
    if not isinstance(stock_code, str) or len(stock_code) == 0:
        return []
    base = stock_code.strip()
    # 우선순위: KS → KQ
    return [f"{base}.KS", f"{base}.KQ"]

def fetch_close_price_yahoo(ticker_candidates: List[str], date_ref: dt.date) -> Tuple[Optional[float], Optional[str], str]:
    """
    주어진 날짜(date_ref)의 '가까운 영업일' 종가를 시도해서 가져옴.
    - date_ref ± 14 영업일 범위에서 최종 종가를 찾음
    반환: (close_px, ticker_used, note)
    """
    if not ticker_candidates:
        return (None, None, "no_ticker_candidate")

    # 2주 범위로 완충
    start = date_ref - dt.timedelta(days=21)
    end = date_ref + dt.timedelta(days=7)

    for t in ticker_candidates:
        try:
            df = yf.download(t, start=start.isoformat(), end=(end + dt.timedelta(days=1)).isoformat(), progress=False, auto_adjust=False)
            if df is None or df.empty:
                continue
            # date_ref에 가장 가까운 과거 영업일 종가
            df = df.sort_index()
            df_before = df[df.index.date <= date_ref]
            if df_before.empty:
                # 직후 영업일로 대체
                df_after = df[df.index.date > date_ref]
                if df_after.empty:
                    continue
                px = float(df_after["Close"].iloc[0])
                note = "after_ref"
            else:
                px = float(df_before["Close"].iloc[-1])
                note = "on_or_before_ref"
            return (px, t, note)
        except Exception:
            continue
    return (None, None, "no_price_found")

# ---- 스냅샷 빌더 -----------------

def _load_corp_master(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"corp_master not found: {path}. 먼저 `python run_pipeline.py bootstrap` 실행하세요."
        )
    df = pd.read_parquet(path)
    need = {"corp_code","corp_name","stock_code","is_listed"}
    for m in need.difference(df.columns):
        df[m] = None
    return df

def build_mcap_snapshot(env: dict, date_ref: str, out_path: str):
    """
    기준일(date_ref, 'YYYY-MM-DD')의 시가총액 스냅샷을 생성.
    - 국내 상장(6자리 stock_code 보유) 대상
    - 가격: yfinance 종가
    - 주식수: DART stockTotqySttus (해당 연도)
    저장: data/mcap_snapshot.parquet
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    client = DartClient(env["DART_API_KEY"], sleep_sec=SLEEP_SEC)

    # 기준일 파싱
    try:
        dref = dt.datetime.strptime(date_ref, "%Y-%m-%d").date()
    except Exception:
        raise ValueError("date_ref must be 'YYYY-MM-DD'")

    df_corp = _load_corp_master("data/corp_master.parquet")
    # 상장만 대상(국내 6자리 종목코드)
    base = df_corp[df_corp["stock_code"].astype(str).str.len()==6].copy()
    if base.empty:
        # 파일은 만들어 둠
        pd.DataFrame(columns=OUT_COLUMNS).to_parquet(out_path, index=False)
        print(f"[OK] mcap snapshot saved (empty): {out_path}")
        return

    rows = []
    year = dref.year
    for i, r in base.reset_index(drop=True).iterrows():
        corp_code = str(r["corp_code"])
        stock_code = str(r["stock_code"])

        # 1) 발행주식수(연도 기준)
        shares = fetch_shares_outstanding(client, corp_code, year)

        # 2) 가격(yahoo)
        tickers = _guess_yahoo_ticker(stock_code)
        close_px, ticker_used, note = fetch_close_price_yahoo(tickers, dref)

        # 3) 시총 계산(원화 가정)
        ccy = "KRW"
        if shares and close_px:
            mcap_local = float(shares) * float(close_px)
        else:
            mcap_local = None

        # 동일통화 가정이므로 mcap_krw = mcap_local
        rows.append({
            "corp_code": corp_code,
            "stock_code": stock_code,
            "date_ref": dref.isoformat(),
            "shares_outstanding": shares,
            "close_px": close_px,
            "ccy": ccy,
            "mcap_local": mcap_local,
            "mcap_krw": mcap_local,
            "price_source": "yahoo",
            "ticker_used": ticker_used,
            "note": note
        })

        if (i+1) % 200 == 0:
            print(f"[INFO] mcap progress {i+1}/{len(base)} ({(i+1)/len(base):.1%})")

    df = pd.DataFrame(rows)
    # 컬럼 정렬
    for c in OUT_COLUMNS:
        if c not in df.columns:
            df[c] = None
    df = df[OUT_COLUMNS]
    df.to_parquet(out_path, index=False)
    print(f"[OK] mcap snapshot saved: {out_path}, rows={len(df)}")
