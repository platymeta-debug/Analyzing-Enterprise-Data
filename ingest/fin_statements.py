import os
import math
import time
from typing import Dict, List, Optional, Tuple

import pandas as pd

from common.dart_client import DartClient

# -----------------------------
# 설정(필요 시 조정)
# -----------------------------
REPRT_CODES = [
    "11011",  # 사업보고서(연간)
    # 필요시 분기/반기까지 확장하려면 아래 주석 해제
    # "11012",  # 반기보고서
    # "11013",  # 1분기
    # "11014",  # 3분기
]
FS_DIV_PRIORITY = ["CFS", "OFS"]  # 연결 우선, 안되면 별도
PAGE_COUNT = 100  # OpenDART 페이지 사이즈
SLEEP_SEC = 0.15  # 요청 간 간격(레이트리밋 회피)
CHECKPOINT_EVERY = 2000  # N건마다 중간 저장
OUT_COLUMNS = [
    "corp_code", "fiscal_year", "reprt_code", "fs_div",
    "revenue", "op_income", "net_income",
    "total_assets", "total_liab", "equity",
    "ocf", "fcf"
]

# 계정명 매핑(여러 명칭 대응)
ACCOUNT_MAP = {
    "revenue": {"매출액", "영업수익", "수익(매출액)"},
    "op_income": {"영업이익"},
    "net_income": {"당기순이익", "분기순이익", "반기순이익"},
    "total_assets": {"자산총계", "총자산"},
    "total_liab": {"부채총계", "총부채"},
    "equity": {"자본총계", "지배기업 소유주지분", "자본과부채총계-부채총계"},  # 자본총계가 기본
    "ocf": {"영업활동현금흐름"},
    # FCF는 공시 표준 계정이 아님(보통 OCF - CapEx로 추정). 여기선 None 유지.
}

def _to_number(x: Optional[str]) -> Optional[float]:
    """
    "1,234", "(1,234)" 등 문자열 금액을 부호 포함 float로 변환.
    """
    if x is None:
        return None
    s = str(x).strip()
    if s == "" or s == "-":
        return None
    neg = False
    if s.startswith("(") and s.endswith(")"):
        neg = True
        s = s[1:-1]
    s = s.replace(",", "")
    try:
        v = float(s)
        return -v if neg else v
    except Exception:
        return None

def _normalize_row(df_accounts: pd.DataFrame) -> Dict[str, Optional[float]]:
    """
    단일 회사-연도-보고서-구분(CFS/OFS)의 계정 테이블에서 핵심 지표를 추출.
    """
    out = {k: None for k in ["revenue","op_income","net_income",
                             "total_assets","total_liab","equity","ocf","fcf"]}
    if df_accounts.empty:
        return out

    # account_nm 컬럼을 표준화 후 매핑
    names = df_accounts["account_nm"].astype(str).str.replace("\u3000", " ", regex=False).str.strip()

    for key, aliases in ACCOUNT_MAP.items():
        # 여러 행 중 첫 매칭 우선
        mask = names.isin(aliases)
        if mask.any():
            val = df_accounts.loc[mask, "thstrm_amount"].iloc[0]
            out[key] = _to_number(val)

    # equity 대체 로직(자본총계가 없고 자산/부채가 있으면 자산-부채로 보정 시도)
    if out["equity"] is None and out["total_assets"] is not None and out["total_liab"] is not None:
        out["equity"] = out["total_assets"] - out["total_liab"]

    # FCF는 정보 부족으로 계산 보류(향후 CapEx 확보시: OCF - CapEx)
    out["fcf"] = None

    return out

def _fetch_single_fs(client: DartClient, corp_code: str, year: int,
                     reprt_code: str, fs_div: str) -> pd.DataFrame:
    """
    단일 회사/연도/보고서/구분에 대한 주요계정 테이블 호출(페이지 순회 포함).
    반환: 해당 조건의 전체 계정 rows(DataFrame)
    """
    all_pages = []
    page_no = 1
    while True:
        j = client.get("fnlttSinglAcntAll", {
            "corp_code": corp_code,
            "bsns_year": str(year),
            "reprt_code": reprt_code,
            "fs_div": fs_div,      # CFS=연결, OFS=별도
            "page_no": page_no,
            "page_count": PAGE_COUNT
        })
        status = str(j.get("status", ""))
        if status != "013":  # 013: 조회된 데이터가 없습니다 (정상 무데이터). '000'이 정상 데이터.
            # pass; 대부분 "000" 이거나 "013"
            pass
        if status not in {"000", "013"}:
            # 기타 에러는 빈 DF로 처리(상태 코드 다양성 때문)
            break

        list_data = j.get("list", [])
        if not list_data:
            break
        df = pd.DataFrame(list_data)
        all_pages.append(df)

        # 다음 페이지
        total_count = int(j.get("total_count", len(list_data)))
        max_page = math.ceil(total_count / PAGE_COUNT) if total_count else page_no
        if page_no >= max_page:
            break
        page_no += 1
        time.sleep(SLEEP_SEC)

    if not all_pages:
        return pd.DataFrame(columns=["account_nm","thstrm_amount"])
    out = pd.concat(all_pages, ignore_index=True)
    # 필요한 컬럼만 유지
    keep = [c for c in out.columns if c in {"account_nm","thstrm_amount"}]
    return out[keep].copy()

def _fetch_one_company_year(client: DartClient, corp_code: str, year: int) -> List[Dict]:
    """
    회사+연도에 대해 REPRT_CODES × FS_DIV_PRIORITY 순서로 조회하여
    첫 유의미 결과를 표준화한 1~N행으로 반환(보통 사업보고서 1행).
    """
    rows = []
    for reprt_code in REPRT_CODES:
        found = False
        for fs_div in FS_DIV_PRIORITY:
            df_raw = _fetch_single_fs(client, corp_code, year, reprt_code, fs_div)
            if df_raw.empty:
                continue
            metrics = _normalize_row(df_raw)
            # 핵심 3종이라도 있으면 유의미한 것으로 간주
            if any(metrics.get(k) is not None for k in ("revenue","op_income","net_income")):
                row = {
                    "corp_code": corp_code,
                    "fiscal_year": year,
                    "reprt_code": reprt_code,
                    "fs_div": fs_div,
                    **metrics
                }
                rows.append(row)
                found = True
                break
        # 사업보고서(연간)에서 이미 확보되면 다음 보고서들은 생략(중복 방지)
        if found:
            break
    return rows

def _load_corp_master(path: str) -> pd.DataFrame:
    if not os.path.exists(path):
        raise FileNotFoundError(
            f"corp_master not found: {path}. 먼저 `python run_pipeline.py bootstrap` 실행하세요."
        )
    df = pd.read_parquet(path)
    # 최소 필드 보장
    need = {"corp_code","corp_name","stock_code","is_listed"}
    missing = need.difference(df.columns)
    for m in missing:
        df[m] = None
    return df

def backfill_financials(env: dict, start_year: int, end_year: int, out_path: str):
    """
    전체 회사(상장/비상장 포함) × 연도 루프.
    - 연결(CFS) 우선, 불가 시 별도(OFS)로 대체
    - 사업보고서(연간) 우선
    - 중간 체크포인트 저장
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    client = DartClient(env["DART_API_KEY"], sleep_sec=SLEEP_SEC)

    # 회사 마스터 로드
    corp_master_path = "data/corp_master.parquet"
    dim = _load_corp_master(corp_master_path)

    # 반복 준비
    all_rows: List[Dict] = []
    total_tasks = len(dim) * (end_year - start_year + 1)
    done = 0

    for year in range(start_year, end_year + 1):
        for _, r in dim.iterrows():
            corp_code = str(r["corp_code"])
            try:
                rows = _fetch_one_company_year(client, corp_code, year)
                if rows:
                    all_rows.extend(rows)
            except Exception as e:
                # 개별 회사 에러는 스킵(로그만 콘솔)
                print(f"[WARN] fail corp={corp_code} year={year}: {e}")

            done += 1
            if done % 200 == 0:
                print(f"[INFO] progress {done}/{total_tasks} ({done/total_tasks:.1%})")

            if done % CHECKPOINT_EVERY == 0:
                _write_checkpoint(all_rows, out_path, mode="ab")

    # 최종 저장
    _write_checkpoint(all_rows, out_path, mode="wb")
    print(f"[OK] financials saved: {out_path}")

def _write_checkpoint(rows: List[Dict], out_path: str, mode: str = "wb"):
    """
    누적 rows를 파케이로 저장(헤더 스키마를 OUT_COLUMNS에 맞춤).
    mode:
      - "wb": 새로쓰기(최종 저장)
      - "ab": 중간체크포인트(기존 파일 있으면 append 형태로 병합 저장)
    """
    if not rows:
        # 빈 데이터라도 스키마 헤더 유지를 위해 빈 DF 저장
        if not os.path.exists(out_path):
            pd.DataFrame(columns=OUT_COLUMNS).to_parquet(out_path, index=False)
        return

    df = pd.DataFrame(rows)
    # 컬럼 정렬/보정
    for c in OUT_COLUMNS:
        if c not in df.columns:
            df[c] = None
    df = df[OUT_COLUMNS]

    if mode == "wb" or not os.path.exists(out_path):
        df.to_parquet(out_path, index=False)
    else:
        # 기존과 병합 저장
        old = pd.read_parquet(out_path) if os.path.exists(out_path) else pd.DataFrame(columns=OUT_COLUMNS)
        merged = pd.concat([old, df], ignore_index=True)
        # 중복 제거(동일 키 우선순위: 나중 것 우선)
        merged.drop_duplicates(subset=["corp_code","fiscal_year","reprt_code","fs_div"], keep="last", inplace=True)
        merged.to_parquet(out_path, index=False)
