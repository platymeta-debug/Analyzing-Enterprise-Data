import os
import math
import time
import datetime as dt
from typing import Dict, List, Optional, Tuple

import pandas as pd

from common.dart_client import DartClient

# ---------------------------------------
# 설정(필요시 조정)
# ---------------------------------------
PAGE_COUNT = 100
SLEEP_SEC = 0.15
CHECKPOINT_EVERY = 5000  # N건마다 중간 저장
WINDOW_DAYS = 90         # DART list 조회 기간 분할(권장 3개월 단위)

OUT_COLUMNS = [
    "rcp_no", "corp_code", "event_date", "event_type", "sub_type",
    "amount", "counterparty", "summary",
    "report_nm", "rcept_dt"
]

# ---------------------------------------
# 이벤트 규칙(Report Name 기반 매핑)
#  - DART 'list' API의 report_nm(보고서명)을 키워드로 표준 타입 분류
#  - 금액/상대방은 상세 API/문서 파싱 단계(2.5단계)에서 보강 예정
# ---------------------------------------
EVENT_RULES: List[Tuple[str, str, Optional[str]]] = [
    # (키워드, 표준 event_type, sub_type)
    ("부도발생",           "DEFAULT",       None),
    ("어음부도",           "DEFAULT",       "BILL"),
    ("영업정지",           "OPS_SUSPEND",   None),
    ("회생절차",           "REHAB",         None),
    ("법정관리",           "REHAB",         "COURT"),
    ("해산사유",           "LIQUIDATION",   None),
    ("청산",               "LIQUIDATION",   "WINDING_UP"),
    ("채권은행 관리",      "BANK_GROUP",    None),
    ("소송의 제기",        "LITIGATION",    "FILED"),
    ("소송의 판결",        "LITIGATION",    "JUDGMENT"),
    ("합병",               "MNA",           "MERGER"),
    ("분할합병",           "MNA",           "MERGER_SPLIT"),
    ("분할",               "MNA",           "SPLIT"),
    ("주식교환",           "MNA",           "STOCK_SWAP"),
    ("주식이전",           "MNA",           "STOCK_TRANSFER"),
    ("영업양수",           "BIZ_ACQ",       "ACQ"),
    ("영업양도",           "BIZ_DISP",      "DISP"),
    ("자산양수",           "ASSET_ACQ",     "ACQ"),
    ("자산양도",           "ASSET_DISP",    "DISP"),
    ("유형자산 양수",      "ASSET_ACQ",     "PPE"),
    ("유형자산 양도",      "ASSET_DISP",    "PPE"),
    ("타법인 주식 및 출자증권 양수", "EQUITY_ACQ", "ACQ"),
    ("타법인 주식 및 출자증권 양도", "EQUITY_DISP", "DISP"),
]

def _classify_event(report_nm: str) -> Tuple[Optional[str], Optional[str]]:
    """보고서명(report_nm)에서 event_type/sub_type 분류."""
    if not isinstance(report_nm, str):
        return (None, None)
    name = report_nm.strip()
    for kw, etype, sub in EVENT_RULES:
        if kw in name:
            return (etype, sub)
    # 대체: '주요사항보고서' 등 포괄 명칭만 있는 경우
    if "주요사항보고서" in name:
        return ("MAJOR", None)
    return (None, None)


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


def _daterange_chunks(bgn: dt.date, end: dt.date, step_days: int):
    cur = bgn
    one = dt.timedelta(days=step_days)
    while cur <= end:
        nxt = min(cur + one - dt.timedelta(days=1), end)
        yield (cur, nxt)
        cur = nxt + dt.timedelta(days=1)


def _fetch_list_for_company(client: DartClient, corp_code: str,
                            bgn_de: str, end_de: str) -> List[Dict]:
    """DART list API 한 회사에 대해 기간 내 페이지네이션 수집."""
    rows = []
    page_no = 1
    while True:
        j = client.get("list", {
            "corp_code": corp_code,
            "bgn_de": bgn_de,   # YYYYMMDD
            "end_de": end_de,   # YYYYMMDD
            "page_no": page_no,
            "page_count": PAGE_COUNT
        })
        status = str(j.get("status", ""))
        if status not in {"000", "013"}:
            # 예외 상태는 빈 결과 처리
            break
        lst = j.get("list", [])
        if not lst:
            break

        for it in lst:
            rows.append({
                "rcept_no": it.get("rcept_no", ""),      # 접수번호
                "corp_code": it.get("corp_code", ""),
                "corp_name": it.get("corp_name", ""),
                "rcept_dt": it.get("rcept_dt", ""),      # YYYYMMDD
                "report_nm": it.get("report_nm", ""),
            })

        total_count = int(j.get("total_count", len(lst)))
        max_page = math.ceil(total_count / PAGE_COUNT) if total_count else page_no
        if page_no >= max_page:
            break
        page_no += 1
        time.sleep(SLEEP_SEC)
    return rows


def backfill_events(env: dict, years: int, out_path: str):
    """
    최근 N년 동안의 'list' 공시를 전사 스캔하여 이벤트 후보를 정규화.
    - report_nm을 기반으로 표준 event_type/sub_type 태깅
    - 금액/상대방/요약은 후속(2.5단계)에서 상세 파싱으로 보강 예정
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    client = DartClient(env["DART_API_KEY"], sleep_sec=SLEEP_SEC)

    # 기간 계산
    end_date = dt.date.today()
    start_date = end_date - dt.timedelta(days=365*years)

    # 회사 마스터
    dim = _load_corp_master("data/corp_master.parquet")

    all_rows = []
    total_tasks = len(dim)
    for idx, r in dim.reset_index(drop=True).iterrows():
        corp_code = str(r["corp_code"])
        # 기간을 90일 윈도우로 쪼개어 조회
        for bgn, ed in _daterange_chunks(start_date, end_date, WINDOW_DAYS):
            bgn_de = bgn.strftime("%Y%m%d")
            end_de = ed.strftime("%Y%m%d")
            try:
                lst = _fetch_list_for_company(client, corp_code, bgn_de, end_de)
            except Exception as e:
                print(f"[WARN] list fail corp={corp_code} {bgn_de}~{end_de}: {e}")
                continue

            # 이벤트 후보만 필터(키워드 매칭)
            for it in lst:
                report_nm = it["report_nm"] or ""
                etype, sub = _classify_event(report_nm)
                if etype is None:
                    continue  # 관심 없는 일반 공시는 스킵

                rcept_dt = it.get("rcept_dt", "")
                # 날짜 정규화
                try:
                    event_date = dt.datetime.strptime(rcept_dt, "%Y%m%d").date().isoformat()
                except Exception:
                    event_date = None

                all_rows.append({
                    "rcp_no": it["rcept_no"],
                    "corp_code": it["corp_code"],
                    "event_date": event_date,
                    "event_type": etype,
                    "sub_type": sub,
                    "amount": None,         # 2.5단계에서 상세 파싱으로 보강
                    "counterparty": None,   # 2.5단계에서 상세 파싱으로 보강
                    "summary": None,        # 2.5단계에서 상세 파싱 또는 NLP로 보강
                    "report_nm": report_nm,
                    "rcept_dt": rcept_dt,
                })

            # 체크포인트 저장
            if len(all_rows) and len(all_rows) % CHECKPOINT_EVERY == 0:
                _write_checkpoint(all_rows, out_path, mode="ab")

        if (idx + 1) % 100 == 0:
            print(f"[INFO] company progress {idx+1}/{total_tasks} ({(idx+1)/total_tasks:.1%})")

    # 최종 저장
    _write_checkpoint(all_rows, out_path, mode="wb")
    print(f"[OK] events saved: {out_path}, rows={len(all_rows)}")


def _write_checkpoint(rows: List[Dict], out_path: str, mode: str = "wb"):
    """rows를 파케이로 저장(헤더 스키마를 OUT_COLUMNS로 정렬)."""
    if not rows:
        if not os.path.exists(out_path):
            pd.DataFrame(columns=OUT_COLUMNS).to_parquet(out_path, index=False)
        return
    df = pd.DataFrame(rows)
    for c in OUT_COLUMNS:
        if c not in df.columns:
            df[c] = None
    df = df[OUT_COLUMNS]
    if mode == "wb" or not os.path.exists(out_path):
        df.to_parquet(out_path, index=False)
    else:
        old = pd.read_parquet(out_path) if os.path.exists(out_path) else pd.DataFrame(columns=OUT_COLUMNS)
        merged = pd.concat([old, df], ignore_index=True)
        merged.drop_duplicates(subset=["rcp_no","corp_code"], keep="last", inplace=True)
        merged.to_parquet(out_path, index=False)
