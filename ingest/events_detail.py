import os
import io
import re
import zipfile
import datetime as dt
from typing import Dict, List, Optional, Tuple

import pandas as pd
from bs4 import BeautifulSoup

from common.dart_client import DartClient

# --------------------------
# 설정값
# --------------------------
MAX_DOCS_PER_EVENT = 6      # rcp_no 당 확인할 첨부/본문 최대 개수
TEXT_BYTES_LIMIT    = 2_000_000  # 너무 큰 파일은 스킵(2MB)
SUMMARY_LEN         = 160    # 요약 최대 길이(문장 자르기)
AMOUNT_KEYS = [
    r"취득금액", r"양수도[ ]?금액", r"거래[ ]?금액", r"총[ ]?투자[ ]?금액",
    r"자산[ ]?양수[ ]?금액", r"영업[ ]?양수[ ]?대가", r"주식[ ]?취득[ ]?대금",
    r"부도[ ]?금액", r"손해배상[ ]?금액"
]
COUNTERPARTY_KEYS = [
    r"거래상대방", r"상대[ ]?회사", r"상대[ ]?법인", r"양수인", r"양도인", r"피인수[ ]?회사",
    r"채권은행", r"법원", r"소송[ ]?상대방", r"관리[ ]?주체"
]
# 숫자 패턴 (1,234,567 또는 123억 4,567만원 등)
NUM_PAT = re.compile(r"[-]?\(?\d{1,3}(?:,\d{3})*(?:\.\d+)?\)?|(\d+)\s*억|\d+\s*원")

def _clean_text(html_or_xml_bytes: bytes) -> str:
    if not html_or_xml_bytes:
        return ""
    # HTML/XML을 soup로 파싱 후 텍스트 추출
    soup = BeautifulSoup(html_or_xml_bytes, "lxml")
    # 테이블 텍스트도 포함해서 추출
    text = soup.get_text("\n", strip=True)
    # 공백 정리
    text = re.sub(r"[ \t]+", " ", text)
    text = re.sub(r"\n{2,}", "\n", text)
    return text

def _take_summary(text: str, max_len: int = SUMMARY_LEN) -> str:
    if not text:
        return None
    # 첫 문단/문장 중심으로 요약
    para = text.split("\n")[0]
    if len(para) > max_len:
        para = para[:max_len].rstrip() + "…"
    return para

def _find_first_number_near(text: str, key_regexes: List[str]) -> Optional[str]:
    # 키워드 근처(+/- 80자)에서 숫자 후보 추출
    for kr in key_regexes:
        for m in re.finditer(kr, text):
            s_idx = max(0, m.start() - 80)
            e_idx = min(len(text), m.end() + 80)
            window = text[s_idx:e_idx]
            nm = re.search(NUM_PAT, window)
            if nm:
                return nm.group(0)
    return None

def _find_counterparty(text: str, key_regexes: List[str]) -> Optional[str]:
    for kr in key_regexes:
        for m in re.finditer(kr, text):
            s_idx = m.end()
            # 키워드 다음 40자 이내에서 괄호/따옴표/콜론 뒤 텍스트를 상대방으로 추정
            window = text[s_idx:s_idx+80]
            # 예: "거래상대방: ㈜OOO" / "상대회사 ㈜OOO" / "(주)OOO"
            m2 = re.search(r"[:：]\s*([^\n]+)", window) or re.search(r"[『「(]\s*([^)\n]{2,40})", window)
            if m2:
                cand = m2.group(1).strip()
                # 너무 긴 건 자름
                cand = re.split(r"[，,;/\n]", cand)[0].strip()
                return cand
    return None

def _extract_from_text(text: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    if not text:
        return (None, None, None)
    amount = _find_first_number_near(text, AMOUNT_KEYS)
    counter = _find_counterparty(text, COUNTERPARTY_KEYS)
    summary = _take_summary(text, SUMMARY_LEN)
    return (amount, counter, summary)

# --------------------------
# document.xml 파서
# --------------------------
def _parse_document_xml(xml_bytes: bytes) -> List[Dict]:
    """
    document.xml의 첨부/본문 목록을 파싱하여 다운로드 가능한 URL 후보 리스트를 반환.
    케이스별로 XML 구조가 상이할 수 있어, <fileName>/<url> 등 일반적인 필드를 폭넓게 탐색.
    """
    soup = BeautifulSoup(xml_bytes, "xml")
    items = []
    # 가장 일반적인 구조: <list> 요소들
    for node in soup.find_all(["list", "item", "attached"]):
        name = None
        url  = None
        for tag in ["fileName", "filename", "title", "name"]:
            el = node.find(tag)
            if el and el.text:
                name = el.text.strip()
                break
        for tag in ["url", "fileUrl", "downloadUrl"]:
            el = node.find(tag)
            if el and el.text:
                url = el.text.strip()
                break
        if name or url:
            items.append({"name": name, "url": url})
    # 중복 제거
    uniq = []
    seen = set()
    for it in items:
        k = (it.get("name"), it.get("url"))
        if k not in seen:
            uniq.append(it)
            seen.add(k)
    return uniq

def _viewer_urls(rcept_no: str) -> List[str]:
    """
    document.xml이 빈약한 경우를 대비한 뷰어 폴백 URL 후보.
    이 URL들은 HTML을 반환하며 텍스트 파싱 대상으로 쓸 수 있다.
    """
    return [
        f"https://dart.fss.or.kr/report/viewer.do?rcpNo={rcept_no}",
        f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}",
    ]

# --------------------------
# 메인: events 상세 보강
# --------------------------
def enrich_events_detail(env: dict, events_in: str, events_out: str):
    """
    events_in(parquet)을 읽어, rcp_no 별로 document.xml/뷰어 HTML을 조회.
    금액/상대방/요약을 베스트에포트로 채워서 events_out에 저장.
    기존 amount/counterparty/summary 값이 이미 있으면 덮어쓰지 않음(보수적).
    """
    os.makedirs(os.path.dirname(events_out), exist_ok=True)
    if not os.path.exists(events_in):
        raise FileNotFoundError(f"events parquet not found: {events_in}")

    df = pd.read_parquet(events_in)
    if df.empty:
        df.to_parquet(events_out, index=False)
        print(f"[OK] events(empty) saved: {events_out}")
        return

    client = DartClient(env["DART_API_KEY"])

    # rcp_no 유니크
    df["_amount"] = df.get("amount")
    df["_counter"] = df.get("counterparty")
    df["_summary"] = df.get("summary")
    keys = df["rcp_no"].dropna().astype(str).unique().tolist()

    def try_enrich_one(rcp_no: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
        # 1) document.xml 시도
        try:
            xml_bytes = client.get_document_xml(rcp_no)
        except Exception:
            xml_bytes = None

        texts: List[str] = []
        if xml_bytes:
            items = _parse_document_xml(xml_bytes)[:MAX_DOCS_PER_EVENT]
            for it in items:
                url = it.get("url")
                if not url:
                    continue
                try:
                    blob = client.get_binary(url)
                    if not blob or len(blob) > TEXT_BYTES_LIMIT:
                        continue
                    txt = _clean_text(blob)
                    if txt:
                        texts.append(txt)
                except Exception:
                    continue

        # 2) 폴백: 뷰어 HTML
        if not texts:
            for url in _viewer_urls(rcp_no):
                try:
                    blob = client.get_binary(url)
                    if not blob or len(blob) > TEXT_BYTES_LIMIT:
                        continue
                    txt = _clean_text(blob)
                    if txt and ("주요사항" in txt or "금액" in txt or "거래" in txt):
                        texts.append(txt)
                        break
                except Exception:
                    continue

        # 텍스트들에서 규칙 추출
        best_amount, best_counter, best_summary = (None, None, None)
        for t in texts:
            a, c, s = _extract_from_text(t)
            best_amount = best_amount or a
            best_counter = best_counter or c
            best_summary = best_summary or s
            if best_amount and best_counter and best_summary:
                break
        return (best_amount, best_counter, best_summary)

    # 실제 보강
    enriched = {}
    for rcp in keys:
        a, c, s = try_enrich_one(rcp)
        enriched[rcp] = {"amount": a, "counterparty": c, "summary": s}

    # 병합(기존 값이 있으면 유지)
    out_rows = []
    for i, r in df.iterrows():
        rcp = str(r["rcp_no"])
        a0 = r.get("amount")
        c0 = r.get("counterparty")
        s0 = r.get("summary")
        add = enriched.get(rcp, {})
        row = dict(r)
        row["amount"] = a0 if pd.notna(a0) and a0 not in ("", None) else add.get("amount")
        row["counterparty"] = c0 if pd.notna(c0) and c0 not in ("", None) else add.get("counterparty")
        row["summary"] = s0 if pd.notna(s0) and s0 not in ("", None) else add.get("summary")
        out_rows.append(row)

    out = pd.DataFrame(out_rows)
    out.drop(columns=[c for c in ["_amount","_counter","_summary"] if c in out.columns], inplace=True, errors="ignore")
    out.to_parquet(events_out, index=False)
    print(f"[OK] events(detail-enriched) saved: {events_out}, rows={len(out)}")
