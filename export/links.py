def naver_finance_url(stock_code: str) -> str:
    """
    네이버 금융 종목 상세 링크(국내 6자리 종목코드 기준).
    """
    if not stock_code:
        return ""
    code = str(stock_code).zfill(6)
    return f"https://finance.naver.com/item/main.naver?code={code}"


def dart_search_url(corp_name: str) -> str:
    """
    DART 공시검색(회사명 키워드) 링크. 정확한 회사 페이지가 아닌 검색 결과로 이동.
    """
    if not corp_name:
        return ""
    from urllib.parse import quote
    q = quote(corp_name)
    return f"https://dart.fss.or.kr/dsac001/search.ax?textCrpNm={q}"
