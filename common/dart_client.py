import time
import requests

BASE_URL = "https://opendart.fss.or.kr/api"

class DartClient:
    def __init__(self, api_key: str, sleep_sec: float = 0.2, max_retries: int = 3):
        self.api_key = api_key
        self.sleep_sec = sleep_sec
        self.max_retries = max_retries

    def get(self, endpoint: str, params: dict):
        url = f"{BASE_URL}/{endpoint}.json"
        p = {"crtfc_key": self.api_key}
        p.update(params or {})
        for attempt in range(self.max_retries):
            r = requests.get(url, params=p, timeout=30)
            if r.status_code == 200:
                return r.json()
            time.sleep(self.sleep_sec * (attempt + 1))
        r.raise_for_status()

    def get_corp_code_zip(self):
        # corpCode is served as ZIP (XML inside). Use the special path.
        url = f"{BASE_URL}/corpCode.xml"
        p = {"crtfc_key": self.api_key}
        r = requests.get(url, params=p, timeout=60)
        r.raise_for_status()
        return r.content

    def get_ok(self, endpoint: str, params: dict):
        """status=='000'만 반환, 그 외는 빈 dict"""
        j = self.get(endpoint, params)
        if str(j.get("status","")) == "000":
            return j
        return {}
