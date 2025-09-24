import os
import pandas as pd
from common.dart_client import DartClient

# NOTE: This is a minimal placeholder that sets up the structure.
# You will implement DS002/DS003 endpoints (fnlttSinglAcntAll etc.) in the next pass.

def backfill_financials(env: dict, start_year: int, end_year: int, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    # Placeholder empty frame with expected columns
    cols = [
        "corp_code","fiscal_year","reprt_code","consolidated",
        "revenue","op_income","net_income",
        "total_assets","total_liab","equity","ocf","fcf"
    ]
    df = pd.DataFrame(columns=cols)
    df.to_parquet(out_path, index=False)
    print(f"[OK] (stub) fin_statements saved: {out_path} (0 rows).")
