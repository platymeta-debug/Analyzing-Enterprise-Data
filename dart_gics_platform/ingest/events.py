import os
import pandas as pd

# Placeholder for major events ingestion (부도/영업정지/회생 절차 등).
# Next pass: call DART list + detail endpoints and normalize.

def backfill_events(env: dict, years: int, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    cols = [
        "rcp_no","corp_code","event_date","event_type","sub_type",
        "amount","counterparty","summary"
    ]
    df = pd.DataFrame(columns=cols)
    df.to_parquet(out_path, index=False)
    print(f"[OK] (stub) events saved: {out_path} (0 rows).")
