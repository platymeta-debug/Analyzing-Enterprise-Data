import io, zipfile, xml.etree.ElementTree as ET, os
import pandas as pd
from common.dart_client import DartClient

def fetch_and_save_corp_master(env: dict, out_path: str):
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    client = DartClient(env["DART_API_KEY"])
    blob = client.get_corp_code_zip()

    with zipfile.ZipFile(io.BytesIO(blob)) as zf:
        # The XML file is usually named CORPCODE.xml inside.
        xml_name = [n for n in zf.namelist() if n.lower().endswith(".xml")][0]
        xml_bytes = zf.read(xml_name)
    root = ET.fromstring(xml_bytes)

    rows = []
    for el in root.findall("list"):
        rows.append({
            "corp_code": el.findtext("corp_code"),
            "corp_name": el.findtext("corp_name"),
            "stock_code": el.findtext("stock_code"),
            "modify_date": el.findtext("modify_date"),
        })
    df = pd.DataFrame(rows)
    # Derive simple flags
    df["is_listed"] = df["stock_code"].notna() & (df["stock_code"].str.len() > 0)
    df.to_parquet(out_path, index=False)
    print(f"[OK] corp_master saved: {out_path}, rows={len(df)}")
