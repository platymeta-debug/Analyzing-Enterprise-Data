import argparse
from common.config import load_env
from ingest.corp_master import fetch_and_save_corp_master
from ingest.fin_statements import backfill_financials
from ingest.events import backfill_events
from ingest.prices import build_mcap_snapshot
from transform.gics_map import apply_gics_mapping
from export.excel_book import build_excel_book

def main():
    parser = argparse.ArgumentParser(
        description="OpenDART + GICS Analytics Pipeline"
    )
    sub = parser.add_subparsers(dest="cmd", required=True)

    p_boot = sub.add_parser("bootstrap", help="Download corp master (corp_code.xml)")
    p_boot.add_argument("--out", default="data/corp_master.parquet")

    p_fin = sub.add_parser("backfill_financials", help="Backfill financial statements (2015~)")
    p_fin.add_argument("--start", type=int, default=2015)
    p_fin.add_argument("--end", type=int, default=2025)
    p_fin.add_argument("--out", default="data/fin_statements.parquet")

    p_evt = sub.add_parser("backfill_events", help="Backfill major events (defaults last 10y)")
    p_evt.add_argument("--years", type=int, default=10)
    p_evt.add_argument("--out", default="data/events.parquet")

    # NEW: mcap snapshot
    p_mcap = sub.add_parser("build_mcap", help="Build market cap snapshot for a given date (YYYY-MM-DD)")
    p_mcap.add_argument("--date", required=True, help="Reference date like 2024-12-31")
    p_mcap.add_argument("--out", default="data/mcap_snapshot.parquet")

    p_gics = sub.add_parser("apply_gics", help="Apply GICS classification to corp master")
    p_gics.add_argument("--corp", default="data/corp_master.parquet", help="Input corp master parquet")
    p_gics.add_argument("--out", default="data/corp_master.parquet", help="Output parquet with GICS columns")
    p_gics.add_argument("--mapping", default="data/gics_mapping.csv", help="Optional CSV mapping table")

    p_xls = sub.add_parser("export_excel", help="Build Excel book from current snapshots")
    p_xls.add_argument("--fin", default="data/fin_statements.parquet")
    p_xls.add_argument("--events", default="data/events.parquet")
    p_xls.add_argument("--corp", default="data/corp_master.parquet")
    p_xls.add_argument("--mcap", default="data/mcap_snapshot.parquet")
    p_xls.add_argument("--macro", default="data/macro_panel.parquet")
    p_xls.add_argument("--out", default="output/Corporate_Macro_Dashboard.xlsx")
    p_xls.add_argument("--year", type=int, default=2024)

    args = parser.parse_args()
    env = load_env()

    if args.cmd == "bootstrap":
        fetch_and_save_corp_master(env, args.out)
    elif args.cmd == "backfill_financials":
        backfill_financials(env, start_year=args.start, end_year=args.end, out_path=args.out)
    elif args.cmd == "backfill_events":
        backfill_events(env, years=args.years, out_path=args.out)
    elif args.cmd == "build_mcap":
        build_mcap_snapshot(env, date_ref=args.date, out_path=args.out)
    elif args.cmd == "apply_gics":
        apply_gics_mapping(args.corp, args.out, mapping_csv=args.mapping)
    elif args.cmd == "export_excel":
        build_excel_book(
            env,
            fin_path=args.fin,
            events_path=args.events,
            corp_path=args.corp,
            mcap_path=args.mcap,
            macro_path=args.macro,
            out_path=args.out,
            focus_year=args.year,
        )

if __name__ == "__main__":
    main()
