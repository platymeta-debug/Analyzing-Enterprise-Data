import os
from dotenv import load_dotenv

def load_env():
    # Load .env if present; environment variables override .env
    load_dotenv(override=False)
    cfg = {
        "DART_API_KEY": os.getenv("DART_API_KEY", ""),
        "ECOS_API_KEY": os.getenv("ECOS_API_KEY", ""),
        "KOSIS_API_KEY": os.getenv("KOSIS_API_KEY", ""),
        "BASE_CCY": os.getenv("BASE_CCY", "KRW"),
        "PRICE_SOURCE": os.getenv("PRICE_SOURCE", "krx"),
        "LOG_LEVEL": os.getenv("LOG_LEVEL", "INFO"),
    }
    missing = [k for k, v in cfg.items() if k.endswith("_API_KEY") and not v]
    if missing:
        print(f"[WARN] Missing API keys in env: {missing}. Some steps may fail.")
    return cfg
