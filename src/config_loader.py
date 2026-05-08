import os
import yaml
from dotenv import load_dotenv

load_dotenv()

def load_config():
    BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    with open(os.path.join(BASE_DIR, "config.yaml"), "r") as f:
        config = yaml.safe_load(f)

    # Database — .env öncelikli, config.yaml fallback
    config["database"]["host"]     = os.getenv("DB_HOST",     config["database"]["host"])
    config["database"]["port"]     = int(os.getenv("DB_PORT", config["database"]["port"]))
    config["database"]["name"]     = os.getenv("DB_NAME",     config["database"]["name"])
    config["database"]["user"]     = os.getenv("DB_USER",     config["database"]["user"])
    config["database"]["password"] = os.getenv("DB_PASSWORD", config["database"]["password"])

    # SMTP — .env'de tanımlıysa config.yaml'ın üzerine yaz
    if "smtp" in config:
        config["smtp"]["user"]     = os.getenv("SMTP_USER",     config["smtp"].get("user", ""))
        config["smtp"]["password"] = os.getenv("SMTP_PASS",     config["smtp"].get("password", ""))
        config["smtp"]["to"]       = os.getenv("SMTP_TO",       config["smtp"].get("to", ""))

    return config