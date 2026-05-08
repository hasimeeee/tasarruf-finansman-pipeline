import os
import yaml
from dotenv import load_dotenv

load_dotenv()

def load_config():
    # config_loader.py src/ icinde, config.yaml proje kokunde
    # BASE_DIR = src/ -> bir ust dizin = proje koku
    BASE_DIR    = os.path.dirname(os.path.abspath(__file__))
    PROJECT_ROOT = os.path.dirname(BASE_DIR)
    config_path = os.path.join(PROJECT_ROOT, "config.yaml")

    if not os.path.exists(config_path):
        raise FileNotFoundError(f"config.yaml bulunamadi: {config_path}")

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    config["database"]["host"]     = os.getenv("DB_HOST",     config["database"]["host"])
    config["database"]["port"]     = int(os.getenv("DB_PORT", config["database"]["port"]))
    config["database"]["name"]     = os.getenv("DB_NAME",     config["database"]["name"])
    config["database"]["user"]     = os.getenv("DB_USER",     config["database"]["user"])
    config["database"]["password"] = os.getenv("DB_PASSWORD", config["database"]["password"])

    if "smtp" in config:
        config["smtp"]["user"]     = os.getenv("SMTP_USER", config["smtp"].get("user", ""))
        config["smtp"]["password"] = os.getenv("SMTP_PASS", config["smtp"].get("password", ""))
        config["smtp"]["to"]       = os.getenv("SMTP_TO",   config["smtp"].get("to", ""))

    return config