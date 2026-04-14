import os
import yaml
from dotenv import load_dotenv

load_dotenv()

def load_config():
    with open("config.yaml", "r") as f:
        config = yaml.safe_load(f)

    # ENV override (Docker / local farkı)
    config["database"]["host"] = os.getenv("DB_HOST", config["database"]["host"])
    config["database"]["port"] = int(os.getenv("DB_PORT", config["database"]["port"]))
    config["database"]["name"] = os.getenv("DB_NAME", config["database"]["name"])
    config["database"]["user"] = os.getenv("DB_USER", config["database"]["user"])
    config["database"]["password"] = os.getenv("DB_PASSWORD", config["database"]["password"])

    return config