import logging
import logging
import os

def get_logger(name=__name__):
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s"
    )
    return logging.getLogger(name)
def get_logger(name=__name__, log_file="logs/pipeline.log"):
    os.makedirs("logs", exist_ok=True)
    
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    
    # Konsola yaz
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)
    
    # Dosyaya yaz
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)
    
    return logger