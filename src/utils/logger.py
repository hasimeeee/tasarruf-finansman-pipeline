import logging
import os


def get_logger(name=__name__, log_file=None):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    log_dir  = os.path.join(base_dir, "logs")          # FIX: absolute path, relative değil

    if log_file is None:
        log_file = os.path.join(log_dir, "pipeline.log")

    os.makedirs(log_dir, exist_ok=True)                # FIX: log_dir kullan, "logs" string'i değil

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.propagate = False

    return logger