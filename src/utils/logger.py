import logging
import os


def get_logger(name=__name__, log_file=None):
    # Script'in bulunduğu dizine göre absolute path üret
    base_dir = os.path.dirname(os.path.abspath(__file__))
    if log_file is None:
        log_file = os.path.join(base_dir, "logs", "pipeline.log")
    os.makedirs("logs", exist_ok=True)

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)

    # Aynı logger'a birden fazla handler eklenmesini önle
    # (modül birden fazla kez import edildiğinde oluşur)
    if logger.handlers:
        return logger

    fmt = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")

    # Konsola yaz
    ch = logging.StreamHandler()
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    # Dosyaya yaz
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Root logger'a iletmeyi kapat — basicConfig ile çakışmayı önler
    logger.propagate = False

    return logger