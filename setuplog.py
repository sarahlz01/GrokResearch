# setuplog.py
from pathlib import Path
from datetime import datetime
import logging, os, sys

def setup_logging(run_name: str = "run", log_dir: str = "logs", level: str | None = None, to_stdout: bool = True):
    """
    Configure root logger once per process. Returns the Path of the log file.
    If logging is already configured, reuses existing handlers and returns RUN_LOG_PATH if set.
    """
    root = logging.getLogger()
    if root.handlers:
        p = os.getenv("RUN_LOG_PATH")
        return Path(p) if p else None

    Path(log_dir).mkdir(exist_ok=True)
    ts = datetime.now().strftime("(%Y-%m-%d)_(%H-%M-%S)")
    log_path = Path(log_dir) / f"{run_name}_{ts}.log"

    fmt = logging.Formatter("%(asctime)s | [%(levelname)-8s] | %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    handlers = [fh]

    if to_stdout:
        sh = logging.StreamHandler(sys.stdout)  # use stdout; swap to stderr if you prefer
        sh.setFormatter(fmt)
        handlers.append(sh)

    root.setLevel((level or os.getenv("LOG_LEVEL", "INFO")).upper())
    for h in handlers:
        root.addHandler(h)

    os.environ["RUN_LOG_PATH"] = str(log_path)
    return log_path
