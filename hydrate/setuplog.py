from pathlib import Path
from datetime import datetime
import logging, os, sys
from typing import Optional

class _FileLineFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.fileline = f"{record.filename}:{record.lineno}"
        return True

def setup_logging(run_name: str = "run", log_dir: str = "logs",
                  level: str | None = None, to_stdout: bool = False):
    root = logging.getLogger()
    if root.handlers:
        return Path(os.getenv("RUN_LOG_PATH", "")) if os.getenv("RUN_LOG_PATH") else None

    Path(log_dir).mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path = Path(log_dir) / f"{run_name}_{ts}.log"

    # Like %(levelname)-8s but for "filename:lineno" with 10 spaces:
    # Use one of these two formats:
    # - Fixed minimum width 10 (can grow if longer): "%(fileline)-10s"
    # - Hard cap at 10 chars (truncate):            "%(fileline)-10.10s"
    fmt = "%(asctime)s | %(levelname)-8s | %(fileline)-21s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt)

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(formatter)
    fh.addFilter(_FileLineFilter())   # populate record.fileline
    handlers = [fh]

    if to_stdout:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        sh.addFilter(_FileLineFilter())
        handlers.append(sh)

    root.setLevel((level or os.getenv("LOG_LEVEL", "INFO")).upper())
    for h in handlers:
        root.addHandler(h)

    os.environ["RUN_LOG_PATH"] = str(log_path)
    return log_path
