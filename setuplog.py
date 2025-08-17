from pathlib import Path
from datetime import datetime
import logging, os, sys
from typing import Optional

<<<<<<< HEAD

def setup_logging(
    run_name: str = "run",
    log_dir: str = "logs",
    level: Optional[str] = None,
    to_stdout: bool = True,
):
    """
    Configure root logger once per process. Returns the Path of the log file.
    If logging is already configured, reuses existing handlers and returns RUN_LOG_PATH if set.
    """
=======
class _FileLineFilter(logging.Filter):
    def filter(self, record: logging.LogRecord) -> bool:
        record.fileline = f"{record.filename}:{record.lineno}"
        return True

def setup_logging(run_name: str = "run", log_dir: str = "logs",
                  level: str | None = None, to_stdout: bool = True):
>>>>>>> 5ec8236c4bd3127e61ee0c03bff1aa6eefae7f38
    root = logging.getLogger()
    if root.handlers:
        return Path(os.getenv("RUN_LOG_PATH", "")) if os.getenv("RUN_LOG_PATH") else None

    Path(log_dir).mkdir(exist_ok=True)
    ts = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    log_path = Path(log_dir) / f"{run_name}_{ts}.log"

<<<<<<< HEAD
    fmt = logging.Formatter(
        "%(asctime)s | [%(levelname)-8s] | %(name)s | %(message)s", "%Y-%m-%d %H:%M:%S"
    )
=======
    # Like %(levelname)-8s but for "filename:lineno" with 10 spaces:
    # Use one of these two formats:
    # - Fixed minimum width 10 (can grow if longer): "%(fileline)-10s"
    # - Hard cap at 10 chars (truncate):            "%(fileline)-10.10s"
    fmt = "%(asctime)s | %(levelname)-8s | %(fileline)-21s | %(message)s"
    datefmt = "%Y-%m-%d %H:%M:%S"
    formatter = logging.Formatter(fmt, datefmt)
>>>>>>> 5ec8236c4bd3127e61ee0c03bff1aa6eefae7f38

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(formatter)
    fh.addFilter(_FileLineFilter())   # populate record.fileline
    handlers = [fh]

    if to_stdout:
<<<<<<< HEAD
        sh = logging.StreamHandler(
            sys.stdout
        )  # use stdout; swap to stderr if you prefer
        sh.setFormatter(fmt)
=======
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        sh.addFilter(_FileLineFilter())
>>>>>>> 5ec8236c4bd3127e61ee0c03bff1aa6eefae7f38
        handlers.append(sh)

    root.setLevel((level or os.getenv("LOG_LEVEL", "INFO")).upper())
    for h in handlers:
        root.addHandler(h)

    os.environ["RUN_LOG_PATH"] = str(log_path)
    return log_path
