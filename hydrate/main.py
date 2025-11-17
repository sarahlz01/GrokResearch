import sys
assert (sys.prefix != sys.base_prefix), "Make sure you have setup the venv and activated it by calling:\tsource venv/bin/activate.\nCheck README for more information"
from calendar import monthrange
from datetime import datetime, timedelta, timezone
import hydra
from dataclasses import dataclass
from omegaconf import DictConfig 
from setuplog import setup_logging
import logging
from hydrate.network.do_hydrate import run_streaming 
import time

@dataclass
class DateCfg:
    year: str
    month: str
    since_day: str
    until_day: str
    
@dataclass
class SettingsCfg:
    handle: str = "grok"
    query_type: str = "Latest"
    include_self_threads: bool = False
    include_quotes: bool = False
    include_retweets: bool = False
    block_hours: int = 6
    number_conversations: int = 150
    build_final_json: bool = True
    out_path: str = 'grok_data/${date.year}-${date.month}.json'
    grok_db_outpath: str = 'grok_data/grok.sqlite3'

@dataclass
class LoggingCfg:
    run_name: str = "run"
    log_dir: str = "logs"
    to_stdout: bool = False

@dataclass
class Config:
    date: DateCfg
    settings: SettingsCfg
    logging: LoggingCfg

@hydra.main(config_path="conf", config_name=None, version_base=None)
def main(cfg: DictConfig):
    setup_logging(run_name=cfg.logging.run_name, log_dir=cfg.logging.log_dir, to_stdout=cfg.logging.to_stdout)

    # Parse + validate once
    year  = int(cfg.date.year)
    month = int(cfg.date.month)
    start_day = int(cfg.date.since_day)
    end_day   = int(cfg.date.until_day)
    block_hours = int(cfg.settings.block_hours)

    if not validate_input_date(year, month, start_day, end_day, block_hours):
        logging.error("🚫\tInvalid date, exiting program")
        raise SystemExit(1)

    logging.info("▶️\tRun starting")
    t0 = time.time()
    for day in range(start_day, end_day + 1):
        logging.info("\n\n")
        logging.info("⏱️\tScraping %d-%d day %d/%d", month, year, day, end_day)
        for block_start in range(0, 24, block_hours):
            start_dt = datetime(year, month, day, block_start, 0, 0, tzinfo=timezone.utc)
            end_dt = start_dt + timedelta(hours=block_hours)

            since = api_ts(start_dt)
            until = api_ts(end_dt)
            logging.info("▶️\tBlock %s → %s | handle=%s | cap=%d", since, until, cfg.settings.handle, cfg.settings.number_conversations)
            try:
                run_streaming(
                    handle=cfg.settings.handle,
                    since=since,
                    until=until,
                    query_type=cfg.settings.query_type,
                    include_self_threads=cfg.settings.include_self_threads,
                    include_quotes=cfg.settings.include_quotes,
                    include_retweets=cfg.settings.include_retweets,
                    build_final_json=cfg.settings.build_final_json,
                    out_path=cfg.settings.out_path,
                    number_conversations=cfg.settings.number_conversations,
                    grok_db_outpath=cfg.settings.grok_db_outpath
                )
            except KeyboardInterrupt:
                logging.error("‼️\tInterrupted; aborting remaining blocks.")
                raise
            except Exception as e:
                logging.exception("🚫\tBlock failed: %s → %s.\tVerbose error: %s", since, until, e)
    elapsed = time.time() - t0
    logging.info("✅✅\tRUN COMPLETE, total time: %.1fs\t✅✅", elapsed)

def api_ts(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")

def validate_input_date(year: int, month: int, since_day: int, until_day: int, block_hours: int) -> bool:
    ok = True
    if not (2006 <= year <= 2100):  # Twitter era; pick what you like
        logging.error("🚫\tinvalid year: %s", year); ok = False
    if not (1 <= month <= 12):
        logging.error("🚫\tinvalid month: %s", month); ok = False
    max_day = monthrange(year, month)[1]
    if not (1 <= since_day <= max_day):
        logging.error("🚫\tinvalid since_day: %s (max %d for %d-%02d)", since_day, max_day, year, month); ok = False
    if not (1 <= until_day <= max_day):
        logging.error("🚫\tinvalid until_day: %s (max %d for %d-%02d)", until_day, max_day, year, month); ok = False
    if not (since_day <= until_day):   # allow single-day runs
        logging.error("🚫\tsince_day must be <= until_day"); ok = False
    if not (1 <= block_hours <= 24):
        logging.error("🚫\tinvalid block_hours: %s", block_hours); ok = False
    return ok

if __name__ == "__main__":
    print("Running!")
    main()
    print("Done! Check logs.")