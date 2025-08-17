# cli.py
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
import hydra
from omegaconf import OmegaConf, DictConfig 
from setuplog import setup_logging
from main import run_streaming  # re-use your existing function
import logging

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
    out_path: str = "grok_data/data.json"

@dataclass
class LoggingCfg:
    run_name: str = "run"
    to_stdout: bool = False

@dataclass
class Config:
    date: DateCfg
    settings: SettingsCfg
    logging: LoggingCfg
    run_by_week: bool = True 

@hydra.main(config_path="conf", config_name="config", version_base=None)
def main(cfg: DictConfig):
    setup_logging(run_name=cfg.logging.run_name, log_dir="logs", to_stdout=cfg.logging.to_stdout)
    if not validate_input_date(cfg):
        raise RuntimeError
    # if we are doing a weekly run
    if (cfg.run_by_week):
        start_day = int(cfg.date.since_day)
        end_day = int(cfg.date.until_day)
        block_hours = cfg.settings.block_hours
        for i in range(start_day, end_day+1):
            day = fix_digit_formatting(i) 
            hr = 0
            for j in range(0, 24, block_hours):
                if (j+block_hours >= 24): # if we are on the last iteration
                    hr = 23
                hour = fix_digit_formatting(hr)
                run_streaming(handle="grok", 
                              since=f"{cfg.date.year}-{cfg.date.month}-{day} 00:00:00",
                              until=f"{cfg.date.year}-{cfg.date.month}-{day} {hour}:59:59",
                              query_type=cfg.settings.query_type,
                              include_self_threads=cfg.settings.include_self_threads,
                              include_quotes=cfg.settings.include_quotes,
                              include_retweets=cfg.settings.include_retweets,
                              build_final_json=cfg.settings.build_final_json,
                              out_path=cfg.settings.out_path,
                              number_conversations=cfg.settings.number_conversations)
                hr += block_hours
    else:
        # Cast because Hydra passed strings (e.g., "05")
        start_day = int(cfg.date.since_day)
        end_day = int(cfg.date.until_day)
        block_hours = cfg.settings.block_hours
        for i in range(start_day, end_day+1):
            day = fix_digit_formatting(i) 
            hr = 0
            for j in range(0, 24, block_hours):
                if (j+block_hours >= 24): # if we are on the last iteration
                    hr = 23
                hour = fix_digit_formatting(hr)
                run_streaming(handle="grok", 
                              since=f"{cfg.date.year}-{cfg.date.month}-{day} 00:00:00",
                              until=f"{cfg.date.year}-{cfg.date.month}-{day} {hour}:59:59",
                              query_type=cfg.settings.query_type,
                              include_self_threads=cfg.settings.include_self_threads,
                              include_quotes=cfg.settings.include_quotes,
                              include_retweets=cfg.settings.include_retweets,
                              build_final_json=cfg.settings.build_final_json,
                              out_path=cfg.settings.out_path,
                              number_conversations=cfg.settings.number_conversations)
                hr += block_hours
        print("e")
        
        

def validate_input_date(cfg: DictConfig) -> bool:
    year = int(cfg.date.year)
    month = int(cfg.date.month)
    since_day = int(cfg.date.since_day)
    until_day = int(cfg.date.until_day)
    
    if not (2023 <= year <= 2025):
        logging.error("🚫\tInvalid year!")
        return False
    if not (1 <= month <= 12):
        logging.error("🚫\tInvalid month!")
        return False
    if not (1 <= since_day <= 31):
        logging.error("🚫\tInvalid since_day!")
        return False
    if not (1 <= until_day <= 31):
        logging.error("🚫\tInvalid until_day!")
        return False
    if not (since_day < until_day):
        logging.error("🚫\t Invalid: since_day must be less than until_day!")
        return False
    return True          
    
def fix_digit_formatting(i: int) -> str: 
    num = str(i)
    if len(num) == 1:
        num = "0" + num
    elif len(num) > 2:
        logging.error("🚫\tInvalid digit!")
        raise RuntimeError
    return num
if __name__ == "__main__":
    main()
