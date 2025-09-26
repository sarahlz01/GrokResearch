# GrokResearch

To run the **SCRAPING** code, run the following commands

- Make sure python version is 3.11.5 or 3.11.6
- From ./GrokResearch: Create a `.env` and set up the environment variable as follows `TWITTERIO_API_KEY="key"` & `HYDRA_FULL_ERROR=1`
- From ./GrokResearch: `cd scraping`
- Set up virtual environment: `python3 -m venv venv`
- Activate/start the virtual environment: `source venv/bin/activate`
- Install packages: `pip install -r scrape_requirements.txt`
- Run the program using one of the config files in /conf/ `python3 main.py --config-dir conf/runs --config-name <NAME OF YAML FILE WITHOUT .yaml>`

Notes

- The grok_sqlite3 database is never wiped clean before each run. It's updated each time, so for testing, delete the sqlite file and the JSON file and run the test
- The sqlite3 database supports concurrent writers if opened in WAL mode (we do this)
