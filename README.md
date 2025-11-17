# GrokResearch

To **Hydrate** the dataset, run the following commands starting from project root (`./GrokResearch`).

- Make sure python version is 3.11.5 or 3.11.6
- From `./GrokResearch`: Create a `.env` and set up the environment variable as follows `TWITTERIO_API_KEY="YOUR_API_KEY"` & `HYDRA_FULL_ERROR=1`
- From ./GrokResearch: `cd scraping`
- Set up virtual environment in `./GrokResearch/scraping`: `python3 -m venv venv`
- Activate/start the virtual environment: `source venv/bin/activate`
- Install packages: `pip install -r scrape_requirements.txt`
- Make sure to download `dehydrated.json` and place it under `./GrokResearch/scraping/hydration` (you will need to create this hydration folder)
- Then run: `python3 hydrate.py --in ./hydration/dehydrated.json --out ./hydration/hydrated.json`
- Note that this will hydrate the **entire** dataset using the [twitterapi.io](https://twitterapi.io/) service, so make sure you have some credits before doing this
- If you run out of credits the hydration script will continue to run but nothing will be added to `hydrate.json`
- Logs can be viewed under `./GrokResearch/scraping/hydration/logs`

To run the **SCRAPING** code, run the following commands

- Make sure python version is 3.11.5 or 3.11.6
- From ./GrokResearch: Create a `.env` and set up the environment variable as follows `TWITTERIO_API_KEY="key"` & `HYDRA_FULL_ERROR=1`
- From ./GrokResearch: `cd scraping`
- Set up virtual environment in ./GrokResearch/scraping: `python3 -m venv venv`
- Activate/start the virtual environment: `source venv/bin/activate`
- Install packages: `pip install -r scrape_requirements.txt`
- Run the program using one of the config files in /conf/ `python3 main.py --config-dir conf/runs --config-name <NAME OF YAML FILE WITHOUT .yaml>`

Notes

- The grok_sqlite3 database is never wiped clean before each run. It's updated each time, so for testing, delete the sqlite file and the JSON file and run the test
- The sqlite3 database supports concurrent writers if opened in WAL mode (we do this)
