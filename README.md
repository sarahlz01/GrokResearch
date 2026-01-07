# GrokResearch

To **Rehydrate** the dataset, run the following commands starting from project root (`./GrokResearch`).

- Make sure python version is 3.11.5 or 3.11.6
- From `./GrokResearch`: Create a `.env` and set up the environment variable as follows `TWITTERIO_API_KEY="YOUR_API_KEY"` & `HYDRA_FULL_ERROR=1`
- From ./GrokResearch: `cd hydrate`
- Set up virtual environment in `./GrokResearch/hydrate`: `python3 -m venv venv`
- Activate/start the virtual environment: `source venv/bin/activate`
- Install packages: `pip install -r hydrate_requirements.txt`
- Make sure to download `dehydrated.json` and place it under `./GrokResearch/hydrate/rehydration`
- Then run: `python3 -m rehydration.rehydrate --in ./rehydration/dehydrated.json --out ./rehydration/hydrated.json`
- Logs will be visible in `./GrokResearch/hydrate/rehydration/logs/`
- Note that this will hydrate the **entire** dataset using the [twitterapi.io](https://twitterapi.io/) service, so make sure you have some credits before doing this
- If you run out of credits the hydration script will continue to run but nothing will be added to `hydrated.json`
- Logs can be viewed under `./GrokResearch/hydrate/hydration/logs`
- If you cancel the hydration at any point, you may need to fix the JSON file at the end by appending a `]` to close the array

To run the **Hydrate** code, run the following commands

- Make sure python version is 3.11.5 or 3.11.6
- From ./GrokResearch: Create a `.env` and set up the environment variable as follows `TWITTERIO_API_KEY="key"` & `HYDRA_FULL_ERROR=1` & `GROK_DB_PATH="grok_data/grok.sqlite3"`
- From ./GrokResearch: `cd hydrate`
- Set up virtual environment in ./GrokResearch/hydrate: `python3 -m venv venv`
- Activate/start the virtual environment: `source venv/bin/activate`
- Install packages: `pip install -r hydrate_requirements.txt`
- Run the program using one of the config files in /conf/ `python3 main.py --config-dir conf/runs --config-name <NAME OF YAML FILE WITHOUT .yaml>`

Notes

- The grok_sqlite3 database is never wiped clean before each run. It's updated each time, so for testing, delete the sqlite file and the JSON file and run the test
