# GrokResearch

To **Rehydrate** the dataset, run the following commands starting from project root (`./GrokResearch`).

- Ensure that Python version is 3.11.5 or 3.11.6
- From `./GrokResearch`: Create a `.env` and set up the environment variable as follows `TWITTERIO_API_KEY="YOUR_API_KEY"` & `HYDRA_FULL_ERROR=1`
- From ./GrokResearch: `cd hydrate`
- Set up virtual environment in `./GrokResearch/hydrate`: `python3 -m venv venv`
- Activate/start the virtual environment: `source venv/bin/activate`
- Install packages: `pip install -r hydrate_requirements.txt`
- Make sure to download have downloaded `dehydrated.json` and place it under `./GrokResearch/hydrate/rehydration`

## Hydration arguments

All of these are relative to `./GrokResearch/hydrate/`

- `--in`: Path to `dehydrated.json` (default: `./rehydration/dehydrated.json`)
- `--out`: Output path for hydrated JSON (default: `./rehydration/hydrated.json`)
- `--no-update-engagement`: Do **not** update the dehydrated engagement counts with the most recent counts from the API
- `--log-every`: Log progress every N conversations (default: `10,000`)
- `--thread-ids`: Only rehydrate specific threadIDs (comma separated IDs, no whitespace)
- `--thread-ids-file`: Rehydrate specific threadIDs using a JSON file containing the IDs (refer to `/GrokResearch/hydrate/hydration/thread_ids_example.json`) for an example

## Example commands

To rehydrate the entire dataset:
`python3 -m rehydration.rehydrate` in the `/GrokResearch/hydrate/` directory

To rehydrate specific threads from the data set, then you can run:
`python3 -m rehydration.rehydrate --thread-ids 111111111111,22222222`

or

`python3 -m rehydration.rehydrate --thread-ids-file ./rehydration/thread_ids.json`

### Notes

- Note that this will hydrate the dataset using the [twitterapi.io](https://twitterapi.io/) service, so make sure you have some credits before doing this
- If you run out of credits the hydration script will continue to run but nothing will be added to `hydrated.json`
- Logs can be viewed under `./GrokResearch/hydrate/hydration/logs`
- If you cancel the hydration at any point, you may need to fix the JSON file at the end by appending a `]` to close the array

### To run the **Hydrate** (different than rehydrating) code, run the following commands

- Make sure python version is 3.11.5 or 3.11.6
- From ./GrokResearch: Create a `.env` and set up the environment variable as follows `TWITTERIO_API_KEY="key"` & `HYDRA_FULL_ERROR=1` & `GROK_DB_PATH="grok_data/grok.sqlite3"`
- From ./GrokResearch: `cd hydrate`
- Set up virtual environment in ./GrokResearch/hydrate: `python3 -m venv venv`
- Activate/start the virtual environment: `source venv/bin/activate`
- Install packages: `pip install -r hydrate_requirements.txt`
- Run the program using one of the config files in /conf/ `python3 main.py --config-dir conf/runs --config-name <NAME OF YAML FILE WITHOUT .yaml>`

Notes

- The grok_sqlite3 database is never wiped clean before each run. It's updated each time, so for testing, delete the sqlite file and the JSON file and run the test
