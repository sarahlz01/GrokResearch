# GrokResearch

To run this code, run the following commands

- Set up virtual environment: `python3 -m venv venv`
- Activate/start the virtual environment `source venv/bin/activate`
- Install packages `pip install -r requirements.txt`
- Create a .env and set up the environment variable as follows `TWITTERIO_API_KEY="key"`
- Run the file `python3 main.py`

Notes

- The grok_sqlit3 database is never wiped clean before each run. It's updated each time, so for testing, delete the sqlite file and run the test
