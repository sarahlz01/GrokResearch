# GrokResearch

To run this code, run the following commands

- Set up virtual environment: `python3 -m venv venv`
- Activate/start the virtual environment `source venv/bin/activate`
- Install packages `pip install -r requirements.txt`
- Create a .env and set up the environment variable as follows `TWITTERIO_API_KEY="key"`
- Run the file `python3 main.py`

Notes

- The grok_sqlit3 database is never wiped clean before each run. It's updated each time, so for testing, delete the sqlite file and the JSON file and run the test
- Change the backoff time depending on if you're a paid or free user of twitterapi.io
- Refer to line 23 of format_objects.py. Change `parts = [f"from:{handle}","to:taka_i_32", "filter:replies"]` to `parts = [f"from:{handle}", "filter:replies"]`
- Change the query date located in the main function where run_streaming is called
