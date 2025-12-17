To run the code, for each month of grok data you want to analyze, you will need to:

1. the path of the data to the env
2. the output directory where you want the analysis to be saved
3. the cache directory
4. and a directory to store batch request files

Example:

- Input file path: `grok_data/Grok_2025-08/output_CLEANED.json`
- Output directory: `outputs/trolling_analysis_results/trolling_analysis_2025_08/`

The month the input data belongs to should be saved in the same month of the output directory

How to run:

1. **Basic Execution of data**:

   Enter the `discussions folder` :

   `cd analysis/discussions`

   Then run the following commands one at a time, if we run the in 5 terminals at a time, it would result in a rate limit, the models we use have a limit 10,000,000 tokens per batch job as of now each chunk has an average of 6 million tokens

   ```bash

   python3 main.py --chunk-id 0 2>&1 | tee analysis_run.log
   python3 main.py --chunk-id 1 2>&1 | tee analysis_run.log
   python3 main.py --chunk-id 2 2>&1 | tee analysis_run.log
   python3 main.py --chunk-id 3 2>&1 | tee analysis_run.log
   python3 main.py --chunk-id 4 2>&1 | tee analysis_run.log

   ```

### **How Chunking Works in the Script**

1. **Purpose of Chunking**:

   - Chunking divides the dataset into smaller, manageable parts.
   - Each chunk is processed independently.
   - Since our data is too big to fit into a single batch request we have to chunk them, we chunk them into 5 equal or thereabout chunks.
2. **Implementation in the Script**:

   - The script uses the `--chunk-id` argument to determine which chunk of data to process.
   - The dataset is divided into `NUMBER_OF_CHUNKS` (e.g., 5 chunks).
   - Each conversation is assigned to a chunk based on its index in the dataset:
     ```python
     if chunk_id is not None:
         if i % NUMBER_OF_CHUNKS != (chunk_id - 1):
             continue
     ```
   - The modulo operation (`i % NUMBER_OF_CHUNKS`) ensures that conversations are evenly distributed across chunks.
3. **How It Works**:

   - For a dataset with `N` conversations and `NUMBER_OF_CHUNKS = 5`:
     - Chunk 1 processes conversations where `i % 5 == 0`.
     - Chunk 2 processes conversations where `i % 5 == 1`.
     - Chunk 3 processes conversations where `i % 5 == 2`.
     - Chunk 4 processes conversations where `i % 5 == 3`.
     - Chunk 5 processes conversations where `i % 5 == 4`.

Your `.env` file should have the following values:

```
TWITTERIO_API_KEY="TWITTERIO_API_KEY"
HYDRA_FULL_ERROR=1
GEMINI_API_KEY="GEMINI_API_KEY"
CACHE_DIR_TROLLING="CACHE FULL PATH"
FILE_PATH_TROLLING="FILE PATH FOR DATA"
OUTPUT_PATH_TROLLING="FULL PATH FOR SAVED DATA"
BATCH_PATH_TROLLING="BATCH REQUEST FULL PATH"
CACHE_DIR_DISCUSSION="CACHE FULL PATH"
FILE_PATH_DISCUSSION="FILE PATH FOR DATA"
OUTPUT_PATH_DISCUSSION="FULL PATH FOR SAVED DATA"
BATCH_PATH_DISCUSSION="BATCH REQUEST FULL PATH"
```
