import os
import json
import logging
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)
logging.basicConfig(
    level=logging.INFO, 
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
OUTPUT_PATH = os.getenv("OUTPUT_PATH_TOXIC")

output_path = Path(OUTPUT_PATH)
base_path = output_path.parent

logger.info(f"OUTPUT_PATH: {output_path}")
logger.info(f"BASE_PATH (parent): {base_path}")

# get all month folders (2025_03 through 2025_09)
month_folders = [f for f in os.listdir(base_path) if f.startswith('2025_')]
month_folders.sort()

logger.info(f"Found {len(month_folders)} folders: {month_folders}")

def merge_chunks(folder: Path, pattern: str = "*.json"):
    merge_results = []
    files = sorted(folder.glob(pattern))

    for file in files:
        try:
            with file.open('r', encoding='utf-8') as f:
                data = json.load(f)
            merge_results.extend(data.get("reply_analysis", []))
        except Exception as e:
            logger.info(f"Skipping {file.name}: {e}")
        
    out_file = folder / "merged_results.json"
    with out_file.open('w', encoding='utf-8') as f:
        json.dump({'reply_analysis': merge_results}, f, indent=2, ensure_ascii=False)
    logger.info(f"Wrote {out_file} ({len(merge_results)} items)")

def main():
    if not month_folders:
        logger.info("No folders found under base path. Exiting")
        return
    for folder in month_folders:
        folders = base_path / folder
        merge_chunks(folders)


if __name__ == "__main__":
    main()