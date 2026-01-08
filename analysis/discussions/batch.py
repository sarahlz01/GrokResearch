import json
import logging
import re
import time
from pathlib import Path
from typing import Dict, List, Optional

from google import genai
from google.genai import types
from google.genai.errors import APIError 

from cache import AnalysisCache
from config import AnalysisConfig
from prompt import Prompts

logger = logging.getLogger(__name__)

class BatchManager:
    def __init__(self, config: AnalysisConfig, prompt: Prompts, cache: Optional[AnalysisCache] = None):
        self.config = config
        self.cache = cache
        self.prompt = prompt
        
        self.batch_path = Path(config.batch_files)
        self.model = self.config.model
        self.max_retries = self.config.max_retries
        self.job_timeout = self.config.job_timeout

        self.batch_path.mkdir(parents=True, exist_ok=True)

        self.client = genai.Client()

    def count_exact_tokens(self, conversations: Dict, prompt: str) -> int:
        for conversation in conversations:
            messages = self.prompt._convert_conversation_format(conversation)
            integrated_prompt = self.prompt.prompt_generator(messages, prompt)

            try:
                response = self.client.models.count_tokens(
                    model=self.model,
                    contents=[{
                        "parts": [{"text": integrated_prompt}]
                    }]
                )
                
                input_tokens = response.total_tokens
                estimated_output = 700 if 'detailed' in prompt.lower() else 250
                
                return input_tokens + estimated_output
                
            except Exception as e:
                logger.error(f"Exact token counting failed: {e}")

    def create_batch_file(self, conversations: List[Dict], chunk_id: int, prompt: str) -> types.File:
        batch_file_path = self.batch_path / f"batch_chunk_{chunk_id}.jsonl"
        logger.info(f"[Chunk {chunk_id}] Creating batch file for {len(conversations)} tweets")

        try:
            with open(batch_file_path, "w", encoding="utf-8") as f:
                for conversation in conversations:
                    conversation_id = conversation.get("conversationId", {})
                    messages = self.prompt._convert_conversation_format(conversation)
                    integrated_prompt = self.prompt.prompt_generator(messages, prompt)

                    request_obj = {
                        "key": conversation_id,
                        "conversationId": conversation_id,
                        "request": {
                            "contents": [{
                                "parts": [{
                                    "text": integrated_prompt
                                }]
                            }]
                        }
                    }
                    f.write(json.dumps(request_obj, ensure_ascii=False) + "\n")
        except IOError as e:
            logger.error(f"[Chunk {chunk_id}] Failed to write local batch file {batch_file_path}: {e}")
            raise

        logger.info(f"[Chunk {chunk_id}] Local batch file created: {batch_file_path.resolve()}")

        try:
            uploaded_file = self.client.files.upload(
                file=batch_file_path,
                config=types.UploadFileConfig(display_name=f"audit_chunk_{chunk_id}", mime_type="jsonl")
            )
        except APIError as e:
            logger.error(f"[Chunk {chunk_id}] API failed to upload file {batch_file_path}. Details: {e}")
            raise
        except Exception as e:
            logger.error(f"[Chunk {chunk_id}] Unexpected error during file upload: {e}")
            raise

        logger.info(f"[Chunk {chunk_id}] Uploaded file: {uploaded_file.name}")
        return uploaded_file

    def _submit_batch_with_retry(self, uploaded_file_name: str, chunk_id: int) -> types.BatchJob:
        for attempt in range(self.max_retries):
            try:
                logger.info(f"Submitting batch job for chunk {chunk_id}, attempt {attempt + 1}")
                job = self.client.batches.create(model=self.model, src=uploaded_file_name,
                                                 config={"display_name": f"audit_job_{chunk_id}"})
                logger.info(f"Job started: {job.name}")
                return job
            except Exception as e:
                if "RESOURCE_EXHAUSTED" in str(e) or "429" in str(e):
                    wait_time = (2 ** (attempt - 1)) * 2
                    logger.warning(f"429 error, retrying in {wait_time}s...")
                    time.sleep(wait_time)
                    continue
                raise e

    def _poll_job_until_complete(self, job: types.BatchJob) -> types.BatchJob:
        if job is None:
            raise ValueError("Cannot poll job: Batch job object is None due to prior submission failure.")
        
        completed_states = {"JOB_STATE_SUCCEEDED", "JOB_STATE_FAILED", "JOB_STATE_CANCELLED"}
        start_time = time.time()

        while True:
            if time.time() - start_time > self.job_timeout:
                logger.error(f"Job {job.name} timed out after {self.job_timeout} seconds while polling.")
                raise TimeoutError(f"Batch job {job.name} exceeded maximum polling time.")
            
            try:
                current_job = self.client.batches.get(name=job.name)
                state = current_job.state.name
                logger.info(f"Job {job.name} state: {state}")

                if state in completed_states:
                    return current_job
            except APIError as e:
                logger.warning(f"API Error while polling job {job.name}. Retrying in 30s. Error: {e}")

            time.sleep(500)

    def handle_results(self, job: types.BatchJob) -> List[Dict]:
        if not job.dest or not job.dest.file_name:
            logger.error(f"Job {job.name} has no output file")
            return

        logger.info(f"Downloading results for job {job.name}...")

        try:
            file_content = self.client.files.download(file=job.dest.file_name).decode("utf-8")
        except APIError as e:
            logger.error(f"API failed to download results for job {job.name}. Details: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error during results download for job {job.name}: {e}")
            raise

        return file_content
    

    def run_batch_pipeline(self, conversations: List[Dict], chunk_id: int, prompt: str) -> List[Dict]:
        if not conversations:
            logger.info(f"Chunk {chunk_id}: No tweets to process")
            return []
            
        estimate_count = self.count_exact_tokens(conversations, prompt)
        logger.info(f"Estimated tokens: {estimate_count}")

        uploaded_file = self.create_batch_file(conversations, chunk_id, prompt)
        job = self._submit_batch_with_retry(uploaded_file_name=uploaded_file.name, chunk_id=chunk_id)

        completed_job = self._poll_job_until_complete(job)
        if completed_job.state.name != "JOB_STATE_SUCCEEDED":
            logger.error(f"Job {job.name} did not succeed: {completed_job.state.name}")
            if hasattr(completed_job, 'error'):
                logger.error(f"Error details: {completed_job.error}")
            return []
        
        results = self.handle_results(completed_job)

        return results
