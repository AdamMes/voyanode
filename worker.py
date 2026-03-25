import boto3
import json
import os
import time
import logging
import PyPDF2
from datetime import datetime, timezone
from config import Config
from utils.s3_utils import download_file_from_voyanode
from utils.chunking import clean_text, split_text
from utils.bedrock_utils import get_embedding
from utils.opensearch_utils import get_opensearch_client, index_document, delete_documents_by_file

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize AWS clients
sqs = boto3.client('sqs', region_name=Config.AWS_REGION)
os_client = get_opensearch_client()


def extract_text_from_pdf(file_path):
    """
    Extracts raw text from a PDF file using PyPDF2 library.
    """
    text = ""
    try:
        with open(file_path, "rb") as f:
            reader = PyPDF2.PdfReader(f)
            for page in reader.pages:
                page_text = page.extract_text()
                if page_text:
                    text += page_text + " "
        return text
    except Exception as e:
        logger.error(f"❌ Error extracting PDF text: {e}")
        return ""


def handle_upload(object_key, file_name):
    """
    Downloads the file from S3, processes its content, and indexes it into OpenSearch.
    Includes metadata for chunk position (e.g., "36 of 86").
    """
    local_path = os.path.join("data", "processed", file_name)
    os.makedirs(os.path.dirname(local_path), exist_ok=True)

    if download_file_from_voyanode(object_key, local_path):
        try:
            # Step 1: Content Extraction
            if file_name.lower().endswith('.pdf'):
                raw_text = extract_text_from_pdf(local_path)
            else:
                with open(local_path, 'r', encoding='utf-8', errors='ignore') as f:
                    raw_text = f.read()

            if not raw_text.strip():
                logger.warning(f"⚠️ No text extracted from {file_name}. Skipping.")
                return

            # Step 2: Preprocessing and Chunking
            cleaned_text = clean_text(raw_text)
            chunks = split_text(cleaned_text, chunk_size=1000, overlap=100)

            # Count total chunks before indexing to support "X of Y" logic
            total_chunks = len(chunks)

            # Step 3: Vectorization and Indexing
            success_count = 0
            for i, chunk in enumerate(chunks):
                vector = get_embedding(chunk)
                if vector:
                    # current_index starts at 1 for human-friendly UI (e.g., Chunk 1 of 10)
                    current_index = i + 1

                    metadata = {
                        "file_name": file_name,
                        "source": f"s3://{Config.S3_BUCKET}/{object_key}",
                        "chunk_index": current_index,
                        "total_chunks": total_chunks,
                        "indexed_at": datetime.now(timezone.utc).isoformat()  # Updated to timezone-aware UTC
                    }
                    # index_document sends chunk content, vector, and metadata to OpenSearch
                    index_document(os_client, chunk, vector, metadata)
                    success_count += 1

            logger.info(f"✅ Success: Indexed {success_count}/{total_chunks} chunks for {file_name}")

        except Exception as e:
            logger.error(f"❌ Error processing {file_name}: {e}")
        finally:
            # Clean up local file after processing
            if os.path.exists(local_path):
                os.remove(local_path)


def start_worker():
    """
    Starts the SQS long-polling loop to listen for S3 events.
    """
    logger.info(f"🤖 VoyaNode Worker is active and listening to {Config.SQS_QUEUE_URL}...")

    while True:
        try:
            response = sqs.receive_message(
                QueueUrl=Config.SQS_QUEUE_URL,
                MaxNumberOfMessages=1,
                WaitTimeSeconds=20
            )

            if 'Messages' in response:
                for msg in response['Messages']:
                    body = json.loads(msg['Body'])
                    if 'Records' in body:
                        for record in body['Records']:
                            event_name = record['eventName']
                            object_key = record['s3']['object']['key']
                            file_name = os.path.basename(object_key)

                            if 'ObjectCreated' in event_name:
                                handle_upload(object_key, file_name)
                            elif 'ObjectRemoved' in event_name:
                                logger.info(f"🗑️ Removal detected: Deleting data for {file_name}")
                                delete_documents_by_file(os_client, file_name)

                    sqs.delete_message(QueueUrl=Config.SQS_QUEUE_URL, ReceiptHandle=msg['ReceiptHandle'])

        except Exception as e:
            logger.error(f"❌ Worker loop error: {e}")
            time.sleep(5)


if __name__ == "__main__":
    try:
        Config.validate()
        os.makedirs(os.path.join("data", "processed"), exist_ok=True)
        start_worker()
    except Exception as e:
        print(f"❌ Worker Startup Error: {e}")