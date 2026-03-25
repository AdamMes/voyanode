import boto3
import logging
import os
from botocore.exceptions import ClientError
from config import Config

# Initialize logging for tracking S3 operations
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Use centralized configuration for consistency across environments
s3_client = boto3.client('s3', region_name=Config.AWS_REGION)

def upload_file_to_voyanode(file_path, object_name=None):
    """
    Uploads a local file to the VoyaNode S3 bucket.
    """
    if object_name is None:
        object_name = os.path.basename(file_path)

    try:
        logger.info(f"Uploading {file_path} to s3://{Config.S3_BUCKET}/{object_name}...")
        s3_client.upload_file(file_path, Config.S3_BUCKET, object_name)
        logger.info(f"✅ Success: {object_name} is now in the cloud.")
        return True
    except ClientError as e:
        logger.error(f"❌ Error uploading file: {e}")
        return False

def list_files_in_voyanode(prefix="data/"):
    """
    Lists files in the bucket, specifically within the 'data/' folder.
    """
    try:
        response = s3_client.list_objects_v2(Bucket=Config.S3_BUCKET, Prefix=prefix)
        if 'Contents' in response:
            return [obj['Key'] for obj in response['Contents'] if obj['Key'] != prefix]
        return []
    except ClientError as e:
        logger.error(f"❌ Error listing files: {e}")
        return []

def delete_file_from_voyanode(file_name):
    """
    Deletes a specific file from the S3 bucket.
    """
    try:
        s3_client.delete_object(Bucket=Config.S3_BUCKET, Key=file_name)
        logger.info(f"🗑️ Success: {file_name} deleted from bucket.")
        return True
    except ClientError as e:
        logger.error(f"❌ Error deleting file: {e}")
        return False

def download_file_from_voyanode(file_name, download_path):
    """
    Downloads a file from S3 to a local path (used by the Worker).
    """
    try:
        s3_client.download_file(Config.S3_BUCKET, file_name, download_path)
        logger.info(f"⬇️ Success: {file_name} downloaded to {download_path}")
        return True
    except ClientError as e:
        logger.error(f"❌ Error downloading file: {e}")
        return False

def delete_all_objects_from_s3():
    """
    Deletes all objects from the S3 bucket to ensure it's empty.
    """
    s3_resource = boto3.resource('s3', region_name=Config.AWS_REGION)
    bucket = s3_resource.Bucket(Config.S3_BUCKET)
    try:
        # Delete all objects in the bucket
        bucket.objects.all().delete()
        logger.info(f"🗑️ S3 Bucket {Config.S3_BUCKET} has been emptied.")
        return True
    except Exception as e:
        logger.error(f"❌ Error emptying bucket: {e}")
        return False