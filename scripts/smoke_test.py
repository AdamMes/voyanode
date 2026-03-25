import boto3
import json
from config import Config


def run_smoke_test():
    """
    Performs a basic connectivity check to AWS services used in the project.
    Uses centralized configuration from the Config class.
    """
    print("🚀 Starting VoyaNode Smoke Test...\n")

    # Validate the configuration before starting
    try:
        Config.validate()
    except ValueError as e:
        print(f"❌ Configuration Error: {e}")
        return

    region = Config.AWS_REGION
    bucket_name = Config.S3_BUCKET
    queue_url = Config.SQS_QUEUE_URL

    # 1. S3 Connectivity Check
    try:
        s3 = boto3.client('s3', region_name=region)
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ S3: Connection successful. Bucket '{bucket_name}' is accessible.")
    except Exception as e:
        print(f"❌ S3: Error accessing bucket '{bucket_name}'. {e}")

    # 2. SQS Connectivity Check
    try:
        sqs = boto3.client('sqs', region_name=region)
        sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['QueueArn'])
        print(f"✅ SQS: Connection successful. Queue is accessible.")
    except Exception as e:
        print(f"❌ SQS: Error accessing queue at {queue_url}. {e}")

    # 3. Bedrock Connectivity Check (Titan Embeddings)
    try:
        bedrock_runtime = boto3.client('bedrock-runtime', region_name=region)
        model_id = Config.TITAN_EMBED_MODEL

        test_body = json.dumps({"inputText": "Smoke test check"})
        bedrock_runtime.invoke_model(body=test_body, modelId=model_id)
        print(f"✅ Bedrock: Connection successful. Model '{model_id}' is responding.")
    except Exception as e:
        print(f"❌ Bedrock: Error invoking model. Check IAM permissions and Model Access. {e}")

    print("\n--- Smoke Test Completed ---")


if __name__ == "__main__":
    run_smoke_test()