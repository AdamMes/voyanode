import boto3
import json
import logging
from botocore.exceptions import ClientError
from config import Config

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def setup_voyanode_infra():
    # Initialize AWS clients using the centralized Config
    s3 = boto3.client("s3", region_name=Config.AWS_REGION)
    sqs = boto3.client("sqs", region_name=Config.AWS_REGION)
    aoss = boto3.client("opensearchserverless", region_name=Config.AWS_REGION)
    sts = boto3.client("sts")

    # Get the current user ARN for permissions
    try:
        user_identity = sts.get_caller_identity()
        user_arn = user_identity["Arn"]
        account_id = user_identity["Account"]
    except ClientError as e:
        print(f"❌ Error getting user identity: {e}")
        return

    print("--- Starting Full VoyaNode Infrastructure Setup ---")
    print(f"📍 Region: {Config.AWS_REGION}")
    print(f"👤 User: {user_arn}\n")

    # 1. S3 Bucket Check/Creation
    try:
        s3.head_bucket(Bucket=Config.S3_BUCKET)
        print(f"ℹ️ Info: Bucket '{Config.S3_BUCKET}' already exists.")
    except ClientError:
        try:
            s3.create_bucket(Bucket=Config.S3_BUCKET)
            print(f"✅ Success: Bucket '{Config.S3_BUCKET}' created.")
        except Exception as e:
            print(f"❌ Error creating bucket: {e}")

    # 2. SQS Queue Check/Creation
    queue_name = Config.SQS_QUEUE_URL.split('/')[-1] if Config.SQS_QUEUE_URL else "voyanode-queue-adam"
    queue_url = ""
    try:
        response = sqs.get_queue_url(QueueName=queue_name)
        queue_url = response['QueueUrl']
        print(f"ℹ️ Info: SQS Queue '{queue_name}' already exists.")
    except ClientError:
        try:
            response = sqs.create_queue(QueueName=queue_name)
            queue_url = response['QueueUrl']
            print("✅ Success: SQS Queue created.")
        except Exception as e:
            print(f"❌ Error creating SQS: {e}")

    # 3. S3 to SQS Integration (Event Notifications)
    if queue_url:
        try:
            # Get Queue ARN
            attributes = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=['QueueArn'])
            queue_arn = attributes['Attributes']['QueueArn']

            # A. Set SQS Policy to allow S3 to send messages
            sqs_policy = {
                "Version": "2012-10-17",
                "Statement": [{
                    "Effect": "Allow",
                    "Principal": {"Service": "s3.amazonaws.com"},
                    "Action": "SQS:SendMessage",
                    "Resource": queue_arn,
                    "Condition": {
                        "ArnLike": {"aws:SourceArn": f"arn:aws:s3:::{Config.S3_BUCKET}"}
                    }
                }]
            }
            sqs.set_queue_attributes(
                QueueUrl=queue_url,
                Attributes={'Policy': json.dumps(sqs_policy)}
            )
            print("✅ Success: SQS Access Policy updated (S3 Permission).")

            # B. Configure S3 Notification
            s3.put_bucket_notification_configuration(
                Bucket=Config.S3_BUCKET,
                NotificationConfiguration={
                    'QueueConfigurations': [{
                        'QueueArn': queue_arn,
                        'Events': ['s3:ObjectCreated:*', 's3:ObjectRemoved:*']
                    }]
                }
            )
            print("✅ Success: S3 Event Notification configured.")
        except Exception as e:
            print(f"❌ Error during S3-SQS integration: {e}")

    # 4. OpenSearch Serverless Infrastructure
    policy_name = "voyanode-policy"

    # A. Encryption Policy
    try:
        aoss.create_security_policy(
            name=policy_name,
            type='encryption',
            policy=json.dumps({
                "Rules": [{"ResourceType": "collection", "Resource": [f"collection/{policy_name}"]}],
                "AWSOwnedKey": True
            })
        )
        print("✅ Success: Encryption Policy created.")
    except ClientError as e:
        if "ConflictException" in str(e):
            print("ℹ️ Info: Encryption Policy already exists.")

    # B. Network Policy (Public Access)
    try:
        aoss.create_security_policy(
            name=policy_name,
            type='network',
            policy=json.dumps([{
                "Rules": [{"ResourceType": "collection", "Resource": [f"collection/{policy_name}"]}],
                "AllowFromPublic": True
            }])
        )
        print("✅ Success: Network Policy created.")
    except ClientError as e:
        if "ConflictException" in str(e):
            print("ℹ️ Info: Network Policy already exists.")

    # C. Data Access Policy (Adding the current user)
    try:
        aoss.create_access_policy(
            name=policy_name,
            type='data',
            policy=json.dumps([{
                "Rules": [
                    {"ResourceType": "collection", "Resource": [f"collection/{policy_name}"], "Permission": ["aoss:*"]},
                    {"ResourceType": "index", "Resource": [f"index/{policy_name}/*"], "Permission": ["aoss:*"]}
                ],
                "Principal": [user_arn]
            }])
        )
        print("✅ Success: Data Access Policy created (User added).")
    except ClientError as e:
        if "ConflictException" in str(e):
            print("ℹ️ Info: Data Access Policy already exists.")

    # D. OpenSearch Collection creation
    try:
        aoss.create_collection(name=policy_name, type='VECTORSEARCH')
        print("🚀 Success: OpenSearch Collection creation initiated. (5-10 mins)")
    except ClientError as e:
        if "ConflictException" in str(e):
            print("ℹ️ Info: OpenSearch Collection already exists.")

    print("\n--- Setup Complete ---")
    print("👉 Next Step: Wait for status 'Active' in AWS Console.")
    print("👉 Then: Copy the Endpoint to OS_HOST in your .env file.")


if __name__ == "__main__":
    setup_voyanode_infra()