import os
from dotenv import load_dotenv

# Load environment variables from the .env file into the system environment
load_dotenv()


class Config:
    """
    Centralized configuration class for the VoyaNode project.
    All components should import their settings from this class to ensure consistency.
    """

    # General AWS Configuration
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")

    # S3 Settings (File Storage)
    S3_BUCKET = os.getenv("S3_BUCKET")

    # SQS Settings (Message Queue for asynchronous document processing)
    SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")

    # OpenSearch Settings (Vector Database)
    OS_HOST = os.getenv("OS_HOST")
    OS_INDEX = os.getenv("OS_INDEX", "voyanode-index")

    # Bedrock Model IDs
    # Using Claude 3.5 Haiku for optimal speed and cost
    CLAUDE_MODEL_ID = os.getenv("CLAUDE_MODEL_ID", "anthropic.claude-3-5-haiku-20241022-v1:0")
    # Using Titan Embeddings v2 for high-precision text vectorization
    TITAN_EMBED_MODEL = os.getenv("TITAN_EMBED_MODEL", "amazon.titan-embed-text-v2:0")

    @staticmethod
    def validate():
        """
        Validates that all critical environment variables are present.
        This should be called during the startup of app.py and worker.py.
        """
        required_vars = ["S3_BUCKET", "SQS_QUEUE_URL", "OS_HOST"]
        missing = [var for var in required_vars if not getattr(Config, var)]

        if missing:
            raise ValueError(f"CRITICAL ERROR: Missing required environment variables: {', '.join(missing)}")

        print("✅ Configuration loaded and validated successfully.")