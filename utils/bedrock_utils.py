import boto3
import json
import logging
from config import Config

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

bedrock_runtime = boto3.client(
    service_name='bedrock-runtime',
    region_name=Config.AWS_REGION
)

def get_embedding(text):
    """
    Converts text to a 1024-dimension vector using Amazon Titan v2.
    """
    try:
        body = json.dumps({
            "inputText": text,
            "dimensions": 1024,
            "normalize": True
        })
        response = bedrock_runtime.invoke_model(
            body=body,
            modelId=Config.TITAN_EMBED_MODEL,
            accept="application/json",
            contentType="application/json"
        )
        response_body = json.loads(response.get("body").read())
        return response_body.get("embedding")
    except Exception as e:
        logger.error(f"❌ Error generating embedding: {e}")
        return None

def ask_claude(prompt, system_prompt="You are a General AI Travel Advisor. Your knowledge is strictly limited to the provided context. If the answer is not in the context, state that you do not have information on this topic based on the uploaded documents."):
    """
    Sends prompt to Claude 3.5 with a focus on document-based answers.
    """
    try:
        body = json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 1000,
            "system": system_prompt,
            "messages": [{"role": "user", "content": prompt}],
            "temperature": 0.3 # Lower temperature for higher factual accuracy
        })
        response = bedrock_runtime.invoke_model(
            body=body,
            modelId=Config.CLAUDE_MODEL_ID,
            accept="application/json",
            contentType="application/json"
        )
        response_body = json.loads(response.get("body").read())
        return response_body['content'][0]['text']
    except Exception as e:
        logger.error(f"❌ Error calling Claude: {e}")
        return "I encountered an error while processing your request."

if __name__ == "__main__":
    # Local test
    print("🧪 Testing General AI Advisor logic...")
    print(ask_claude("What is in the documents?", "Context: The documents contain info about Tokyo."))