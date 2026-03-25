import sys
import os
import logging

# Add the project root to the sys.path to allow importing from utils and config
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.opensearch_utils import get_opensearch_client
from config import Config

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_voyanode_index():
    """
    Creates a k-NN index in OpenSearch Serverless with specific mappings
    for vector search and metadata.
    """
    client = get_opensearch_client()
    index_name = Config.OS_INDEX

    # Index configuration optimized for Amazon Titan Text Embeddings v2
    # Dimension: 1024 is the standard for Titan v2
    index_body = {
        "settings": {
            "index": {
                "knn": "true",
                "knn.algo_param.ef_search": "512"
            }
        },
        "mappings": {
            "properties": {
                "embedding": {
                    "type": "knn_vector",
                    "dimension": 1024,
                    "method": {
                        "name": "hnsw",
                        "engine": "nmslib",
                        "space_type": "l2",
                        "parameters": {
                            "ef_construction": 128,
                            "m": 16
                        }
                    }
                },
                "text": {"type": "text"},
                "file_name": {"type": "keyword"},
                "source": {"type": "keyword"}
            }
        }
    }

    try:
        # Check if index already exists to avoid errors
        if not client.indices.exists(index=index_name):
            print(f"🚀 Creating index: {index_name}...")
            client.indices.create(index=index_name, body=index_body)
            print(f"✅ Success: Index '{index_name}' created successfully.")
        else:
            print(f"ℹ️ Info: Index '{index_name}' already exists.")
    except Exception as e:
        print(f"❌ Error during index creation: {e}")

# --- Local Test Block ---
if __name__ == "__main__":
    print("🧪 Running Local Index Creation Test...")
    create_voyanode_index()