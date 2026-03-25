import boto3
from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import logging
from config import Config

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_opensearch_client():
    """
    Creates and returns an authenticated OpenSearch client for AWS Serverless (AOSS).
    """
    service = 'aoss'
    region = Config.AWS_REGION
    credentials = boto3.Session().get_credentials()

    # Use native OpenSearch AWS signer for Serverless
    auth = AWSV4SignerAuth(credentials, region, service)

    return OpenSearch(
        hosts=[{'host': Config.OS_HOST, 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection,
        pool_maxsize=20,
        timeout=30
    )


def create_index_with_mapping(client):
    """
    Explicitly creates the index with k-NN mapping.
    This prevents the 'Field is not knn_vector type' error.
    """
    index_name = Config.OS_INDEX

    # Mapping for Amazon Titan Text Embeddings v2 (1024 dimensions)
    index_body = {
        "settings": {
            "index.knn": "true"
        },
        "mappings": {
            "properties": {
                "embedding": {
                    "type": "knn_vector",
                    "dimension": 1024,
                    "method": {
                        "name": "hnsw",
                        "space_type": "l2",
                        "engine": "nmslib"
                    }
                },
                "text": {"type": "text"},
                "file_name": {"type": "keyword"},
                "chunk_index": {"type": "integer"},
                "total_chunks": {"type": "integer"}
            }
        }
    }

    try:
        if not client.indices.exists(index=index_name):
            client.indices.create(index=index_name, body=index_body)
            logger.info(f"✅ Created index '{index_name}' with k-NN mapping.")
            return True
        return False
    except Exception as e:
        logger.error(f"❌ Error creating index: {e}")
        return False


def index_document(client, text, vector, metadata):
    """
    Indexes a chunk into OpenSearch.
    Checks if index exists first to avoid incorrect auto-creation.
    """
    index_name = Config.OS_INDEX

    # Ensure index exists with correct mapping before first insertion
    if not client.indices.exists(index=index_name):
        create_index_with_mapping(client)

    document = {
        "text": text,
        "embedding": vector,
        "file_name": metadata.get("file_name"),
        "chunk_index": metadata.get("chunk_index"),
        "total_chunks": metadata.get("total_chunks"),
        "source": metadata.get("source"),
        "indexed_at": metadata.get("indexed_at")
    }

    try:
        return client.index(index=index_name, body=document)
    except Exception as e:
        logger.error(f"❌ Error indexing document: {e}")
        return None


def vector_search(client, query_vector, k=5):
    """
    Performs a k-NN search on the 'embedding' field.
    """
    query = {
        "size": k,
        "query": {
            "knn": {
                "embedding": {
                    "vector": query_vector,
                    "k": k
                }
            }
        }
    }
    try:
        response = client.search(index=Config.OS_INDEX, body=query)
        return response['hits']['hits']
    except Exception as e:
        logger.error(f"❌ Error during vector search: {e}")
        return []


def wipe_and_restart_index(client):
    """
    Deletes the current index and recreates it with correct settings.
    Use this to fix 'knn_vector type' errors.
    """
    try:
        if client.indices.exists(index=Config.OS_INDEX):
            client.indices.delete(index=Config.OS_INDEX)
            logger.info(f"🗑️ Old index deleted.")
        return create_index_with_mapping(client)
    except Exception as e:
        logger.error(f"❌ Reset failed: {e}")
        return False


def delete_documents_by_file(client, file_name):
    """
    OpenSearch Serverless does NOT support _delete_by_query.
    This version searches for IDs first and then deletes them in bulk.
    """
    try:
        # שלב 1: מציאת כל ה-IDs של הצאנקים השייכים לקובץ
        search_query = {
            "query": {
                "term": {
                    "file_name": file_name
                }
            },
            "_source": False,  # אנחנו צריכים רק את ה-ID, לא את התוכן
            "size": 1000  # מקסימום צאנקים למחיקה בבת אחת
        }

        results = client.search(index=Config.OS_INDEX, body=search_query)
        hits = results.get('hits', {}).get('hits', [])

        if not hits:
            logger.info(f"ℹ️ No documents found for {file_name} in the index.")
            return True

        # שלב 2: הכנת בקשת Bulk Delete
        bulk_data = []
        for hit in hits:
            bulk_data.append({
                "delete": {
                    "_index": Config.OS_INDEX,
                    "_id": hit["_id"]
                }
            })

        # שלב 3: ביצוע המחיקה
        if bulk_data:
            client.bulk(body=bulk_data)
            logger.info(f"🗑️ Successfully deleted {len(hits)} chunks for {file_name} from OpenSearch.")
            return True

    except Exception as e:
        logger.error(f"❌ Error during AOSS deletion for {file_name}: {e}")
        return False


# --- Improved Diagnostic Tool ---
if __name__ == "__main__":

    print("\n--- 🧪 VoyaNode OpenSearch Diagnostic Tool ---")
    os_client = get_opensearch_client()

    print("Attempting to create index with correct k-NN mappings...")
    if create_index_with_mapping(os_client):
        print("✅ SUCCESS: Index 'voyanode-index' created correctly.")
    else:
        print("ℹ️ Index already exists or could not be created.")

    try:
        print(f"1. Connecting to host: {Config.OS_HOST}...")
        os_client.indices.get_alias()
        print("✅ Connection Authenticated!")

        target_index = Config.OS_INDEX
        if os_client.indices.exists(index=target_index):
            print(f"✅ Index '{target_index}' is active.")
            # Fetch count via search (AOSS compliant)
            count_query = {"query": {"match_all": {}}, "size": 0}
            results = os_client.search(index=target_index, body=count_query)
            doc_count = results['hits']['total']['value']
            print(f"📊 Current Documents: {doc_count}")
        else:
            print(f"⚠️ Index '{target_index}' does not exist.")
    except Exception as e:
        print(f"❌ Diagnostic Error: {e}")
    print("--- 🧪 Diagnostic Complete ---\n")