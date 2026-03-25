import os
import logging
from flask import Flask, render_template, request, jsonify, redirect, url_for
from config import Config
from utils.s3_utils import upload_file_to_voyanode, list_files_in_voyanode, delete_file_from_voyanode, s3_client
from utils.opensearch_utils import get_opensearch_client, vector_search, delete_documents_by_file
from utils.bedrock_utils import get_embedding, ask_claude
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = "voyanode_secret_key"

# Initialize OpenSearch client once
os_client = get_opensearch_client()


@app.context_processor
def inject_now():
    """Injects current year for dynamic footer rendering."""
    return {'current_year': datetime.now().year}


@app.route('/')
def index():
    """Renders the main Chat interface."""
    return render_template('index.html')


@app.route('/view/<path:filename>')
def view_file(filename):
    """Generates a presigned URL to view PDF files in-browser."""
    try:
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': Config.S3_BUCKET,
                'Key': filename,
                'ResponseContentDisposition': 'inline',
                'ResponseContentType': 'application/pdf'
            },
            ExpiresIn=600
        )
        return redirect(presigned_url)
    except Exception as e:
        logger.error(f"❌ View link error: {e}")
        return "Could not open file", 500


@app.route('/download/<path:filename>')
def download_file(filename):
    """Generates a presigned URL for forced file download."""
    try:
        presigned_url = s3_client.generate_presigned_url(
            'get_object',
            Params={'Bucket': Config.S3_BUCKET, 'Key': filename},
            ExpiresIn=600
        )
        return redirect(presigned_url)
    except Exception as e:
        logger.error(f"❌ Download link error: {e}")
        return "Could not download file", 500


@app.route('/about')
def about():
    """Renders the technological overview page."""
    return render_template('about.html')


@app.route('/data')
def data_management():
    """Lists files for the Knowledge Base table."""
    files = list_files_in_voyanode()
    return render_template('data.html', files=files)


@app.route('/upload', methods=['POST'])
def upload_file():
    """Handles direct file uploads to S3."""
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400

    temp_path = os.path.join("data", "processed", file.filename)
    os.makedirs(os.path.dirname(temp_path), exist_ok=True)
    file.save(temp_path)

    s3_key = f"data/{file.filename}"
    if upload_file_to_voyanode(temp_path, s3_key):
        os.remove(temp_path)
        return jsonify({"success": True, "filename": file.filename})

    return jsonify({"error": "Upload failed"}), 500


@app.route('/delete/<path:filename>')
def delete_file(filename):
    """Removes file from S3 and its associated vectors from OpenSearch."""
    if delete_file_from_voyanode(filename):
        raw_name = os.path.basename(filename)
        delete_documents_by_file(os_client, raw_name)
    return redirect(url_for('data_management'))


@app.route('/chat', methods=['POST'])
def chat():
    """
    Core RAG endpoint with content de-duplication logic.
    """
    user_query = request.json.get("message")
    if not user_query:
        return jsonify({"error": "Empty message"}), 400

    # Step 1: Embed the user question
    query_vector = get_embedding(user_query)

    # Step 2: Retrieve relevant chunks
    # This calls your fixed function in opensearch_utils.py
    search_results = vector_search(os_client, query_vector, k=5)

    if not search_results:
        logger.warning("⚠️ No search results returned from OpenSearch")

    # Step 3: Filter duplicates and prepare rich metadata
    sources_data = []
    seen_texts = set()
    context_chunks = []

    for hit in search_results:
        content = hit['_source']['text'].strip()

        # De-duplication check: Skip if exact text was already added
        if content not in seen_texts:
            sources_data.append({
                "file_name": hit['_source']['file_name'],
                "content": hit['_source']['text'],
                "chunk_index": hit['_source'].get('chunk_index', '?'),
                "total_chunks": hit['_source'].get('total_chunks', '?')
            })
            context_chunks.append(hit['_source']['text'])
            seen_texts.add(content)

    # Step 4: Build prompt and call Claude
    context = "\n---\n".join(context_chunks)
    system_prompt = (
        "You are VoyaNode, a General AI Travel Advisor. "
        "Strictly use the provided context to answer questions. "
        "If the answer is not in the documents, state clearly: "
        "'I am a travel advisor, and I don't have information about this topic in my current knowledge base.'"
    )

    full_prompt = f"Context:\n{context}\n\nQuestion: {user_query}"
    ai_response = ask_claude(full_prompt, system_prompt)

    # Step 5: Clean sources if AI has no info
    no_info_phrase = "I don't have information about this topic"
    if no_info_phrase.lower() in ai_response.lower():
        sources_data = []

    return jsonify({
        "response": ai_response,
        "sources": sources_data
    })


# ייבוא הפונקציה הנכונה
from utils.opensearch_utils import wipe_and_restart_index


@app.route('/wipe-index', methods=['POST'])
def wipe_index():
    """
    Resets the index and ensures it's recreated with correct k-NN mappings.
    Also clears all files from S3 to keep the system in sync.
    """
    # 1. Reset the vector index (your existing logic)
    if wipe_and_restart_index(os_client):
        logger.info("✅ OpenSearch: Index wiped and recreated.")

        # 2. Clear the S3 bucket (new addition)
        from utils.s3_utils import delete_all_objects_from_s3
        if delete_all_objects_from_s3():
            logger.info("✅ S3 Bucket: All files deleted. System is now completely fresh.")
        else:
            logger.warning("⚠️ Index was wiped, but there was an error clearing the S3 bucket.")

    else:
        logger.error("❌ System Reset failed - OpenSearch index could not be wiped.")

    return redirect(url_for('data_management'))

if __name__ == '__main__':
    try:
        Config.validate()
        app.run(debug=True, port=5001)
    except Exception as e:
        logger.error(f"❌ Startup Error: {e}")