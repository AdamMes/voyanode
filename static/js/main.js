/*
 * VoyaNode - Main JS Logic
 * Optimized for RAG transparency and dynamic metadata display.
 */

document.addEventListener('DOMContentLoaded', function() {
    // --- 1. UI Elements ---
    const elements = {
        fileInput: document.getElementById('file-input'),
        uploadBtn: document.getElementById('upload-btn'),
        fileInfo: document.getElementById('file-info'),
        fileNameDisplay: document.getElementById('file-name-display'),
        clearFileBtn: document.getElementById('clear-file'),
        uploadStatus: document.getElementById('upload-status'),
        uploadForm: document.getElementById('upload-form'),
        chatForm: document.getElementById('chat-form'),
        userInput: document.getElementById('user-input'),
        chatWindow: document.getElementById('chat-window')
    };

    // --- 2. Helper Functions ---

    const scrollToBottom = () => {
        elements.chatWindow.scrollTop = elements.chatWindow.scrollHeight;
    };

    const appendMessage = (role, text, id = null) => {
        const msgDiv = document.createElement('div');
        msgDiv.className = `message ${role}-message`;
        if (id) msgDiv.id = id;
        msgDiv.textContent = text;
        elements.chatWindow.appendChild(msgDiv);
        scrollToBottom();
        return msgDiv;
    };

    /**
     * Renders an accordion for each unique chunk returned by the RAG pipeline.
     * Displays metadata in "Chunk X of Y" format.
     */
    const renderSourcesHtml = (sources) => {
        if (!sources || sources.length === 0) return '';

        let html = '<div class="sources-wrapper">';
        sources.forEach((src) => {
            const fileName = src.file_name || 'Unknown File';
            const current = src.chunk_index || '?';
            const total = src.total_chunks || '?';
            const content = src.content || 'No content preview available.';

            // Generate the details/summary element (Accordion)
            html += `
                <details class="source-details">
                    <summary>Source: ${fileName} (Chunk ${current} of ${total})</summary>
                    <div class="chunk-content">
                        ${content.replace(/\n/g, '<br>')}
                    </div>
                </details>
            `;
        });
        html += '</div>';
        return html;
    };

    // --- 3. File Upload Logic ---

    elements.fileInput.addEventListener('change', function() {
        if (this.files && this.files.length > 0) {
            elements.fileNameDisplay.textContent = this.files[0].name;
            elements.fileInfo.classList.remove('d-none');
            elements.uploadBtn.disabled = false;
        }
    });

    elements.clearFileBtn.addEventListener('click', function() {
        elements.fileInput.value = '';
        elements.fileInfo.classList.add('d-none');
        elements.uploadBtn.disabled = true;
    });

    elements.uploadForm.addEventListener('submit', async (e) => {
        e.preventDefault();
        const formData = new FormData(e.target);

        elements.uploadBtn.disabled = true;
        const originalBtnText = elements.uploadBtn.innerHTML;
        elements.uploadBtn.innerHTML = '<span class="spinner-border spinner-border-sm"></span> Processing...';

        try {
            const response = await fetch('/upload', { method: 'POST', body: formData });
            const data = await response.json();

            if (data.success) {
                elements.uploadStatus.innerHTML = '<span class="text-success">✅ File uploaded successfully!</span>';
                elements.fileInput.value = '';
                elements.fileInfo.classList.add('d-none');
            } else {
                elements.uploadStatus.innerHTML = `<span class="text-danger">❌ ${data.error || 'Upload failed'}</span>`;
                elements.uploadBtn.disabled = false;
            }
        } catch (error) {
            elements.uploadStatus.innerHTML = '<span class="text-danger">❌ System error during upload.</span>';
            elements.uploadBtn.disabled = false;
        } finally {
            elements.uploadBtn.innerHTML = originalBtnText;
        }
    });

    // --- 4. Chat Logic (AJAX) ---

    elements.chatForm.addEventListener('submit', async (e) => {
        e.preventDefault(); // Prevents page reload on Enter

        const message = elements.userInput.value.trim();
        if (!message) return;

        const submitBtn = elements.chatForm.querySelector('button[type="submit"]');

        // Lock button and show thinking state
        submitBtn.disabled = true;
        const originalText = submitBtn.innerHTML;
        submitBtn.innerHTML = 'Thinking...';

        // Display user message and clear input
        appendMessage('user', message);
        elements.userInput.value = '';

        // Display AI placeholder
        const typingId = `typing-${Date.now()}`;
        appendMessage('ai', '...', typingId);

        try {
            const response = await fetch('/chat', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ message: message })
            });

            const data = await response.json();
            const typingElem = document.getElementById(typingId);

            if (typingElem) {
                // Set text answer first
                typingElem.textContent = data.response;

                // Append rich sources if available from the RAG search
                if (data.sources && data.sources.length > 0) {
                    typingElem.innerHTML += renderSourcesHtml(data.sources);
                }
            }
        } catch (error) {
            const typingElem = document.getElementById(typingId);
            if (typingElem) typingElem.textContent = 'Sorry, a system error occurred.';
        } finally {
            // Unlock button and scroll
            submitBtn.disabled = false;
            submitBtn.innerHTML = originalText;
            scrollToBottom();
        }
    });
});