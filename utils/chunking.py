import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def clean_text(text):
    """
    Standardizes the text by removing extra whitespaces and newlines.
    """
    if not text:
        return ""
    # Splitting by whitespace and joining ensures only single spaces remain
    cleaned = " ".join(text.split())
    return cleaned


def split_text(text, chunk_size=100, overlap=20):
    """
    Splits a long string into smaller overlapping segments.
    Note: Defaults here are small (100/20) just for the visibility of the test.
    In production, we use larger values like 1000/100.
    """
    if not text:
        logger.warning("The provided text is empty. No chunks created.")
        return []

    chunks = []
    start = 0
    text_length = len(text)

    while start < text_length:
        end = start + chunk_size
        chunk = text[start:end]
        chunks.append(chunk)

        if end >= text_length:
            break

        # Shift forward by (size - overlap) to create the next chunk
        start += (chunk_size - overlap)

    logger.info(f"Chunking complete: Created {len(chunks)} chunks.")
    return chunks


# --- Local Test Block ---
if __name__ == "__main__":
    print("🧪 Testing Chunking Utilities...\n")

    # 1. Test clean_text
    raw_text = "This is a   test sentence \n with weird \t spacing   and newlines."
    cleaned = clean_text(raw_text)
    print(f"Original: '{raw_text}'")
    print(f"Cleaned:  '{cleaned}'")
    print("-" * 30)

    # 2. Test split_text (with small numbers to see the overlap clearly)
    long_text = (
        "Eilat is Israel's southernmost city, a busy port and popular resort at the northern tip of the Red Sea. "
        "The city's beaches, coral reef, and nightlife make it a popular destination for domestic and international tourism."
    )

    # We use size=50 and overlap=10 for the test
    test_chunks = split_text(long_text, chunk_size=50, overlap=10)

    for i, c in enumerate(test_chunks):
        print(f"Chunk {i + 1} (Length {len(c)}): '{c}'")
        if i < len(test_chunks) - 1:
            # Show the overlap with the next chunk
            next_start = test_chunks[i + 1][:10]
            print(f"   ↳ Overlap with next: '{next_start}'...")