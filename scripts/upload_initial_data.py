import os
import sys

# --- Path Configuration ---
# Get the absolute path of the directory where this script is located (VoyaNode/scripts)
current_script_dir = os.path.dirname(os.path.abspath(__file__))

# Get the project root (VoyaNode directory)
project_root = os.path.dirname(current_script_dir)

# Add the project root to sys.path so Python can find the 'utils' package
if project_root not in sys.path:
    sys.path.insert(0, project_root)

# Now we can safely import our S3 utility
try:
    from utils.s3_utils import upload_file_to_voyanode

    print("✅ Successfully linked VoyaNode utilities.")
except ImportError as e:
    print(f"❌ Critical Error: Could not find 'utils'. {e}")
    sys.exit(1)

# Set the path to the raw data folder
DATA_DIR = os.path.join(project_root, "data", "raw")


def upload_all_travel_data():
    """
    Scans the data/raw folder and uploads all valid files to S3.
    """
    # Ensure the directory exists
    if not os.path.exists(DATA_DIR):
        print(f"❌ Error: Data folder not found at {DATA_DIR}")
        return

    print(f"--- Starting VoyaNode Data Upload to S3 ---")

    # List all files in the directory
    all_items = os.listdir(DATA_DIR)

    # Filter for files only and skip hidden system files (like .DS_Store)
    files_to_upload = [
        f for f in all_items
        if os.path.isfile(os.path.join(DATA_DIR, f)) and not f.startswith('.')
    ]

    if not files_to_upload:
        print(f"⚠️ No valid documents found in {DATA_DIR}")
        return

    count = 0
    for filename in files_to_upload:
        file_path = os.path.join(DATA_DIR, filename)

        # Define the S3 path (Key) to include the 'data/' folder
        s3_key = f"data/{filename}"

        # Perform the upload with the new object_name
        # We pass s3_key as the second argument (object_name)
        success = upload_file_to_voyanode(file_path, object_name=s3_key)

        if success:
            count += 1

    print(f"\n--- Upload Summary ---")
    print(f"Successfully uploaded {count} files to VoyaNode storage.")


if __name__ == "__main__":
    upload_all_travel_data()