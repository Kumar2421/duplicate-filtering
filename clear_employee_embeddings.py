import logging
from qdrant_client import QdrantClient
from qdrant_client.http import models
import os
import sys

# Add backend to path to import settings if needed, 
# but let's keep it simple and read from environment or defaults
QDRANT_PATH = os.getenv("QDRANT_PATH", "./backend/qdrant_db")
COLLECTION_NAME = "employee_enrollment"

def clear_employees():
    logging.basicConfig(level=logging.INFO)
    logger = logging.getLogger(__name__)

    if not os.path.exists(QDRANT_PATH):
        # Try relative to root if running from root
        if os.path.exists("./backend/qdrant_db"):
            q_path = "./backend/qdrant_db"
        else:
            logger.error(f"Qdrant path {QDRANT_PATH} not found.")
            return
    else:
        q_path = QDRANT_PATH

    logger.info(f"Connecting to Qdrant at {q_path}...")
    try:
        client = QdrantClient(path=q_path)
        
        collections = client.get_collections().collections
        exists = any(c.name == COLLECTION_NAME for c in collections)
        
        if exists:
            logger.info(f"Deleting collection '{COLLECTION_NAME}'...")
            client.delete_collection(collection_name=COLLECTION_NAME)
            logger.info(f"Collection '{COLLECTION_NAME}' deleted successfully.")
            
            # Recreating it ensures it's ready for fresh enrollment
            # We need to know the vector size. Buffalo_L uses 512.
            logger.info(f"Recreating collection '{COLLECTION_NAME}' with vector size 512...")
            client.recreate_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=models.VectorParams(size=512, distance=models.Distance.COSINE),
            )
            logger.info("Collection recreated and ready for fresh start.")
        else:
            logger.warning(f"Collection '{COLLECTION_NAME}' does not exist.")
            
    except Exception as e:
        logger.error(f"Error clearing employee embeddings: {e}")

if __name__ == "__main__":
    clear_employees()
