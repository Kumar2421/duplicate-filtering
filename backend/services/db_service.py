from qdrant_client import QdrantClient
from qdrant_client.http import models
import logging

class DBService:
    def __init__(self, collection_name="face_embeddings", vector_size=512, path=None, client=None):
        """
        Initialize Qdrant client and recreate collection if it doesn't exist.
        """
        if client:
            self.client = client
        else:
            self.client = QdrantClient(path=path or "./qdrant_db")
        
        self.collection_name = collection_name
        self.vector_size = vector_size
        self.logger = logging.getLogger(__name__)
        self._setup_collection()

    def _setup_collection(self):
        """
        Recreates or confirms if collection exists in Qdrant.
        """
        try:
            collections = self.client.get_collections().collections
            exists = any(c.name == self.collection_name for c in collections)
            
            if not exists:
                self.client.recreate_collection(
                    collection_name=self.collection_name,
                    vectors_config=models.VectorParams(size=self.vector_size, distance=models.Distance.COSINE)
                )
                self.logger.info(f"Collection {self.collection_name} created.")
            else:
                self.logger.info(f"Collection {self.collection_name} already exists.")
        except Exception as e:
            self.logger.error(f"Error setting up Qdrant collection: {str(e)}")

    def insert_embedding(self, face_id: str, embedding: list, metadata: dict):
        """
        Insert embedding into Qdrant.
        """
        try:
            # We use face_id as point ID if it's numeric or unique string
            # Qdrant supports UUIDs or uint64 for point IDs
            # If face_id is not proper, we could hash it or let Qdrant assign.
            # For now, let's keep it simple.
            self.client.upsert(
                collection_name=self.collection_name,
                points=[
                    models.PointStruct(
                        id=face_id, 
                        vector=embedding, 
                        payload=metadata
                    )
                ]
            )
        except Exception as e:
            self.logger.error(f"Error inserting into Qdrant: {str(e)}")

    def search_duplicates(self, embedding: list, threshold=0.75):
        """
        Search for potential duplicates in Qdrant based on threshold.
        """
        try:
            results = self.client.search(
                collection_name=self.collection_name,
                query_vector=embedding,
                limit=5,
                score_threshold=threshold
            )
            return results
        except Exception as e:
            self.logger.error(f"Error searching Qdrant: {str(e)}")
            return []
