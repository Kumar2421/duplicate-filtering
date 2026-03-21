import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    MAX_GLOBAL_EMBEDDINGS: int = int(os.getenv("MAX_GLOBAL_EMBEDDINGS", "5000"))
    MAX_PER_VISIT: int = int(os.getenv("MAX_PER_VISIT", "5"))
    MAX_EMBEDDINGS_PER_GROUP: int = int(os.getenv("MAX_EMBEDDINGS_PER_GROUP", "5"))
    MIN_QUALITY: float = float(os.getenv("MIN_QUALITY", "0.4"))

    SIM_THRESHOLD: float = float(os.getenv("SIM_THRESHOLD", "0.5"))
    PRIMARY_MATCH_THRESHOLD: float = float(os.getenv("PRIMARY_MATCH_THRESHOLD", "0.5"))
    STRICT_MATCH_THRESHOLD: float = float(os.getenv("STRICT_MATCH_THRESHOLD", "0.75"))
    STORAGE_PATH: str = os.getenv("STORAGE_PATH", "./data/raw")
    CACHE_PATH: str = os.getenv("CACHE_PATH", "./data/cache")
    LOGS_PATH: str = os.getenv("LOGS_PATH", "./data/logs")

    DOWNLOAD_RETRIES: int = int(os.getenv("DOWNLOAD_RETRIES", "3"))
    DOWNLOAD_TIMEOUT_SECONDS: float = float(os.getenv("DOWNLOAD_TIMEOUT_SECONDS", "20"))
    DOWNLOAD_CONCURRENCY: int = int(os.getenv("DOWNLOAD_CONCURRENCY", "12"))

    MAX_IMAGE_DIM: int = int(os.getenv("MAX_IMAGE_DIM", "1600"))

    QDRANT_COLLECTION: str = os.getenv("QDRANT_COLLECTION", "face_embeddings")
    QDRANT_VECTOR_SIZE: int = int(os.getenv("QDRANT_VECTOR_SIZE", "512"))
    QDRANT_PATH: str = os.getenv("QDRANT_PATH", "./qdrant_db")
    QDRANT_DISTANCE: str = os.getenv("QDRANT_DISTANCE", "COSINE")

    BATCH_SIZE: int = int(os.getenv("BATCH_SIZE", "200"))

    IMAGE_BASE_URL: str = os.getenv("IMAGE_BASE_URL", "")


settings = Settings()
