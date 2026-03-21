from __future__ import annotations

from dataclasses import dataclass


@dataclass
class PipelineMetrics:
    total_visits_processed: int = 0
    total_images_found: int = 0
    total_images_downloaded: int = 0
    total_images_invalid: int = 0
    total_images_filtered: int = 0
    total_embeddings_extracted: int = 0
    total_embeddings_stored: int = 0
    total_images_failed_download: int = 0

    def to_dict(self):
        return {
            "totalVisitsProcessed": self.total_visits_processed,
            "totalImagesFound": self.total_images_found,
            "totalImagesDownloaded": self.total_images_downloaded,
            "totalImagesInvalid": self.total_images_invalid,
            "totalImagesFiltered": self.total_images_filtered,
            "totalEmbeddingsExtracted": self.total_embeddings_extracted,
            "totalEmbeddingsStored": self.total_embeddings_stored,
            "totalImagesFailedDownload": self.total_images_failed_download,
        }
