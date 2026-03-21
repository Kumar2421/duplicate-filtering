from __future__ import annotations
import numpy as np
from typing import List, Dict, Any, Optional
from .similarity import cosine_similarity
import uuid

class ClusterEngine:
    def __init__(self, threshold: float = 0.7):
        self.threshold = threshold

    def build_visit_vector(self, embeddings: List[List[float]]) -> Optional[np.ndarray]:
        """Convert multiple embeddings for a visit into a single mean vector."""
        if not embeddings:
            return None
        return np.mean(embeddings, axis=0)

    def cluster_visits(self, visits: List[Dict[str, Any]], existing_clusters: Optional[List[Dict[str, Any]]] = None) -> List[Dict[str, Any]]:
        """
        Incremental clustering of visits based on their mean vectors.
        Each visit in 'visits' should have a 'vector' key (np.ndarray).
        If existing_clusters is provided, it attempts to match new visits to them first.
        """
        clusters = existing_clusters if existing_clusters is not None else []
        
        # Convert existing centers to numpy if they aren't already (from JSON)
        for cluster in clusters:
            if cluster.get("center") is not None:
                if not isinstance(cluster["center"], np.ndarray):
                    cluster["center"] = np.array(cluster["center"])
            elif cluster.get("visits"):
                # Try to find a visit with a vector to use as center
                for v in cluster["visits"]:
                    if "vector" in v and v["vector"] is not None:
                        cluster["center"] = np.array(v["vector"])
                        break
                    elif "embeddings" in v and v["embeddings"]:
                        # Fallback if vector isn't cached but embeddings are
                        cluster["center"] = np.mean(v["embeddings"], axis=0)
                        break

        for visit in visits:
            visit_vec = visit.get("vector")
            if visit_vec is None:
                continue

            assigned = False
            best_sim = -1.0
            best_cluster = None

            for cluster in clusters:
                if cluster.get("center") is None:
                    continue
                sim = cosine_similarity(visit_vec, cluster["center"])
                if sim > self.threshold and sim > best_sim:
                    best_sim = sim
                    best_cluster = cluster

            if best_cluster:
                # Deduplicate by visitId before adding
                existing_vids = {v["visitId"] for v in best_cluster["visits"]}
                if visit["visitId"] not in existing_vids:
                    best_cluster["visits"].append(visit)
                    # Update center: mean of all visit vectors in cluster
                    all_vectors = []
                    for v in best_cluster["visits"]:
                        if "vector" in v:
                            all_vectors.append(v["vector"])
                        elif "embeddings" in v and v["embeddings"]:
                            # Fallback if vector isn't cached but embeddings are
                            all_vectors.append(np.mean(v["embeddings"], axis=0))
                    
                    if all_vectors:
                        best_cluster["center"] = np.mean(all_vectors, axis=0)
                
                visit["similarityScore"] = best_sim
                assigned = True
            
            if not assigned:
                clusters.append({
                    "clusterId": f"cluster_{uuid.uuid4().hex[:8]}",
                    "visits": [visit],
                    "center": visit_vec,
                    "type": "valid"
                })
                visit["similarityScore"] = 1.0

        return clusters
