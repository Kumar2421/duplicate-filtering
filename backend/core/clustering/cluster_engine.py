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
        Hybrid clustering: Groups by ID (visitId/customerId) first, then by visual similarity.
        Flags conflicts (same ID, different face) and duplicates (different ID, same face).
        """
        clusters = existing_clusters if existing_clusters is not None else []
        
        # 1. Map existing IDs to clusters for immediate lookup
        id_to_cluster_idx = {}
        for i, cluster in enumerate(clusters):
            # Ensure center is a numpy array
            if cluster.get("center") is not None and not isinstance(cluster["center"], np.ndarray):
                cluster["center"] = np.array(cluster["center"])
            
            for v in cluster.get("visits", []):
                vid = v.get("visitId")
                cid = v.get("customerId")
                if vid: id_to_cluster_idx[str(vid)] = i
                if cid: id_to_cluster_idx[str(cid)] = i

        for visit in visits:
            visit_vec = visit.get("vector")
            if visit_vec is None:
                continue

            assigned = False
            target_cluster = None
            best_sim = -1.0
            
            vid = str(visit.get("visitId"))
            cid = str(visit.get("customerId"))

            # STEP 1: PRIORITY - ID MATCHING (Force group same IDs)
            if vid in id_to_cluster_idx:
                target_cluster = clusters[id_to_cluster_idx[vid]]
                assigned = True
            elif cid in id_to_cluster_idx:
                target_cluster = clusters[id_to_cluster_idx[cid]]
                assigned = True

            # STEP 2: VISUAL MATCHING (If no ID match, or to check for face similarity in matched ID)
            # Find best visual match regardless of whether ID was matched
            best_visual_cluster = None
            max_visual_sim = -1.0
            for i, cluster in enumerate(clusters):
                if cluster.get("center") is None:
                    continue
                sim = float(cosine_similarity(visit_vec, cluster["center"]))
                if sim > max_visual_sim:
                    max_visual_sim = sim
                    best_visual_cluster = cluster

            # If not assigned by ID, try assigning by visual similarity
            if not assigned and max_visual_sim > self.threshold:
                target_cluster = best_visual_cluster
                assigned = True
                best_sim = max_visual_sim
                # Flag as DUPLICATE if it's a different ID but same face
                visit["duplicate"] = "multiple_ids"
            elif assigned:
                # If assigned by ID, calculate similarity to its own cluster center for metadata
                if target_cluster and target_cluster.get("center") is not None:
                    best_sim = float(cosine_similarity(visit_vec, target_cluster["center"]))
                
                # Flag as CONFLICT if it's the same ID but face is different
                if best_sim < self.threshold:
                    visit["conflict"] = "different_face"

            # STEP 3: UPDATE OR CREATE
            if assigned and target_cluster:
                existing_vids = {str(v["visitId"]) for v in target_cluster["visits"]}
                if vid not in existing_vids:
                    target_cluster["visits"].append(visit)
                    
                    # Update ID maps
                    idx = clusters.index(target_cluster)
                    id_to_cluster_idx[vid] = idx
                    if cid: id_to_cluster_idx[cid] = idx

                    # Update center
                    all_vectors = [np.array(v["vector"]) for v in target_cluster["visits"] if "vector" in v]
                    if all_vectors:
                        target_cluster["center"] = np.mean(all_vectors, axis=0)
                
                visit["similarityScore"] = best_sim if best_sim != -1.0 else 1.0
            else:
                new_cluster = {
                    "clusterId": f"cluster_{uuid.uuid4().hex[:8]}",
                    "visits": [visit],
                    "center": visit_vec,
                    "type": "valid"
                }
                clusters.append(new_cluster)
                idx = len(clusters) - 1
                id_to_cluster_idx[vid] = idx
                if cid: id_to_cluster_idx[cid] = idx
                visit["similarityScore"] = 1.0

        return clusters
