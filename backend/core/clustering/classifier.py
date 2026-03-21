from typing import List, Dict, Any

class ClusterClassifier:
    def classify_clusters(self, clusters: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Classifies clusters into: duplicate, conflict, or valid.
        """
        customer_to_clusters = {}

        for cluster in clusters:
            customer_ids = list(set(v["customerId"] for v in cluster["visits"]))
            cluster["customerIds"] = customer_ids
            
            # Rule 1: DUPLICATE (Same face/cluster, different customerIds)
            if len(customer_ids) > 1:
                cluster["type"] = "duplicate"
            else:
                cluster["type"] = "valid"

            # Track which clusters each customer belongs to for Conflict detection
            for cid in customer_ids:
                if cid not in customer_to_clusters:
                    customer_to_clusters[cid] = []
                customer_to_clusters[cid].append(cluster)

        # Rule 2: CONFLICT (Same customerId appears in multiple clusters/faces)
        for cid, member_clusters in customer_to_clusters.items():
            if len(member_clusters) > 1:
                for cluster in member_clusters:
                    # Conflict takes precedence over valid, but maybe not over duplicate?
                    # Usually, if it's already a duplicate cluster, it's a mess anyway.
                    # We'll mark it as conflict if it's not already duplicate.
                    if cluster["type"] == "valid":
                        cluster["type"] = "conflict"
                    elif cluster["type"] == "duplicate":
                        # If a cluster is both duplicate (multiple IDs) AND those IDs 
                        # appear elsewhere, it's a high-priority conflict/duplicate.
                        cluster["type"] = "conflict" # Conflict is more severe for ID integrity

        # Final pass for stats
        for cluster in clusters:
            cluster["stats"] = {
                "totalVisits": len(cluster["visits"]),
                "uniqueCustomers": len(cluster["customerIds"])
            }

        return clusters
