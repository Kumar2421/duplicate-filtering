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
            is_duplicate = len(customer_ids) > 1
            
            # Track which clusters each customer belongs to for Conflict detection
            for cid in customer_ids:
                if cid not in customer_to_clusters:
                    customer_to_clusters[cid] = []
                customer_to_clusters[cid].append(cluster)
            
            # Initial type based on duplicate rule
            cluster["type"] = "duplicate" if is_duplicate else "valid"

        # Rule 2: CONFLICT (Same customerId appears in multiple clusters/faces)
        for cid, member_clusters in customer_to_clusters.items():
            if len(member_clusters) > 1:
                for cluster in member_clusters:
                    # Mark as conflict if it's already duplicate or valid
                    # Conflict is an additional state that can coexist or override
                    cluster["type"] = "conflict"

        # Final pass for stats
        for cluster in clusters:
            cluster["stats"] = {
                "totalVisits": len(cluster["visits"]),
                "uniqueCustomers": len(cluster["customerIds"])
            }

        return clusters
