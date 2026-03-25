import json
import os

def count_flagged_clusters(file_path):
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        return

    with open(file_path, 'r') as f:
        data = json.load(f)

    clusters = data.get('clusters', [])
    
    # Filtering logic: clusters that are flagged as 'duplicate' or 'conflict'
    # or have any visit with conflictIds
    flagged_clusters = []
    for cluster in clusters:
        is_flagged = False
        
        # Check cluster type
        if cluster.get('type') in ['duplicate', 'conflict']:
            is_flagged = True
        
        # Check visits for conflicts
        if not is_flagged:
            for visit in cluster.get('visits', []):
                if visit.get('conflictIds') and len(visit.get('conflictIds')) > 0:
                    is_flagged = True
                    break
        
        if is_flagged:
            flagged_clusters.append(cluster)

    print(f"Total clusters in JSON: {len(clusters)}")
    print(f"Flagged (Duplicate/Conflict) clusters: {len(flagged_clusters)}")
    
    for i, cluster in enumerate(flagged_clusters):
        c_id = cluster.get('clusterId')
        c_type = cluster.get('type')
        v_count = len(cluster.get('visits', []))
        cust_ids = cluster.get('customerIds', [])
        print(f"{i+1}. ID: {c_id} | Type: {c_type} | Visits: {v_count} | Customers: {cust_ids}")

if __name__ == "__main__":
    path = "/mnt/additional-disk/duplicate-filtering/data/processed/TMJ-CBE/2026-03-24/visit-clusters.json"
    count_flagged_clusters(path)
