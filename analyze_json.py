import json
import os

json_path = r"d:\projects\duplicate-filtering\data\processed\TMJ-CBE\2026-03-20\visit-clusters.json"

if not os.path.exists(json_path):
    print(f"File not found: {json_path}")
else:
    with open(json_path, 'r') as f:
        data = json.load(f)
    
    clusters = data.get('clusters', [])
    duplicates = [c for c in clusters if len(c.get('visits', [])) > 1]
    
    print(f"Total clusters: {len(clusters)}")
    print(f"Duplicate clusters found: {len(duplicates)}")
    
    for d in duplicates:
        print(f"Cluster ID: {d['clusterId']}, Visits: {len(d['visits'])}")
        for v in d['visits']:
            print(f"  - Visit ID: {v['visitId']}, Similarity: {v.get('similarityScore')}")
