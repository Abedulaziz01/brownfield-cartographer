#!/usr/bin/env python3
"""Compare Surveyor (code structure) and Hydrologist (data lineage) graphs."""

import json
from pathlib import Path

def compare_graphs(surveyor_file: str = ".cartography/module_graph.json",
                   hydrologist_file: str = ".cartography/lineage_graph.json"):
    """Compare statistics from both agents."""
    
    print("="*60)
    print("📊 SURVEYOR vs HYDROLOGIST COMPARISON")
    print("="*60)
    
    # Load Surveyor graph
    if Path(surveyor_file).exists():
        with open(surveyor_file) as f:
            surveyor_data = json.load(f)
        
        nodes = surveyor_data.get("nodes", [])
        links = surveyor_data.get("links", [])
        
        print(f"\n🔍 Surveyor (Code Structure):")
        print(f"  Modules found: {len(nodes)}")
        print(f"  Dependencies: {len(links)}")
    else:
        print(f"\n❌ Surveyor file not found: {surveyor_file}")
    
    # Load Hydrologist graph
    if Path(hydrologist_file).exists():
        with open(hydrologist_file) as f:
            hydrologist_data = json.load(f)
        
        datasets = hydrologist_data.get("datasets", {})
        edges = hydrologist_data.get("edges", [])
        transformations = hydrologist_data.get("transformations", [])
        
        print(f"\n🌊 Hydrologist (Data Lineage):")
        print(f"  Datasets found: {len(datasets)}")
        print(f"  Data flows: {len(edges)}")
        print(f"  Transformations: {len(transformations)}")
        
        # Dataset types breakdown
        type_counts = {}
        for ds in datasets.values():
            ds_type = ds.get("type", "unknown")
            type_counts[ds_type] = type_counts.get(ds_type, 0) + 1
        
        print(f"\n  Dataset types:")
        for ds_type, count in type_counts.items():
            print(f"    {ds_type}: {count}")
    else:
        print(f"\n❌ Hydrologist file not found: {hydrologist_file}")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    compare_graphs()