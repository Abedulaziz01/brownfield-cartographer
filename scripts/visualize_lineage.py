#!/usr/bin/env python3
"""Visualize the data lineage graph from hydrologist agent."""

import json
import networkx as nx
import matplotlib.pyplot as plt
from pathlib import Path

def visualize_lineage(graph_file: str = ".cartography/lineage_graph.json", 
                      output_file: str = None,
                      max_nodes: int = 50):
    """
    Visualize the data lineage graph.
    
    Args:
        graph_file: Path to the lineage graph JSON file
        output_file: Optional path to save the visualization
        max_nodes: Maximum number of nodes to display (for large graphs)
    """
    print(f"📊 Loading lineage graph from {graph_file}...")
    
    with open(graph_file) as f:
        data = json.load(f)
    
    # Create a new graph
    G = nx.DiGraph()
    
    # Add dataset nodes
    datasets = data.get("datasets", {})
    print(f"Found {len(datasets)} datasets")
    
    # Add transformation nodes (if they exist in the graph)
    transformations = data.get("transformations", [])
    print(f"Found {len(transformations)} transformations")
    
    # Add edges
    edges = data.get("edges", [])
    print(f"Found {len(edges)} data flow edges")
    
    # If graph is too large, sample it
    if len(datasets) > max_nodes:
        print(f"Graph is large ({len(datasets)} nodes). Showing first {max_nodes} nodes...")
        # Get the first max_nodes datasets
        sampled_datasets = dict(list(datasets.items())[:max_nodes])
        # Filter edges that involve sampled datasets
        sampled_dataset_names = set(sampled_datasets.keys())
        sampled_edges = [e for e in edges if e["source"] in sampled_dataset_names 
                        and e["target"] in sampled_dataset_names]
    else:
        sampled_datasets = datasets
        sampled_edges = edges
    
    # Add nodes to graph - FIXED: Don't use 'type' as a keyword argument
    for name, metadata in sampled_datasets.items():
        node_type = metadata.get("type", "unknown")
        # Store type as a node attribute, not as a keyword argument
        G.add_node(name, node_type=node_type, **metadata)
    
    # Add edges to graph
    for edge in sampled_edges:
        G.add_edge(edge["source"], edge["target"], 
                  edge_type=edge.get("type", "unknown"),
                  file=edge.get("file", ""))
    
    # Draw the graph
    plt.figure(figsize=(20, 16))
    
    # Use different colors for different node types
    color_map = {
        "table": "lightblue",
        "file": "lightgreen",
        "file_path": "lightyellow",
        "dataset": "lightgray",
        "unknown": "gray"
    }
    
    node_colors = [color_map.get(G.nodes[node].get("node_type", "unknown"), "gray") 
                   for node in G.nodes()]
    
    # Create layout
    pos = nx.spring_layout(G, k=3, iterations=50)
    
    # Draw nodes
    nx.draw_networkx_nodes(G, pos, node_color=node_colors, 
                          node_size=2000, alpha=0.8)
    
    # Draw edges with arrows
    nx.draw_networkx_edges(G, pos, edge_color='gray', 
                          arrows=True, arrowsize=20, width=1, alpha=0.5)
    
    # Draw labels
    nx.draw_networkx_labels(G, pos, font_size=8, font_weight='bold')
    
    plt.title(f"Data Lineage Graph - {len(G.nodes)} datasets, {len(G.edges)} flows")
    plt.axis('off')
    
    if output_file:
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"✅ Saved visualization to {output_file}")
    
    plt.show()
    
    # Print some statistics
    print("\n📊 Lineage Graph Statistics:")
    print(f"  Total datasets: {len(datasets)}")
    print(f"  Total transformations: {len(transformations)}")
    print(f"  Total data flows: {len(edges)}")
    
    # Find sources (no incoming edges) and sinks (no outgoing edges)
    sources = data.get("sources", [])
    sinks = data.get("sinks", [])
    
    print(f"\n📥 Source datasets (no inputs): {len(sources)}")
    if sources:
        print(f"  Examples: {sources[:5]}")
    
    print(f"\n📤 Sink datasets (no outputs): {len(sinks)}")
    if sinks:
        print(f"  Examples: {sinks[:5]}")

def visualize_specific_lineage(dataset_name: str, graph_file: str = ".cartography/lineage_graph.json"):
    """
    Visualize lineage for a specific dataset.
    
    Args:
        dataset_name: Name of the dataset to visualize
        graph_file: Path to the lineage graph JSON file
    """
    with open(graph_file) as f:
        data = json.load(f)
    
    if dataset_name not in data["datasets"]:
        print(f"❌ Dataset '{dataset_name}' not found")
        print(f"Available datasets: {list(data['datasets'].keys())[:10]}...")
        return
    
    # Create a subgraph with the dataset and its neighbors
    G = nx.DiGraph()
    
    # Add the target dataset
    target_metadata = data["datasets"][dataset_name]
    G.add_node(dataset_name, node_type=target_metadata.get("type", "unknown"), **target_metadata)
    
    # Add upstream dependencies (what produces this)
    upstream = set()
    for edge in data["edges"]:
        if edge["target"] == dataset_name:
            source = edge["source"]
            upstream.add(source)
            source_metadata = data["datasets"].get(source, {})
            G.add_node(source, node_type=source_metadata.get("type", "unknown"), **source_metadata)
            G.add_edge(source, dataset_name, edge_type=edge.get("type", "unknown"), file=edge.get("file", ""))
    
    # Add downstream dependencies (what consumes this)
    downstream = set()
    for edge in data["edges"]:
        if edge["source"] == dataset_name:
            target = edge["target"]
            downstream.add(target)
            target_metadata = data["datasets"].get(target, {})
            G.add_node(target, node_type=target_metadata.get("type", "unknown"), **target_metadata)
            G.add_edge(dataset_name, target, edge_type=edge.get("type", "unknown"), file=edge.get("file", ""))
    
    # Draw the subgraph
    plt.figure(figsize=(15, 10))
    pos = nx.spring_layout(G, k=2, iterations=50)
    
    # Color nodes by type
    color_map = {
        "table": "lightblue",
        "file": "lightgreen",
        "file_path": "lightyellow",
        "dataset": "lightgray",
        "unknown": "gray"
    }
    
    node_colors = [color_map.get(G.nodes[node].get("node_type", "unknown"), "gray") 
                   for node in G.nodes()]
    
    # Highlight the target dataset
    target_idx = list(G.nodes()).index(dataset_name)
    node_colors[target_idx] = "red"
    
    nx.draw(G, pos, node_color=node_colors, node_size=2000,
           with_labels=True, font_size=8, font_weight='bold',
           arrows=True, arrowsize=20, edge_color='gray', alpha=0.7)
    
    plt.title(f"Lineage for: {dataset_name}\nUpstream: {len(upstream)} | Downstream: {len(downstream)}")
    plt.axis('off')
    plt.tight_layout()
    plt.show()
    
    print(f"\n📊 Lineage for '{dataset_name}':")
    print(f"  Upstream (what feeds it): {list(upstream)}")
    print(f"  Downstream (what it feeds): {list(downstream)}")

def print_graph_stats(graph_file: str = ".cartography/lineage_graph.json"):
    """Print statistics about the lineage graph without visualizing."""
    
    with open(graph_file) as f:
        data = json.load(f)
    
    datasets = data.get("datasets", {})
    edges = data.get("edges", [])
    transformations = data.get("transformations", [])
    
    print("\n" + "="*60)
    print("📊 LINEAGE GRAPH STATISTICS")
    print("="*60)
    print(f"Total datasets: {len(datasets)}")
    print(f"Total transformations: {len(transformations)}")
    print(f"Total data flows: {len(edges)}")
    
    # Dataset types breakdown
    type_counts = {}
    for name, ds in datasets.items():
        ds_type = ds.get("type", "unknown")
        type_counts[ds_type] = type_counts.get(ds_type, 0) + 1
    
    print("\n📁 Dataset types:")
    for ds_type, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True):
        print(f"  {ds_type}: {count}")
    
    # Sources and sinks
    sources = data.get("sources", [])
    sinks = data.get("sinks", [])
    
    print(f"\n📥 Sources (no inputs): {len(sources)}")
    if sources:
        print(f"  First 10: {sources[:10]}")
    
    print(f"\n📤 Sinks (no outputs): {len(sinks)}")
    if sinks:
        print(f"  First 10: {sinks[:10]}")
    
    # Most connected datasets
    in_degree = {}
    out_degree = {}
    
    for edge in edges:
        out_degree[edge["source"]] = out_degree.get(edge["source"], 0) + 1
        in_degree[edge["target"]] = in_degree.get(edge["target"], 0) + 1
    
    print("\n🔝 Most referenced datasets (incoming edges):")
    top_in = sorted(in_degree.items(), key=lambda x: x[1], reverse=True)[:5]
    for ds, count in top_in:
        print(f"  {ds}: {count} incoming flows")
    
    print("\n🔝 Most referencing datasets (outgoing edges):")
    top_out = sorted(out_degree.items(), key=lambda x: x[1], reverse=True)[:5]
    for ds, count in top_out:
        print(f"  {ds}: {count} outgoing flows")
    
    print("\n" + "="*60)

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Visualize data lineage graph")
    parser.add_argument("--file", default=".cartography/lineage_graph.json",
                       help="Path to lineage graph JSON file")
    parser.add_argument("--dataset", help="Specific dataset to visualize")
    parser.add_argument("--output", help="Save visualization to file")
    parser.add_argument("--max-nodes", type=int, default=50,
                       help="Maximum nodes to display")
    parser.add_argument("--stats", action="store_true",
                       help="Just print statistics without visualization")
    
    args = parser.parse_args()
    
    if not Path(args.file).exists():
        print(f"❌ File not found: {args.file}")
        print("Make sure you've run hydrologist first:")
        print("  python -m src.agents.hydrologist --repo <your-repo> --output .cartography")
        sys.exit(1)
    
    if args.stats:
        print_graph_stats(args.file)
    elif args.dataset:
        visualize_specific_lineage(args.dataset, args.file)
    else:
        visualize_lineage(args.file, args.output, args.max_nodes)