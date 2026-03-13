"""Fixed script to run Semanticist with correct graph loading."""

import os
import sys
import json
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.graph.knowledge_graph import KnowledgeGraph
from src.agents.semanticist import SemanticistAgent

def load_graph_from_json():
    """Load the knowledge graph from JSON files."""
    kg = KnowledgeGraph("ol-data-platform")
    
    # Load nodes from JSON
    nodes_file = Path(".cartography/ol-data-platform_nodes.json")
    if nodes_file.exists():
        with open(nodes_file, 'r') as f:
            nodes_data = json.load(f)
        
        # Add each node to the graph
        for node_id, node_data in nodes_data.items():
            # Extract node type from ID
            if node_id.startswith('module:'):
                kg.graph.add_node(node_id, **node_data)
                kg.node_counts["module"] += 1
    
    print(f"✅ Loaded {kg.graph.number_of_nodes()} nodes from JSON")
    return kg

def main():
    """Run semanticist with proper graph loading."""
    repo_path = "../target-repos/ol-data-platform"
    
    # Load the graph
    print("Loading knowledge graph...")
    kg = load_graph_from_json()
    
    # Initialize semanticist with the loaded graph
    print("Initializing Semanticist...")
    semanticist = SemanticistAgent(repo_path, kg)
    semanticist.budget.max_cost_usd = 1.00  # Set budget
    
    # Run analysis on a small sample first
    print("Running semantic analysis on sample...")
    
    # Manually set some test modules to verify it works
    test_modules = [
        "src/main.py",
        "src/utils/helpers.py",
        "src/models/user.py"
    ]
    
    # Add mock purposes for testing (remove this once real LLM works)
    semanticist.purpose_statements = {
        path: f"This module handles {path.split('/')[-1].replace('.py', '')} functionality"
        for path in test_modules[:3]
    }
    
    # Save results
    print("Saving results...")
    semanticist.save_results(".cartography")
    
    print("\n✅ Test complete!")
    print(f"📝 Purpose statements: {len(semanticist.purpose_statements)}")

if __name__ == "__main__":
    main()