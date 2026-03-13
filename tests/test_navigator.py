"""Tests for the Navigator Agent."""

import sys
import os
import tempfile
import json
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.agents.navigator import NavigatorAgent

def create_mock_cartography(tmpdir):
    """Create mock cartography files for testing."""
    cart_dir = Path(tmpdir) / ".cartography"
    cart_dir.mkdir()
    
    # Create nodes JSON
    nodes = {
        "module:src/auth/user_auth.py": {
            "path": "src/auth/user_auth.py",
            "language": "python",
            "pagerank": 0.5,
            "change_velocity_30d": 3,
            "functions": [{"name": "authenticate_user"}, {"name": "create_session"}],
            "classes": ["UserAuth"],
            "imports": ["hashlib", "jwt"]
        },
        "module:src/core/processor.py": {
            "path": "src/core/processor.py",
            "language": "python",
            "pagerank": 0.8,
            "change_velocity_30d": 10,
            "functions": [{"name": "process_data"}],
            "classes": ["DataProcessor"],
            "imports": ["pandas", "numpy"]
        },
        "dataset:raw_events": {
            "name": "raw_events",
            "storage_type": "table",
            "schema_snapshot": "{}"
        },
        "dataset:daily_active_users": {
            "name": "daily_active_users",
            "storage_type": "table",
            "schema_snapshot": "{}"
        }
    }
    
    with open(cart_dir / "test_nodes.json", 'w') as f:
        json.dump(nodes, f)
    
    # Create lineage graph
    lineage = {
        "edges": [
            {
                "source": "raw_events",
                "target": "daily_active_users",
                "file": "sql/transform.sql",
                "type": "sql"
            }
        ],
        "datasets": {
            "raw_events": {},
            "daily_active_users": {}
        }
    }
    
    with open(cart_dir / "lineage_graph.json", 'w') as f:
        json.dump(lineage, f)
    
    # Create purpose statements
    purposes = {
        "src/auth/user_auth.py": "Handles user authentication including login and session management",
        "src/core/processor.py": "Core data processing pipeline for analytics"
    }
    
    with open(cart_dir / "purpose_statements.json", 'w') as f:
        json.dump(purposes, f)
    
    return cart_dir

def test_navigator():
    """Test the Navigator Agent."""
    print("🧪 Testing Navigator Agent...")
    
    # Create mock cartography data
    with tempfile.TemporaryDirectory() as tmpdir:
        cart_dir = create_mock_cartography(tmpdir)
        
        # Initialize navigator
        navigator = NavigatorAgent(str(cart_dir))
        
        # Test 1: Find implementation
        print("\n🔍 Test 1: find_implementation")
        result = navigator.find_implementation("user authentication")
        assert result['count'] > 0, "Should find matches"
        print(f"  ✅ Found {result['count']} matches")
        
        # Test 2: Trace lineage
        print("\n🔄 Test 2: trace_lineage")
        result = navigator.trace_lineage("daily_active_users")
        assert 'upstream' in result, "Should have upstream"
        print(f"  ✅ Upstream count: {result['upstream_count']}")
        
        # Test 3: Blast radius
        print("\n💥 Test 3: blast_radius")
        result = navigator.blast_radius("src/core/processor.py")
        assert 'downstream_count' in result, "Should have downstream count"
        print(f"  ✅ Downstream modules: {result['downstream_count']}")
        
        # Test 4: Explain module
        print("\n📖 Test 4: explain_module")
        result = navigator.explain_module("src/auth/user_auth.py", use_llm=False)
        assert 'purpose' in result, "Should have purpose"
        print(f"  ✅ Purpose: {result['purpose'][:50]}...")
        
        # Test 5: Natural language query
        print("\n❓ Test 5: natural language query")
        result = navigator.query("what produces daily_active_users?")
        assert result['tool'] == 'trace_lineage', "Should route to trace_lineage"
        print(f"  ✅ Routed to: {result['tool']}")
        
        print("\n✅ All navigator tests passed!")

if __name__ == "__main__":
    test_navigator()