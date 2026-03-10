"""Tests for the Knowledge Graph module."""

import sys
import os
from pathlib import Path

# Add parent directory to path so we can import src
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.graph.knowledge_graph import KnowledgeGraph

def test_basic_operations():
    """Test basic graph operations."""
    print("\n🔧 Testing Basic Operations...")
    
    # Create graph
    kg = KnowledgeGraph("test_basic")
    
    # Add a module
    module_id = kg.add_module_node(
        path="test.py",
        language="python",
        purpose_statement="Test module"
    )
    
    # Verify module was added
    node = kg.get_node(module_id)
    assert node is not None, "Module should exist"
    assert node["path"] == "test.py", "Path should match"
    print(f"✅ Module added: {node}")
    
    # Add a dataset
    dataset_id = kg.add_dataset_node(
        name="test_table",
        storage_type="table"
    )
    
    # Add edge
    kg.add_import_edge("test.py", "nonexistent.py")  # Should warn but not crash
    
    print("✅ Basic operations passed")
    return kg

def test_serialization():
    """Test saving and loading."""
    print("\n💾 Testing Serialization...")
    
    kg = KnowledgeGraph("test_serialize")
    
    # Add some data
    kg.add_module_node("file1.py", "python")
    kg.add_module_node("file2.py", "python")
    kg.add_dataset_node("data1")
    kg.add_import_edge("file1.py", "file2.py")
    
    # Save to temp directory
    import tempfile
    with tempfile.TemporaryDirectory() as tmpdir:
        files = kg.serialize_graph(tmpdir)
        
        # Check files were created
        for file_type, file_path in files.items():
            assert Path(file_path).exists(), f"{file_type} file should exist"
            print(f"✅ {file_type} saved: {Path(file_path).name}")
    
    print("✅ Serialization passed")

def test_queries():
    """Test query methods."""
    print("\n🔍 Testing Queries...")
    
    kg = KnowledgeGraph("test_queries")
    
    # Build a small dependency graph
    kg.add_module_node("ingest.py", "python")
    kg.add_module_node("transform.py", "python")
    kg.add_dataset_node("raw_data")
    kg.add_dataset_node("clean_data")
    
    # Add transformation
    trans_id = kg.add_transformation_node(
        source_datasets=["raw_data"],
        target_datasets=["clean_data"],
        transformation_type="python",
        source_file="transform.py"
    )
    
    # Add edges
    kg.add_consumes_edge(trans_id, "raw_data")
    kg.add_produces_edge(trans_id, "clean_data")
    
    # Test upstream sources
    upstream = kg.get_upstream_sources("dataset:clean_data")
    print(f"Upstream of clean_data: {upstream}")
    assert len(upstream) > 0, "Should have upstream"
    
    # Test blast radius
    downstream = kg.get_downstream_dependents("dataset:raw_data")
    print(f"Downstream of raw_data: {downstream}")
    
    print("✅ Queries passed")

if __name__ == "__main__":
    print("🧪 Running Knowledge Graph Tests")
    print("=" * 50)
    
    test_basic_operations()
    test_serialization()
    test_queries()
    
    print("\n" + "=" * 50)
    print("✅ ALL TESTS PASSED!")