"""Tests for the Archivist Agent."""

import sys
import os
import tempfile
import json
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.agents.archivist import ArchivistAgent

def create_mock_cartography(tmpdir):
    """Create mock cartography files for testing."""
    cart_dir = Path(tmpdir) / ".cartography"
    cart_dir.mkdir()
    
    # Create surveyor_summary.json
    with open(cart_dir / "surveyor_summary.json", 'w') as f:
        json.dump({
            "files_analyzed": 150,
            "languages": {"python": 100, "sql": 30, "yaml": 20}
        }, f)
    
    # Create hydrologist_summary.json
    with open(cart_dir / "hydrologist_summary.json", 'w') as f:
        json.dump({
            "sources": ["raw_events", "raw_users"],
            "sinks": ["final_report", "dashboard"]
        }, f)
    
    # Create purpose_statements.json
    with open(cart_dir / "purpose_statements.json", 'w') as f:
        json.dump({
            "src/main.py": "Main entry point for the application",
            "src/utils.py": "Utility functions for data processing",
            "src/models.py": "Data models and database schemas"
        }, f)
    
    # Create doc_drift.json
    with open(cart_dir / "doc_drift.json", 'w') as f:
        json.dump({
            "src/main.py": {"is_accurate": "yes"},
            "src/utils.py": {"is_accurate": "no", "missing": ["error handling"]}
        }, f)
    
    # Create domain_clusters.json
    with open(cart_dir / "domain_clusters.json", 'w') as f:
        json.dump({
            "Ingestion": ["src/ingest.py"],
            "Processing": ["src/process.py"],
            "API": ["src/api.py"]
        }, f)
    
    # Create day_one_answers.json
    with open(cart_dir / "day_one_answers.json", 'w') as f:
        json.dump({
            "primary_ingestion_path": {
                "answer": "Data comes from Kafka",
                "evidence": ["src/kafka.py"]
            }
        }, f)
    
    # Create nodes JSON
    with open(cart_dir / "test_repo_nodes.json", 'w') as f:
        json.dump({
            "module:src/main.py": {
                "path": "src/main.py",
                "language": "python",
                "pagerank": 0.5,
                "change_velocity_30d": 10
            },
            "module:src/utils.py": {
                "path": "src/utils.py",
                "language": "python",
                "pagerank": 0.3,
                "change_velocity_30d": 5
            }
        }, f)
    
    return cart_dir

def test_archivist():
    """Test the Archivist Agent."""
    print("🧪 Testing Archivist Agent...")
    
    # Create a temporary directory
    with tempfile.TemporaryDirectory() as tmpdir:
        # Create mock cartography data
        cart_dir = create_mock_cartography(tmpdir)
        
        # Create a fake repo
        repo_dir = Path(tmpdir) / "repo"
        repo_dir.mkdir()
        
        # Initialize archivist
        archivist = ArchivistAgent(str(repo_dir), str(cart_dir))
        
        # Generate files
        files = archivist.save_all()
        
        # Check if files were created
        for name, path in files.items():
            assert Path(path).exists(), f"{name} should exist"
            print(f"  ✅ {name} created: {Path(path).name}")
        
        # Check content of CODEBASE.md
        with open(files['codebase_md'], 'r', encoding='utf-8') as f:
            content = f.read()
            assert "Architecture Overview" in content
            assert "Critical Path" in content
            print(f"  ✅ CODEBASE.md has {len(content)} chars")
        
        # Check content of onboarding_brief.md
        with open(files['onboarding_brief'], 'r', encoding='utf-8') as f:
            content = f.read()
            assert "Onboarding Brief" in content
            assert "Quick Stats" in content
            print(f"  ✅ onboarding_brief.md has {len(content)} chars")
        
        # Check trace file
        with open(files['trace'], 'r', encoding='utf-8') as f:
            lines = f.readlines()
            assert len(lines) > 0
            print(f"  ✅ trace has {len(lines)} entries")
        
        print("\n✅ All archivist tests passed!")

if __name__ == "__main__":
    test_archivist()