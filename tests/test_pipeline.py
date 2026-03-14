#!/usr/bin/env python3
"""Test script to verify pipeline works."""

import sys
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from orchestrator import PipelineOrchestrator

def test_pipeline():
    """Test the pipeline with a small sample."""
    
    # Use a small public repo for testing
    test_repo = "https://github.com/dbt-labs/jaffle_shop.git"
    
    print(f"\n🧪 Testing pipeline with: {test_repo}")
    print("="*60)
    
    # Run orchestrator with small budget and sample
    orch = PipelineOrchestrator(test_repo, ".cartography_test")
    result = orch.run_pipeline(budget=0.50, sample=10)
    
    if result['success']:
        print("\n✅ Test passed!")
        return 0
    else:
        print("\n❌ Test failed!")
        return 1

if __name__ == "__main__":
    sys.exit(test_pipeline())