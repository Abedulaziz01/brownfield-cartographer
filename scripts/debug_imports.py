#!/usr/bin/env python3
"""Debug script to analyze import detection issues."""

import sys
import os
import json
from pathlib import Path
from collections import Counter

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from src.analyzers.language_router import LanguageRouter

def debug_imports(repo_path: str):
    """Debug import detection on a repository."""
    print(f"\n🔍 Debugging import detection for: {repo_path}")
    
    # Initialize
    analyzer = TreeSitterAnalyzer()
    router = LanguageRouter()
    
    # Find all Python files
    python_files = []
    for root, dirs, files in os.walk(repo_path):
        dirs[:] = [d for d in dirs if d not in ['.git', '__pycache__', 'node_modules']]
        for file in files:
            if file.endswith('.py'):
                python_files.append(os.path.join(root, file))
    
    print(f"\n📊 Found {len(python_files)} Python files")
    
    # Sample analysis on first 10 files
    sample_size = min(10, len(python_files))
    print(f"\n🔬 Deep analysis of first {sample_size} files:")
    
    total_imports = 0
    files_with_imports = 0
    
    for i, file_path in enumerate(python_files[:sample_size]):
        rel_path = os.path.relpath(file_path, repo_path)
        print(f"\n--- File {i+1}: {rel_path} ---")
        
        # Read file content
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        print(f"Lines: {len(content.splitlines())}")
        
        # Show first few lines
        first_lines = content.split('\n')[:5]
        print("First 5 lines:")
        for line in first_lines:
            if line.strip():
                print(f"  {line[:80]}")
        
        # Check for import statements manually
        import_lines = []
        for line in content.split('\n'):
            line = line.strip()
            if line.startswith('import ') or line.startswith('from '):
                import_lines.append(line)
        
        print(f"\nManual import scan found: {len(import_lines)} import lines")
        for line in import_lines[:3]:
            print(f"  {line}")
        
        # Run analyzer
        result = analyzer.analyze_file(file_path, repo_path)
        
        print(f"Analyzer found:")
        print(f"  - imports: {result.get('imports', [])}")
        print(f"  - raw_imports: {result.get('raw_imports', [])}")
        print(f"  - parse_method: {result.get('parse_method', 'unknown')}")
        
        if result.get('imports'):
            files_with_imports += 1
            total_imports += len(result['imports'])
    
    print(f"\n📈 Summary:")
    print(f"  Files with imports detected: {files_with_imports}/{sample_size}")
    print(f"  Average imports per file: {total_imports/sample_size if sample_size else 0:.2f}")
    
    # Check tree-sitter availability
    print(f"\n🛠️  Tree-sitter status:")
    print(f"  Available: {analyzer.router.tree_sitter_available}")
    print(f"  Parsers loaded: {list(analyzer.router.parsers.keys())}")
    
    return

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python scripts/debug_imports.py <repo_path>")
        sys.exit(1)
    
    debug_imports(sys.argv[1])