#!/usr/bin/env python3
"""CLI entry point for Brownfield Cartographer."""

import argparse
import sys
from pathlib import Path

def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Brownfield Cartographer - Codebase Intelligence")
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # Analyze command
    analyze_parser = subparsers.add_parser("analyze", help="Run full analysis pipeline")
    analyze_parser.add_argument("--repo", type=str, required=True, help="Path to repository")
    analyze_parser.add_argument("--output", type=str, default=".cartography", help="Output directory")
    analyze_parser.add_argument("--sample", type=int, help="Sample size for semantic analysis")
    analyze_parser.add_argument("--budget", type=float, default=5.0, help="LLM budget in USD")
    analyze_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    # Query command
    query_parser = subparsers.add_parser("query", help="Query the knowledge graph")
    query_parser.add_argument("--cartography", type=str, default=".cartography", help="Cartography directory")
    query_parser.add_argument("--repo", type=str, help="Path to repository (for reading files)")
    query_parser.add_argument("--question", type=str, help="Single question to ask")
    query_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    # Surveyor command
    surveyor_parser = subparsers.add_parser("surveyor", help="Run only Surveyor agent")
    surveyor_parser.add_argument("--repo", type=str, required=True, help="Path to repository")
    surveyor_parser.add_argument("--output", type=str, default=".cartography", help="Output directory")
    surveyor_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    # Hydrologist command
    hydrologist_parser = subparsers.add_parser("hydrologist", help="Run only Hydrologist agent")
    hydrologist_parser.add_argument("--repo", type=str, required=True, help="Path to repository")
    hydrologist_parser.add_argument("--output", type=str, default=".cartography", help="Output directory")
    hydrologist_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    # Semanticist command
    semanticist_parser = subparsers.add_parser("semanticist", help="Run only Semanticist agent")
    semanticist_parser.add_argument("--repo", type=str, required=True, help="Path to repository")
    semanticist_parser.add_argument("--kg", type=str, help="Path to knowledge graph file")
    semanticist_parser.add_argument("--output", type=str, default=".cartography", help="Output directory")
    semanticist_parser.add_argument("--sample", type=int, help="Sample size")
    semanticist_parser.add_argument("--budget", type=float, default=5.0, help="LLM budget")
    semanticist_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    # Archivist command
    archivist_parser = subparsers.add_parser("archivist", help="Run only Archivist agent")
    archivist_parser.add_argument("--repo", type=str, required=True, help="Path to repository")
    archivist_parser.add_argument("--cartography", type=str, default=".cartography", help="Cartography directory")
    archivist_parser.add_argument("--verbose", action="store_true", help="Enable verbose logging")
    
    args = parser.parse_args()
    
    if args.command == "analyze":
        # Run full pipeline
        print("🚀 Running full analysis pipeline...")
        
        # Import agents
        from agents.surveyor import main as surveyor_main
        from agents.hydrologist import main as hydrologist_main
        from agents.semanticist import main as semanticist_main
        from agents.archivist import main as archivist_main
        
        # Run Surveyor
        print("\n📋 Phase 1: Surveyor Agent")
        sys.argv = ["surveyor.py", "--repo", args.repo, "--output", args.output]
        if args.verbose:
            sys.argv.append("--verbose")
        surveyor_main()
        
        # Run Hydrologist
        print("\n💧 Phase 2: Hydrologist Agent")
        sys.argv = ["hydrologist.py", "--repo", args.repo, "--output", args.output]
        if args.verbose:
            sys.argv.append("--verbose")
        hydrologist_main()
        
        # Run Semanticist
        print("\n🧠 Phase 3: Semanticist Agent")
        sys.argv = ["semanticist.py", "--repo", args.repo, "--output", args.output, "--budget", str(args.budget)]
        if args.sample:
            sys.argv.extend(["--sample", str(args.sample)])
        if args.verbose:
            sys.argv.append("--verbose")
        semanticist_main()
        
        # Run Archivist
        print("\n📚 Phase 4: Archivist Agent")
        sys.argv = ["archivist.py", "--repo", args.repo, "--cartography", args.output]
        if args.verbose:
            sys.argv.append("--verbose")
        archivist_main()
        
        print("\n✅ Full analysis complete!")
        print(f"📁 Results saved to {args.output}/")
        
    elif args.command == "query":
        # Run navigator
        from agents.navigator import main as navigator_main
        
        sys.argv = ["navigator.py", "--cartography", args.cartography]
        if args.repo:
            sys.argv.extend(["--repo", args.repo])
        if args.question:
            sys.argv.extend(["--query", args.question])
        if args.verbose:
            sys.argv.append("--verbose")
        navigator_main()
        
    elif args.command == "surveyor":
        from agents.surveyor import main as surveyor_main
        surveyor_main()
        
    elif args.command == "hydrologist":
        from agents.hydrologist import main as hydrologist_main
        hydrologist_main()
        
    elif args.command == "semanticist":
        from agents.semanticist import main as semanticist_main
        semanticist_main()
        
    elif args.command == "archivist":
        from agents.archivist import main as archivist_main
        archivist_main()
        
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
    # Add to cli.py

def clone_github_repo(url: str, target_dir: str) -> str:
    """Clone a GitHub repository."""
    import subprocess
    import tempfile
    
    # Create temp directory
    repo_name = url.split('/')[-1].replace('.git', '')
    clone_path = os.path.join(target_dir, repo_name)
    
    if os.path.exists(clone_path):
        logger.info(f"Repository already exists at {clone_path}")
        return clone_path
    
    # Clone the repo
    logger.info(f"Cloning {url} to {clone_path}")
    subprocess.run(['git', 'clone', url, clone_path], check=True)
    
    return clone_path

def incremental_update(repo_path: str, cartography_dir: str) -> bool:
    """Check if incremental update is needed and run it."""
    from datetime import datetime
    import subprocess
    
    # Check last analysis time
    last_run_file = Path(cartography_dir) / "last_run.txt"
    
    if not last_run_file.exists():
        logger.info("No previous run found, performing full analysis")
        return False
    
    # Get last commit hash
    result = subprocess.run(
        ['git', 'rev-parse', 'HEAD'],
        cwd=repo_path,
        capture_output=True,
        text=True
    )
    current_hash = result.stdout.strip()
    
    # Check if anything changed
    result = subprocess.run(
        ['git', 'diff', '--name-only', 'HEAD'],
        cwd=repo_path,
        capture_output=True,
        text=True
    )
    changed_files = result.stdout.strip().split('\n')
    
    if not changed_files or changed_files == ['']:
        logger.info("No changes detected since last run")
        return True
    
    # Run incremental analysis on changed files
    logger.info(f"Found {len(changed_files)} changed files, running incremental update")
    
    # Here you would run analysis on only changed files
    # This requires modifying your agents to accept file lists
    
    return True