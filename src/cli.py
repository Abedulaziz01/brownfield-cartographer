#!/usr/bin/env python3
"""CLI entry point for Brownfield Cartographer."""

import argparse
import sys
import logging
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent))

from orchestrator import PipelineOrchestrator
from agents.navigator import main as navigator_main


def setup_logging(verbose: bool = False):
    """Setup logging configuration."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%H:%M:%S'
    )


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description="Brownfield Cartographer - Codebase Intelligence System",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Analyze a local repository
  python src/cli.py analyze --repo ../path/to/repo --budget 5.00

  # Analyze a GitHub repository (auto-cloned)
  python src/cli.py analyze --repo https://github.com/username/repo.git --budget 5.00

  # Quick analysis with sample (cheaper)
  python src/cli.py analyze --repo ../path/to/repo --sample 50 --budget 1.00

  # Query an analyzed codebase
  python src/cli.py query --question "what produces daily_active_users?"

  # Interactive query mode
  python src/cli.py query
        """
    )
    
    subparsers = parser.add_subparsers(dest="command", help="Commands")
    
    # ========== ANALYZE COMMAND ==========
    analyze_parser = subparsers.add_parser("analyze", help="Run full analysis pipeline")
    analyze_parser.add_argument("--repo", type=str, required=True,
                               help="Path to repository or GitHub URL")
    analyze_parser.add_argument("--output", type=str, default=".cartography",
                               help="Output directory (default: .cartography)")
    analyze_parser.add_argument("--sample", type=int, default=0,
                               help="Sample N files for semantic analysis (cheaper)")
    analyze_parser.add_argument("--budget", type=float, default=5.0,
                               help="Maximum LLM budget in USD (default: 5.0)")
    analyze_parser.add_argument("--verbose", action="store_true",
                               help="Enable verbose logging")
    
    # ========== QUERY COMMAND ==========
    query_parser = subparsers.add_parser("query", help="Query the knowledge graph")
    query_parser.add_argument("--cartography", type=str, default=".cartography",
                             help="Cartography directory (default: .cartography)")
    query_parser.add_argument("--repo", type=str,
                             help="Path to repository (for reading files)")
    query_parser.add_argument("--question", type=str,
                             help="Single question to ask (if omitted, starts interactive mode)")
    query_parser.add_argument("--verbose", action="store_true",
                             help="Enable verbose logging")
    
    # ========== SURVEYOR COMMAND ==========
    surveyor_parser = subparsers.add_parser("surveyor", help="Run only Surveyor agent")
    surveyor_parser.add_argument("--repo", type=str, required=True,
                                help="Path to repository")
    surveyor_parser.add_argument("--output", type=str, default=".cartography",
                                help="Output directory")
    surveyor_parser.add_argument("--verbose", action="store_true",
                                help="Enable verbose logging")
    
    # ========== HYDROLOGIST COMMAND ==========
    hydrologist_parser = subparsers.add_parser("hydrologist", help="Run only Hydrologist agent")
    hydrologist_parser.add_argument("--repo", type=str, required=True,
                                   help="Path to repository")
    hydrologist_parser.add_argument("--output", type=str, default=".cartography",
                                   help="Output directory")
    hydrologist_parser.add_argument("--verbose", action="store_true",
                                   help="Enable verbose logging")
    
    # ========== SEMANTICIST COMMAND ==========
    semanticist_parser = subparsers.add_parser("semanticist", help="Run only Semanticist agent")
    semanticist_parser.add_argument("--repo", type=str, required=True,
                                   help="Path to repository")
    semanticist_parser.add_argument("--kg", type=str,
                                   help="Path to knowledge graph file (optional)")
    semanticist_parser.add_argument("--output", type=str, default=".cartography",
                                   help="Output directory")
    semanticist_parser.add_argument("--sample", type=int, default=0,
                                   help="Sample size")
    semanticist_parser.add_argument("--budget", type=float, default=5.0,
                                   help="LLM budget")
    semanticist_parser.add_argument("--verbose", action="store_true",
                                   help="Enable verbose logging")
    
    # ========== ARCHIVIST COMMAND ==========
    archivist_parser = subparsers.add_parser("archivist", help="Run only Archivist agent")
    archivist_parser.add_argument("--repo", type=str, required=True,
                                 help="Path to repository")
    archivist_parser.add_argument("--cartography", type=str, default=".cartography",
                                 help="Cartography directory")
    archivist_parser.add_argument("--verbose", action="store_true",
                                 help="Enable verbose logging")
    
    args = parser.parse_args()
    
    # Setup logging
    verbose = getattr(args, 'verbose', False)
    setup_logging(verbose)
    
    # Route commands
    if args.command == "analyze":
        # Run full pipeline via orchestrator
        print("\n" + "="*60)
        print("🚀 BROWNFIELD CARTOGRAPHER - FULL ANALYSIS PIPELINE")
        print("="*60)
        print(f"Repository: {args.repo}")
        print(f"Output dir: {args.output}")
        print(f"LLM Budget: ${args.budget}")
        if args.sample:
            print(f"Sample:     {args.sample} files")
        print("="*60 + "\n")
        
        orchestrator = PipelineOrchestrator(args.repo, args.output)
        result = orchestrator.run_pipeline(
            budget=args.budget,
            sample=args.sample
        )
        
        if result['success']:
            print(f"\n✅ Analysis complete!")
            print(f"📁 Results saved to {args.output}/")
            print(f"⏱️  Total time: {result['total_time']:.2f}s")
            
            # Show summary
            status = result['status']
            print(f"\n📊 Agent Status:")
            print(f"  Surveyor:    {'✅' if status['surveyor']['completed'] else '❌'} ({status['surveyor'].get('time', 0):.1f}s)")
            print(f"  Hydrologist: {'✅' if status['hydrologist']['completed'] else '❌'} ({status['hydrologist'].get('time', 0):.1f}s)")
            print(f"  Semanticist: {'✅' if status['semanticist']['completed'] else '❌'} ({status['semanticist'].get('time', 0):.1f}s)")
            print(f"  Archivist:   {'✅' if status['archivist']['completed'] else '❌'} ({status['archivist'].get('time', 0):.1f}s)")
            
            return 0
        else:
            print(f"\n❌ Analysis failed: {result.get('error', 'Unknown error')}")
            return 1
        
    elif args.command == "query":
        # Run navigator
        sys.argv = ["navigator.py"]
        if args.cartography:
            sys.argv.extend(["--cartography", args.cartography])
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
        return 1
    
    return 0


if __name__ == "__main__":
    sys.exit(main())