#!/usr/bin/env python3
"""Archivist Agent - Creates living documentation from codebase analysis.

This agent generates:
- CODEBASE.md: Living context file for AI agents
- onboarding_brief.md: Day-One brief for new engineers
- cartography_trace.jsonl: Audit trail of all analysis
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Optional, Any, Tuple
import logging

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from graph.knowledge_graph import KnowledgeGraph

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class ArchivistAgent:
    """
    Archivist Agent - Creates living documentation from analysis results.
    
    Responsibilities:
    - Generate CODEBASE.md (AI context file)
    - Generate onboarding_brief.md (human-readable summary)
    - Create cartography_trace.jsonl (audit trail)
    """
    
    def __init__(self, repo_path: str, cartography_dir: str = ".cartography"):
        """
        Initialize the Archivist Agent.
        
        Args:
            repo_path: Path to the repository
            cartography_dir: Directory containing analysis results
        """
        self.repo_path = Path(repo_path).resolve()
        self.repo_name = self.repo_path.name
        self.cartography_dir = Path(cartography_dir)
        
        # Load all analysis results
        self.surveyor_results = self._load_json("surveyor_summary.json")
        self.hydrologist_results = self._load_json("hydrologist_summary.json")
        self.purpose_statements = self._load_json("purpose_statements.json")
        self.doc_drift = self._load_json("doc_drift.json")
        self.domain_clusters = self._load_json("domain_clusters.json")
        self.day_one_answers = self._load_json("day_one_answers.json")
        self.module_graph = self._load_json("module_graph.json")
        self.lineage_graph = self._load_json("lineage_graph.json")
        
        # Try to load nodes JSON if available
        self.nodes_data = self._load_json(f"{self.repo_name}_nodes.json")
        if not self.nodes_data:
            # Try alternative names
            self.nodes_data = self._load_json("ol-data-platform_nodes.json")
        
        logger.info(f"Archivist Agent initialized for {self.repo_path}")
        logger.info(f"Loaded data from {cartography_dir}")
    
    def _load_json(self, filename: str) -> Dict:
        """Load a JSON file from cartography directory."""
        file_path = self.cartography_dir / filename
        if file_path.exists():
            try:
                with open(file_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                logger.warning(f"Could not load {filename}: {e}")
        return {}
    
    def _load_file_content(self, filepath: str) -> str:
        """Load content of a file from the repository."""
        full_path = self.repo_path / filepath
        if full_path.exists():
            try:
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    return f.read()[:500]  # First 500 chars only
            except:
                pass
        return ""
    
    def generate_codebase_md(self) -> str:
        """
        Generate CODEBASE.md - living context for AI agents.
        
        This file is designed to be injected into AI coding agents
        to give them instant architectural awareness.
        """
        logger.info("Generating CODEBASE.md...")
        
        lines = []
        
        # Header
        lines.append(f"# {self.repo_name} Codebase Intelligence")
        lines.append(f"\n*Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
        lines.append(f"*Analysis covers {self._get_file_count()} files*")
        lines.append("\n---\n")
        
        # 1. ARCHITECTURE OVERVIEW
        lines.append("## 🏗️ Architecture Overview")
        lines.append("")
        
        # Get language breakdown
        lang_counts = self._get_language_breakdown()
        if lang_counts:
            lines.append("**Languages:**")
            for lang, count in sorted(lang_counts.items(), key=lambda x: x[1], reverse=True):
                lines.append(f"- {lang}: {count} files")
        
        # Get domain breakdown if available
        if self.domain_clusters:
            lines.append("\n**Business Domains:**")
            for domain, modules in self.domain_clusters.items():
                lines.append(f"- **{domain}**: {len(modules)} modules")
        
        lines.append("")
        
        # 2. CRITICAL PATH
        lines.append("## 🔥 Critical Path (High Impact Modules)")
        lines.append("")
        lines.append("These modules have the highest PageRank - changes here affect many other files:")
        lines.append("")
        
        high_impact = self._get_high_impact_modules(10)
        if high_impact:
            for i, module in enumerate(high_impact, 1):
                path = module.get('path', 'unknown')
                pagerank = module.get('pagerank', 0)
                purpose = self.purpose_statements.get(path, "No purpose statement")
                lines.append(f"### {i}. `{path}`")
                lines.append(f"**Impact score:** {pagerank:.4f}")
                lines.append(f"**Purpose:** {purpose}")
                
                # Add git velocity if available
                velocity = module.get('change_velocity_30d', 0)
                if velocity > 0:
                    lines.append(f"**Changes (30d):** {velocity}")
                lines.append("")
        else:
            lines.append("*No PageRank data available*")
            lines.append("")
        
        # 3. DATA FLOW
        lines.append("## 📊 Data Flow")
        lines.append("")
        
        # Sources and sinks
        sources = self._get_sources()
        sinks = self._get_sinks()
        
        if sources:
            lines.append(f"**📥 Source Datasets** (where data enters)")
            for source in sources[:5]:  # Top 5
                lines.append(f"- `{source}`")
            if len(sources) > 5:
                lines.append(f"  *...and {len(sources)-5} more*")
            lines.append("")
        
        if sinks:
            lines.append(f"**📤 Sink Datasets** (final outputs)")
            for sink in sinks[:5]:  # Top 5
                lines.append(f"- `{sink}`")
            if len(sinks) > 5:
                lines.append(f"  *...and {len(sinks)-5} more*")
            lines.append("")
        
        # 4. DOCUMENTATION HEALTH
        lines.append("## 📝 Documentation Health")
        lines.append("")
        
        # Count files with docstrings vs without
        total_modules = len([n for n in (self.nodes_data or {}).keys() if n.startswith('module:')])
        files_with_purpose = len(self.purpose_statements)
        
        lines.append(f"- **Files with purpose statements:** {files_with_purpose}/{total_modules}")
        
        # Documentation drift - FIXED: Define inaccurate variable first
        drift_count = len(self.doc_drift)
        inaccurate = 0  # FIXED: Initialize inaccurate variable
        
        if drift_count > 0:
            inaccurate = sum(1 for d in self.doc_drift.values() 
                           if isinstance(d, dict) and d.get('is_accurate') == 'no')
            lines.append(f"- **Files with docstring drift:** {inaccurate}/{drift_count}")
            
            # Show examples
            if inaccurate > 0:
                lines.append("\n**Examples of documentation drift:**")
                for path, drift in list(self.doc_drift.items())[:3]:
                    if isinstance(drift, dict) and drift.get('is_accurate') == 'no':
                        lines.append(f"- `{path}`")
        
        lines.append("")
        
        # 5. CHANGE VELOCITY
        lines.append("## ⚡ Change Velocity")
        lines.append("")
        lines.append("Files that change most frequently (likely pain points):")
        lines.append("")
        
        high_velocity = self._get_high_velocity_files(10)
        if high_velocity:
            for i, (path, changes) in enumerate(high_velocity, 1):
                lines.append(f"{i}. `{path}` ({changes} changes in 30d)")
        else:
            lines.append("*No git velocity data available*")
        
        lines.append("")
        
        # 6. KNOWN DEBT
        lines.append("## 🚧 Known Technical Debt")
        lines.append("")
        
        debt_items = []
        
        # Circular dependencies
        if self.surveyor_results:
            circular = self.surveyor_results.get('circular_dependencies', [])
            if circular:
                debt_items.append(f"- **Circular dependencies:** {len(circular)} cycles detected")
        
        # Dead code candidates
        dead_code = self._get_dead_code_candidates(5)
        if dead_code:
            debt_items.append(f"- **Dead code candidates:** {len(dead_code)} modules may be unused")
            for path in dead_code:
                debt_items.append(f"  - `{path}`")
        
        # Documentation drift - FIXED: Use the inaccurate variable we defined above
        if inaccurate > 0:
            debt_items.append(f"- **Documentation drift:** {inaccurate} files have outdated docs")
        
        if debt_items:
            lines.extend(debt_items)
        else:
            lines.append("*No major debt detected*")
        
        lines.append("")
        
        # 7. DAY-ONE ANSWERS (if available)
        if self.day_one_answers:
            lines.append("## 🎯 Day-One Answers")
            lines.append("")
            lines.append("Answers to the 5 critical questions every new engineer needs:")
            lines.append("")
            
            answers = self.day_one_answers
            
            # Question 1: Ingestion
            if "primary_ingestion_path" in answers:
                ans = answers["primary_ingestion_path"]
                lines.append("### 1️⃣ Primary Ingestion Path")
                lines.append(ans.get('answer', 'N/A'))
                evidence = ans.get('evidence', [])
                if evidence:
                    lines.append(f"   *Evidence: {', '.join(evidence[:2])}*")
                lines.append("")
            
            # Question 2: Critical datasets
            if "critical_datasets" in answers:
                lines.append("### 2️⃣ Critical Output Datasets")
                for dataset in answers["critical_datasets"][:3]:
                    name = dataset.get('name', 'Unknown')
                    why = dataset.get('why_critical', '')
                    lines.append(f"- **{name}**: {why}")
                lines.append("")
            
            # Question 3: Blast radius
            if "blast_radius_module" in answers:
                lines.append("### 3️⃣ Blast Radius Module")
                module = answers["blast_radius_module"]
                lines.append(f"**{module.get('module', 'Unknown')}**")
                lines.append(module.get('why', ''))
                lines.append("")
            
            # Question 4: Business logic
            if "business_logic_location" in answers:
                lines.append("### 4️⃣ Business Logic Location")
                loc = answers["business_logic_location"]
                lines.append(f"**{loc.get('location', 'Unknown')}**")
                lines.append(loc.get('pattern', ''))
                lines.append("")
            
            # Question 5: Change velocity
            if "change_velocity" in answers:
                lines.append("### 5️⃣ Change Velocity Pattern")
                vel = answers["change_velocity"]
                lines.append(vel.get('insight', ''))
                lines.append("")
        
        # 8. MODULE PURPOSE INDEX
        lines.append("## 📇 Module Purpose Index")
        lines.append("")
        lines.append("Quick reference of what each module does:")
        lines.append("")
        
        # Group by domain if available
        if self.domain_clusters:
            for domain, modules in self.domain_clusters.items():
                lines.append(f"### {domain}")
                for module_path in modules[:5]:  # Show first 5 per domain
                    purpose = self.purpose_statements.get(module_path, "No purpose statement")
                    lines.append(f"- `{module_path}`")
                    lines.append(f"  - {purpose[:100]}...")
                if len(modules) > 5:
                    lines.append(f"  *...and {len(modules)-5} more*")
                lines.append("")
        else:
            # Just list all modules with purposes
            for path, purpose in list(self.purpose_statements.items())[:20]:
                lines.append(f"- `{path}`")
                lines.append(f"  - {purpose[:100]}...")
        
        # Footer
        lines.append("\n---")
        lines.append(f"*This CODEBASE.md was automatically generated by Brownfield Cartographer*")
        lines.append(f"*Run `python src/agents/archivist.py` to update*")
        
        return "\n".join(lines)
    
    def generate_onboarding_brief(self) -> str:
        """
        Generate onboarding_brief.md - human-readable summary for new engineers.
        
        This is a shorter, more readable version focused on what a new
        engineer needs to know on Day 1.
        """
        logger.info("Generating onboarding_brief.md...")
        
        lines = []
        
        # Header
        lines.append(f"# Onboarding Brief: {self.repo_name}")
        lines.append(f"\n*Welcome! Here's what you need to know on Day 1.*")
        lines.append(f"*Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}*")
        lines.append("\n---\n")
        
        # Quick Stats
        lines.append("## 📊 Quick Stats")
        lines.append("")
        lines.append(f"- **Total Files:** {self._get_file_count()}")
        lines.append(f"- **Languages:** {', '.join(self._get_top_languages(3))}")
        lines.append(f"- **Data Sources:** {len(self._get_sources())}")
        lines.append(f"- **Data Sinks:** {len(self._get_sinks())}")
        lines.append("")
        
        # Day-One Answers (if available)
        if self.day_one_answers:
            lines.append("## 🎯 The 5 Things You MUST Know")
            lines.append("")
            
            answers = self.day_one_answers
            
            # Question 1
            if "primary_ingestion_path" in answers:
                ans = answers["primary_ingestion_path"]
                lines.append("### 1. How does data get in?")
                lines.append(f"   {ans.get('answer', 'N/A')}")
                lines.append("")
            
            # Question 2
            if "critical_datasets" in answers:
                lines.append("### 2. What are the most important outputs?")
                for dataset in answers["critical_datasets"][:3]:
                    name = dataset.get('name', 'Unknown')
                    why = dataset.get('why_critical', '')
                    lines.append(f"- **{name}**: {why}")
                lines.append("")
            
            # Question 3
            if "blast_radius_module" in answers:
                lines.append("### 3. What would cause the most damage?")
                module = answers["blast_radius_module"]
                lines.append(f"   **{module.get('module', 'Unknown')}**")
                lines.append(f"   {module.get('why', '')}")
                lines.append("")
            
            # Question 4
            if "business_logic_location" in answers:
                lines.append("### 4. Where is the business logic?")
                loc = answers["business_logic_location"]
                lines.append(f"   **{loc.get('location', 'Unknown')}**")
                lines.append(f"   {loc.get('pattern', '')}")
                lines.append("")
            
            # Question 5
            if "change_velocity" in answers:
                lines.append("### 5. What's changing most frequently?")
                vel = answers["change_velocity"]
                lines.append(f"   {vel.get('insight', '')}")
                lines.append("")
        
        # Critical Modules
        lines.append("## 🔥 Critical Modules")
        lines.append("")
        lines.append("These modules are the heart of the system. Learn them first:")
        lines.append("")
        
        high_impact = self._get_high_impact_modules(5)
        if high_impact:
            for i, module in enumerate(high_impact, 1):
                path = module.get('path', 'unknown')
                purpose = self.purpose_statements.get(path, "No purpose statement")
                lines.append(f"### {i}. `{path}`")
                lines.append(f"   {purpose}")
                lines.append("")
        
        # Data Lineage Summary
        lines.append("## 🔄 Data Flow Summary")
        lines.append("")
        
        sources = self._get_sources()[:5]
        sinks = self._get_sinks()[:5]
        
        if sources:
            lines.append("**Where data comes from:**")
            for source in sources:
                lines.append(f"- `{source}`")
            lines.append("")
        
        if sinks:
            lines.append("**Where data goes:**")
            for sink in sinks:
                lines.append(f"- `{sink}`")
            lines.append("")
        
        # Documentation Status
        lines.append("## 📝 Documentation Status")
        lines.append("")
        
        total = self._get_file_count()
        documented = len(self.purpose_statements)
        if total > 0:
            pct = (documented / total) * 100
            lines.append(f"- **Documented:** {documented}/{total} files ({pct:.1f}%)")
        
        if self.doc_drift:
            inaccurate = sum(1 for d in self.doc_drift.values() 
                           if isinstance(d, dict) and d.get('is_accurate') == 'no')
            if inaccurate > 0:
                lines.append(f"- **⚠️ Outdated docs:** {inaccurate} files")
        
        lines.append("")
        
        # First Tasks
        lines.append("## 🚀 Suggested First Tasks")
        lines.append("")
        lines.append("Based on the analysis, here's where to start:")
        lines.append("")
        
        # Suggest looking at high-velocity files
        high_velocity = self._get_high_velocity_files(3)
        if high_velocity:
            lines.append("1. **Understand the most active areas:**")
            for path, changes in high_velocity:
                lines.append(f"   - `{path}` ({changes} changes recently)")
        
        # Suggest looking at undocumented files
        if documented < total:
            lines.append("2. **Document undocumented modules:**")
            lines.append("   - Focus on critical path modules first")
        
        # Suggest investigating dead code
        dead_code = self._get_dead_code_candidates(3)
        if dead_code:
            lines.append("3. **Investigate potential dead code:**")
            for path in dead_code:
                lines.append(f"   - `{path}`")
        
        lines.append("\n---")
        lines.append(f"*This onboarding brief was generated by Brownfield Cartographer*")
        lines.append(f"*Run `python src/agents/archivist.py` to update*")
        
        return "\n".join(lines)
    
    def generate_trace(self) -> List[Dict]:
        """
        Generate cartography_trace.jsonl - audit trail of all analysis.
        
        This combines traces from all agents into one file.
        """
        logger.info("Generating cartography_trace.jsonl...")
        
        trace = []
        
        # Add surveyor trace if exists
        surveyor_trace = self.cartography_dir / "surveyor_trace.jsonl"
        if surveyor_trace.exists():
            with open(surveyor_trace, 'r') as f:
                for line in f:
                    try:
                        trace.append(json.loads(line))
                    except:
                        pass
        
        # Add hydrologist trace if exists
        hydrologist_trace = self.cartography_dir / "hydrologist_trace.jsonl"
        if hydrologist_trace.exists():
            with open(hydrologist_trace, 'r') as f:
                for line in f:
                    try:
                        trace.append(json.loads(line))
                    except:
                        pass
        
        # Add semanticist trace if exists
        semantic_trace = self.cartography_dir / "semantic_trace.jsonl"
        if semantic_trace.exists():
            with open(semantic_trace, 'r') as f:
                for line in f:
                    try:
                        trace.append(json.loads(line))
                    except:
                        pass
        
        # Add archivist entry
        trace.append({
            "action": "generate_documentation",
            "timestamp": datetime.now().isoformat(),
            "files_generated": ["CODEBASE.md", "onboarding_brief.md"],
            "success": True
        })
        
        return trace
    
    def save_all(self):
        """Save all generated files."""
        logger.info("Saving all generated files...")
        
        # Create output directory if needed
        self.cartography_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate and save CODEBASE.md
        codebase_md = self.generate_codebase_md()
        codebase_path = self.cartography_dir / "CODEBASE.md"
        with open(codebase_path, 'w', encoding='utf-8') as f:
            f.write(codebase_md)
        logger.info(f"✅ Saved {codebase_path}")
        
        # Generate and save onboarding_brief.md
        onboarding = self.generate_onboarding_brief()
        onboarding_path = self.cartography_dir / "onboarding_brief.md"
        with open(onboarding_path, 'w', encoding='utf-8') as f:
            f.write(onboarding)
        logger.info(f"✅ Saved {onboarding_path}")
        
        # Generate and save trace
        trace = self.generate_trace()
        trace_path = self.cartography_dir / "cartography_trace.jsonl"
        with open(trace_path, 'w', encoding='utf-8') as f:
            for entry in trace:
                f.write(json.dumps(entry) + '\n')
        logger.info(f"✅ Saved {trace_path} ({len(trace)} entries)")
        
        return {
            "codebase_md": str(codebase_path),
            "onboarding_brief": str(onboarding_path),
            "trace": str(trace_path)
        }
    
    # Helper methods
    def _get_file_count(self) -> int:
        """Get total number of files analyzed."""
        if self.surveyor_results:
            return self.surveyor_results.get('files_analyzed', 0)
        if self.nodes_data:
            return len([n for n in self.nodes_data.keys() if n.startswith('module:')])
        return 0
    
    def _get_language_breakdown(self) -> Dict[str, int]:
        """Get breakdown of files by language."""
        if self.surveyor_results:
            return self.surveyor_results.get('languages', {})
        
        # Count from nodes
        counts = {}
        if self.nodes_data:
            for node_id, node_data in self.nodes_data.items():
                if node_id.startswith('module:'):
                    lang = node_data.get('language', 'unknown')
                    counts[lang] = counts.get(lang, 0) + 1
        return counts
    
    def _get_top_languages(self, n: int = 3) -> List[str]:
        """Get top N languages."""
        counts = self._get_language_breakdown()
        sorted_langs = sorted(counts.items(), key=lambda x: x[1], reverse=True)
        return [lang for lang, _ in sorted_langs[:n]]
    
    def _get_high_impact_modules(self, n: int = 10) -> List[Dict]:
        """Get top N modules by PageRank."""
        modules = []
        
        if self.nodes_data:
            for node_id, node_data in self.nodes_data.items():
                if node_id.startswith('module:'):
                    pagerank = node_data.get('pagerank', 0)
                    if pagerank > 0:
                        modules.append({
                            'path': node_data.get('path', node_id.split(':',1)[1]),
                            'pagerank': pagerank,
                            'change_velocity_30d': node_data.get('change_velocity_30d', 0)
                        })
        
        # Sort by PageRank
        modules.sort(key=lambda x: x['pagerank'], reverse=True)
        return modules[:n]
    
    def _get_high_velocity_files(self, n: int = 10) -> List[Tuple[str, int]]:
        """Get top N files by change velocity."""
        files = []
        
        if self.nodes_data:
            for node_id, node_data in self.nodes_data.items():
                if node_id.startswith('module:'):
                    velocity = node_data.get('change_velocity_30d', 0)
                    if velocity > 0:
                        path = node_data.get('path', node_id.split(':',1)[1])
                        files.append((path, velocity))
        
        # Sort by velocity
        files.sort(key=lambda x: x[1], reverse=True)
        return files[:n]
    
    def _get_sources(self) -> List[str]:
        """Get source datasets (no inputs)."""
        if self.hydrologist_results:
            return self.hydrologist_results.get('sources', [])
        if self.lineage_graph:
            return self.lineage_graph.get('sources', [])
        return []
    
    def _get_sinks(self) -> List[str]:
        """Get sink datasets (no outputs)."""
        if self.hydrologist_results:
            return self.hydrologist_results.get('sinks', [])
        if self.lineage_graph:
            return self.lineage_graph.get('sinks', [])
        return []
    
    def _get_dead_code_candidates(self, n: int = 10) -> List[str]:
        """Get potential dead code candidates."""
        candidates = []
        
        if self.nodes_data:
            for node_id, node_data in self.nodes_data.items():
                if node_id.startswith('module:'):
                    if node_data.get('is_dead_code_candidate', False):
                        path = node_data.get('path', node_id.split(':',1)[1])
                        candidates.append(path)
        
        return candidates[:n]


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Archivist Agent - Generate documentation")
    parser.add_argument('--repo', type=str, required=True,
                       help='Path to repository')
    parser.add_argument('--cartography', type=str, default='.cartography',
                       help='Cartography directory (default: .cartography)')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    # Set logging level
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    # Check if repository exists
    if not os.path.exists(args.repo):
        logger.error(f"Repository path does not exist: {args.repo}")
        sys.exit(1)
    
    # Check if cartography directory exists
    if not os.path.exists(args.cartography):
        logger.error(f"Cartography directory does not exist: {args.cartography}")
        logger.error("Run Surveyor, Hydrologist, and Semanticist first!")
        sys.exit(1)
    
    # Run archivist
    try:
        archivist = ArchivistAgent(args.repo, args.cartography)
        files = archivist.save_all()
        
        print(f"\n✅ Archivist Agent complete!")
        print(f"📄 Generated files:")
        print(f"  - {files['codebase_md']}")
        print(f"  - {files['onboarding_brief']}")
        print(f"  - {files['trace']}")
        
        # Show preview of CODEBASE.md
        print(f"\n📋 CODEBASE.md preview (first 10 lines):")
        with open(files['codebase_md'], 'r', encoding='utf-8') as f:
            for i, line in enumerate(f):
                if i < 10:
                    print(f"  {line.rstrip()}")
                else:
                    break
        
    except Exception as e:
        logger.error(f"Documentation generation failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()