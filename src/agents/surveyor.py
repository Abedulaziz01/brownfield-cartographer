#!/usr/bin/env python3
"""Surveyor Agent - Static structure analysis of codebases.

This agent builds the module dependency graph, computes PageRank,
detects circular dependencies, and identifies dead code candidates.
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
import logging
import networkx as nx  # Make sure this is imported!

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.graph.knowledge_graph import KnowledgeGraph
from src.analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from src.analyzers.git_analyzer import GitAnalyzer
from src.analyzers.language_router import LanguageRouter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SurveyorAgent:
    """
    Surveyor Agent - Maps the static structure of a codebase.
    
    Responsibilities:
    - Run AST analysis on all files
    - Build module import graph
    - Compute PageRank to find important modules
    - Detect circular dependencies
    - Identify dead code candidates
    """
    
    def __init__(self, repo_path: str):
        """
        Initialize the Surveyor Agent.
        
        Args:
            repo_path: Path to the repository to analyze
        """
        self.repo_path = Path(repo_path).resolve()
        self.repo_name = self.repo_path.name
        
        # Initialize analyzers
        self.tree_sitter = TreeSitterAnalyzer()
        self.git_analyzer = GitAnalyzer(str(self.repo_path))
        self.language_router = LanguageRouter()
        
        # Initialize knowledge graph
        self.kg = KnowledgeGraph(self.repo_name)
        
        # Store analysis results
        self.file_analyses = {}
        self.import_graph = {}  # module -> {resolved: [], unresolved: []}
        
        # PageRank storage
        self.pagerank_scores = {}
        self.pagerank_stats = {
            "computed": False,
            "total_nodes": 0,
            "max_score": 0,
            "min_score": 0,
            "mean_score": 0,
            "median_score": 0,
            "std_dev": 0,
            "top_nodes": []
        }
        
        logger.info(f"Surveyor Agent initialized for {self.repo_path}")
    
    def analyze(self) -> KnowledgeGraph:
        """
        Run full analysis on the repository.
        
        Returns:
            Populated KnowledgeGraph
        """
        logger.info(f"Starting analysis of {self.repo_path}")
        
        # Step 1: Find all files and analyze them
        self._analyze_files()
        
        # Step 2: Build import graph
        self._build_import_graph()
        
        # Step 3: Add git velocity data
        self._add_git_metadata()
        
        # Step 4: Compute graph metrics with enhanced PageRank
        self._compute_metrics_enhanced()
        
        # Step 5: Detect dead code candidates
        self._detect_dead_code()
        
        # Step 6: Detect circular dependencies
        self._detect_circular_deps()
        
        # Step 7: Log PageRank summary
        self._log_pagerank_summary()
        
        logger.info(f"Analysis complete. Graph has {self.kg.graph.number_of_nodes()} nodes")
        return self.kg
    
    def _analyze_files(self):
        """Analyze all files in the repository with better logging."""
        logger.info("Analyzing files...")
        
        # Track language statistics
        lang_stats = {}
        
        # Walk through repository
        for root, dirs, files in os.walk(self.repo_path):
            # Skip common ignored directories
            dirs[:] = [d for d in dirs if d not in 
                      ['.git', '__pycache__', 'node_modules', 'venv', 'env', '.venv', '.env', 'build', 'dist']]
            
            for file in files:
                file_path = os.path.join(root, file)
                rel_path = os.path.relpath(file_path, self.repo_path)
                
                # Get language
                lang = self.language_router.get_language(file_path)
                if not lang:
                    continue
                
                # Count languages
                lang_stats[lang] = lang_stats.get(lang, 0) + 1
                
                # Analyze file
                try:
                    analysis = self.tree_sitter.analyze_file(file_path, str(self.repo_path))
                    self.file_analyses[rel_path] = analysis
                    
                    # Log if file has imports
                    if analysis.get('imports'):
                        logger.debug(f"{rel_path} has {len(analysis['imports'])} imports: {analysis['imports']}")
                    elif analysis.get('raw_imports'):
                        logger.debug(f"{rel_path} has raw imports: {analysis['raw_imports']}")
                    
                    # Add to knowledge graph
                    self.kg.add_module_node(
                        path=rel_path,
                        language=lang,
                        complexity_score=self._calculate_complexity(analysis),
                        purpose_statement="",
                        domain_cluster="",
                        change_velocity_30d=0,
                        is_dead_code_candidate=False,
                        last_modified=datetime.now().isoformat(),
                        functions=analysis.get('functions', []),
                        classes=analysis.get('classes', []),
                        import_count=len(analysis.get('imports', []))
                    )
                    
                except Exception as e:
                    logger.warning(f"Error analyzing {rel_path}: {e}")
        
        logger.info(f"Analyzed {len(self.file_analyses)} files")
        logger.info(f"Language breakdown: {lang_stats}")
        
        # Count total imports found
        total_imports = sum(len(a.get('imports', [])) for a in self.file_analyses.values())
        logger.info(f"Total imports found across all files: {total_imports}")
    
    def _calculate_complexity(self, analysis: Dict) -> float:
        """
        Calculate a complexity score for a file.
        
        Simple heuristic: functions * 2 + classes * 3 + imports * 1
        """
        num_functions = len(analysis.get('functions', []))
        num_classes = len(analysis.get('classes', []))
        num_imports = len(analysis.get('imports', []))
        
        return num_functions * 2.0 + num_classes * 3.0 + num_imports * 1.0
    
    def _build_import_graph(self):
        """Build the module import graph with better resolution."""
        logger.info("Building import graph...")
        
        import_count = 0
        unresolved_imports = []
        
        # First pass: collect all imports
        for file_path, analysis in self.file_analyses.items():
            imports = analysis.get('imports', [])
            raw_imports = analysis.get('raw_imports', [])
            
            self.import_graph[file_path] = {
                'resolved': [],
                'unresolved': []
            }
            
            # Log what we found
            if imports:
                logger.debug(f"{file_path} imports: {imports}")
                import_count += len(imports)
            
            # Try to resolve each import
            for imp in imports:
                resolved = self._resolve_import_enhanced(file_path, imp)
                
                if resolved:
                    self.import_graph[file_path]['resolved'].append(resolved)
                    # Add edge to knowledge graph
                    self.kg.add_import_edge(file_path, resolved, weight=1)
                    logger.debug(f"  Resolved: {imp} -> {resolved}")
                else:
                    self.import_graph[file_path]['unresolved'].append(imp)
                    unresolved_imports.append(f"{file_path} -> {imp}")
            
            # Also try to resolve from raw imports for debugging
            if not imports and raw_imports:
                logger.debug(f"{file_path} has raw imports but no resolved: {raw_imports}")
        
        logger.info(f"Built import graph: {import_count} total imports")
        logger.info(f"  Resolved: {import_count - len(unresolved_imports)} imports")
        logger.info(f"  Unresolved: {len(unresolved_imports)} imports")
        
        # Log some unresolved imports for debugging (first 10)
        if unresolved_imports:
            logger.warning("Sample unresolved imports (first 10):")
            for imp in unresolved_imports[:10]:
                logger.warning(f"  {imp}")
    
    def _resolve_import_enhanced(self, source_file: str, import_name: str) -> Optional[str]:
        """
        Enhanced import resolution with multiple strategies.
        
        Args:
            source_file: Path to source file relative to repo root
            import_name: Import string (e.g., 'os', 'utils.helpers', '..models')
            
        Returns:
            Resolved file path or None
        """
        # Handle relative imports (Python)
        if import_name.startswith('relative:') or import_name.startswith('.'):
            return self._resolve_relative_import(source_file, import_name)
        
        # Handle standard library imports (skip - they're not in repo)
        if self._is_stdlib_import(import_name):
            return None
        
        # Strategy 1: Direct match with .py extension
        candidate = import_name
        if not candidate.endswith('.py'):
            candidate = candidate + '.py'
        
        if candidate in self.file_analyses:
            logger.debug(f"Strategy 1 matched: {candidate}")
            return candidate
        
        # Strategy 2: Match as directory module (__init__.py)
        candidate = os.path.join(import_name, '__init__.py')
        if candidate in self.file_analyses:
            logger.debug(f"Strategy 2 matched: {candidate}")
            return candidate
        
        # Strategy 3: Match with dots converted to path separators
        candidate = import_name.replace('.', '/') + '.py'
        if candidate in self.file_analyses:
            logger.debug(f"Strategy 3 matched: {candidate}")
            return candidate
        
        # Strategy 4: Match as package (directory with __init__)
        candidate = import_name.replace('.', '/')
        init_candidate = os.path.join(candidate, '__init__.py')
        if init_candidate in self.file_analyses:
            logger.debug(f"Strategy 4 matched: {init_candidate}")
            return init_candidate
        
        # Strategy 5: Look in same directory as source file
        source_dir = os.path.dirname(source_file)
        candidate = os.path.join(source_dir, import_name)
        if not candidate.endswith('.py'):
            candidate = candidate + '.py'
        
        if candidate in self.file_analyses:
            logger.debug(f"Strategy 5 matched: {candidate}")
            return candidate
        
        # Strategy 6: Look in common src/ directory
        if not source_dir.startswith('src/'):
            candidate = os.path.join('src', import_name)
            if not candidate.endswith('.py'):
                candidate = candidate + '.py'
            
            if candidate in self.file_analyses:
                logger.debug(f"Strategy 6 matched: {candidate}")
                return candidate
        
        # Strategy 7: Fuzzy match - look for any file ending with the import name
        for file_path in self.file_analyses.keys():
            if file_path.endswith(import_name + '.py') or file_path.endswith('/' + import_name + '.py'):
                logger.debug(f"Strategy 7 matched (fuzzy): {file_path}")
                return file_path
        
        return None
    
    def _resolve_relative_import(self, source_file: str, import_name: str) -> Optional[str]:
        """Resolve relative Python imports (e.g., from .utils import x)."""
        # Strip 'relative:' prefix if present
        if import_name.startswith('relative:'):
            import_name = import_name[9:]
        
        # Count dots to determine level
        level = 0
        while import_name.startswith('.'):
            level += 1
            import_name = import_name[1:]
        
        # Get source directory
        source_dir_parts = Path(source_file).parent.parts
        
        # Go up 'level' directories
        if level > 0:
            if level <= len(source_dir_parts):
                base_dir = os.path.join(*source_dir_parts[:-level])
            else:
                return None
        else:
            base_dir = os.path.join(*source_dir_parts)
        
        # Convert remaining import to path
        if import_name:
            module_path = import_name.replace('.', '/')
            # Try as module
            candidate = os.path.join(base_dir, module_path + '.py')
            if candidate in self.file_analyses:
                return candidate
            
            # Try as package
            candidate = os.path.join(base_dir, module_path, '__init__.py')
            if candidate in self.file_analyses:
                return candidate
        else:
            # Just dots (from . import x) - look for __init__.py in current/source dir
            candidate = os.path.join(base_dir, '__init__.py')
            if candidate in self.file_analyses:
                return candidate
        
        return None
    
    def _is_stdlib_import(self, import_name: str) -> bool:
        """Check if import is from Python standard library."""
        stdlib_modules = {
            'os', 'sys', 're', 'json', 'math', 'datetime', 'time', 'random',
            'collections', 'itertools', 'functools', 'pathlib', 'logging',
            'argparse', 'subprocess', 'threading', 'multiprocessing', 'socket',
            'http', 'urllib', 'xml', 'csv', 'sqlite3', 'hashlib', 'hmac',
            'uuid', 'base64', 'tempfile', 'shutil', 'glob', 'fnmatch',
            'copy', 'pprint', 'traceback', 'warnings', 'contextlib',
            'abc', 'enum', 'typing', 'dataclasses'
        }
        
        # Get base module name (first part before dot)
        base = import_name.split('.')[0]
        return base in stdlib_modules
    
    def _add_git_metadata(self):
        """Add git velocity data to module nodes."""
        if not self.git_analyzer.is_git_repo:
            logger.warning("Not a git repository, skipping git metadata")
            return
        
        logger.info("Adding git metadata...")
        
        # Get change frequency for all files
        change_freq = self.git_analyzer.get_file_change_frequency(days=30)
        
        # Update module nodes with change velocity
        for file_path, changes in change_freq.items():
            node_id = f"module:{file_path}"
            if self.kg.graph.has_node(node_id):
                self.kg.graph.nodes[node_id]['change_velocity_30d'] = changes
                
                # Get detailed info
                velocity = self.git_analyzer.get_file_change_velocity(file_path)
                self.kg.graph.nodes[node_id]['git_velocity'] = velocity
        
        logger.info(f"Added git metadata for {len(change_freq)} files")
    
    def _compute_metrics_enhanced(self):
        """
        Enhanced computation of graph metrics with better PageRank capture.
        """
        logger.info("Computing enhanced graph metrics...")
        
        if self.kg.graph.number_of_nodes() == 0:
            logger.warning("Empty graph, skipping metrics")
            return
        
        # Count edges per node (degree)
        for node in self.kg.graph.nodes:
            if node.startswith('module:'):
                in_degree = self.kg.graph.in_degree(node)
                out_degree = self.kg.graph.out_degree(node)
                self.kg.graph.nodes[node]['in_degree'] = in_degree
                self.kg.graph.nodes[node]['out_degree'] = out_degree
        
        # Compute PageRank with multiple attempts
        self._compute_pagerank_with_fallback()
        
        # Log PageRank statistics
        self._log_pagerank_stats()
    
    def _compute_pagerank_with_fallback(self):
        """
        Compute PageRank with fallback strategies for disconnected graphs.
        """
        # Only consider module nodes for PageRank
        module_nodes = [n for n in self.kg.graph.nodes if n.startswith('module:')]
        
        if len(module_nodes) < 2:
            logger.warning(f"Not enough module nodes for PageRank (found {len(module_nodes)})")
            return
        
        try:
            # Create subgraph of just modules
            subgraph = self.kg.graph.subgraph(module_nodes)
            
            # Check if graph has edges
            edge_count = subgraph.number_of_edges()
            logger.info(f"Module subgraph has {edge_count} edges")
            
            if edge_count == 0:
                logger.warning("No edges in module graph - PageRank will be uniform")
                # Assign uniform PageRank
                uniform_score = 1.0 / len(module_nodes)
                for node in module_nodes:
                    self.pagerank_scores[node] = uniform_score
                    self.kg.graph.nodes[node]['pagerank'] = uniform_score
                self.pagerank_stats["computed"] = True
                self.pagerank_stats["method"] = "uniform (no edges)"
                return
            
            # Try standard PageRank
            logger.info("Computing standard PageRank...")
            self.pagerank_scores = nx.pagerank(subgraph, weight='weight', alpha=0.85)
            
            # Normalize scores for better visualization
            max_score = max(self.pagerank_scores.values())
            if max_score > 0:
                for node in self.pagerank_scores:
                    self.pagerank_scores[node] = self.pagerank_scores[node] / max_score
            
            # Update knowledge graph
            for node, score in self.pagerank_scores.items():
                self.kg.graph.nodes[node]['pagerank'] = score
            
            self.pagerank_stats["computed"] = True
            self.pagerank_stats["method"] = "standard"
            self.pagerank_stats["edge_count"] = edge_count
            
            logger.info(f"PageRank computed successfully for {len(self.pagerank_scores)} nodes")
            
        except nx.PowerIterationFailedConvergence:
            logger.warning("PageRank power iteration failed to converge, trying with different alpha")
            try:
                # Try with higher alpha (more damping)
                self.pagerank_scores = nx.pagerank(subgraph, weight='weight', alpha=0.9, max_iter=200)
                
                # Normalize
                max_score = max(self.pagerank_scores.values())
                if max_score > 0:
                    for node in self.pagerank_scores:
                        self.pagerank_scores[node] = self.pagerank_scores[node] / max_score
                
                for node, score in self.pagerank_scores.items():
                    self.kg.graph.nodes[node]['pagerank'] = score
                
                self.pagerank_stats["computed"] = True
                self.pagerank_stats["method"] = "standard (alpha=0.9)"
                logger.info("PageRank computed with alternative parameters")
                
            except Exception as e:
                logger.warning(f"Alternative PageRank also failed: {e}")
                self._compute_pagerank_alternative(subgraph, module_nodes)
        
        except Exception as e:
            logger.warning(f"Error computing PageRank: {e}")
            self._compute_pagerank_alternative(subgraph, module_nodes)
    
    def _compute_pagerank_alternative(self, subgraph, module_nodes):
        """
        Alternative PageRank computation using degree centrality.
        """
        logger.info("Computing alternative importance metrics...")
        
        # Use degree centrality as fallback
        try:
            # Calculate degree centrality
            centrality = nx.degree_centrality(subgraph)
            
            # Normalize
            max_cent = max(centrality.values()) if centrality else 1
            if max_cent > 0:
                for node in module_nodes:
                    score = centrality.get(node, 0) / max_cent
                    self.pagerank_scores[node] = score
                    self.kg.graph.nodes[node]['pagerank'] = score
                    self.kg.graph.nodes[node]['pagerank_method'] = 'degree_centrality'
            
            self.pagerank_stats["computed"] = True
            self.pagerank_stats["method"] = "degree_centrality (fallback)"
            logger.info("Used degree centrality as fallback for PageRank")
            
        except Exception as e:
            logger.warning(f"Even degree centrality failed: {e}")
            # Ultimate fallback: uniform distribution
            uniform_score = 1.0 / len(module_nodes) if module_nodes else 0
            for node in module_nodes:
                self.pagerank_scores[node] = uniform_score
                self.kg.graph.nodes[node]['pagerank'] = uniform_score
                self.kg.graph.nodes[node]['pagerank_method'] = 'uniform_fallback'
            
            self.pagerank_stats["computed"] = True
            self.pagerank_stats["method"] = "uniform (ultimate fallback)"
    
    def _log_pagerank_stats(self):
        """Log detailed PageRank statistics."""
        if not self.pagerank_scores:
            logger.warning("No PageRank scores to log")
            return
        
        scores = list(self.pagerank_scores.values())
        
        if not scores:
            return
        
        # Calculate statistics
        import numpy as np
        self.pagerank_stats["total_nodes"] = len(scores)
        self.pagerank_stats["max_score"] = max(scores)
        self.pagerank_stats["min_score"] = min(scores)
        self.pagerank_stats["mean_score"] = np.mean(scores)
        self.pagerank_stats["median_score"] = np.median(scores)
        self.pagerank_stats["std_dev"] = np.std(scores)
        
        # Get top nodes
        top_nodes = sorted(self.pagerank_scores.items(), key=lambda x: x[1], reverse=True)[:10]
        self.pagerank_stats["top_nodes"] = [
            {
                "node": node,
                "path": self.kg.graph.nodes[node].get('path', 'unknown'),
                "score": score
            }
            for node, score in top_nodes
        ]
        
        # Log summary
        logger.info("=" * 60)
        logger.info("📊 PAGERANK SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Method: {self.pagerank_stats.get('method', 'unknown')}")
        logger.info(f"Nodes scored: {self.pagerank_stats['total_nodes']}")
        logger.info(f"Score range: {self.pagerank_stats['min_score']:.6f} - {self.pagerank_stats['max_score']:.6f}")
        logger.info(f"Mean score: {self.pagerank_stats['mean_score']:.6f}")
        logger.info(f"Median score: {self.pagerank_stats['median_score']:.6f}")
        logger.info(f"Std deviation: {self.pagerank_stats['std_dev']:.6f}")
        
        logger.info("\n🔥 TOP 10 HIGH-IMPACT MODULES:")
        for i, node_info in enumerate(self.pagerank_stats["top_nodes"], 1):
            logger.info(f"  {i:2d}. {node_info['path']} (score: {node_info['score']:.6f})")
        
        logger.info("=" * 60)
    
    def _log_pagerank_summary(self):
        """Log a summary of PageRank results to console."""
        if not self.pagerank_scores:
            print("\n⚠️  No PageRank scores computed")
            return
        
        print("\n" + "=" * 70)
        print("📊 PAGERANK ANALYSIS RESULTS")
        print("=" * 70)
        print(f"Method: {self.pagerank_stats.get('method', 'unknown')}")
        print(f"Nodes scored: {self.pagerank_stats['total_nodes']}")
        print(f"Score range: {self.pagerank_stats['min_score']:.6f} - {self.pagerank_stats['max_score']:.6f}")
        
        print("\n🔥 TOP 10 HIGHEST IMPACT MODULES:")
        print("-" * 70)
        for i, node_info in enumerate(self.pagerank_stats.get("top_nodes", [])[:10], 1):
            # Truncate long paths for display
            path = node_info['path']
            if len(path) > 60:
                path = "..." + path[-57:]
            print(f"{i:2d}. Score: {node_info['score']:.6f} | {path}")
        
        print("=" * 70)
    
    def _detect_dead_code(self):
        """Identify potential dead code candidates."""
        logger.info("Detecting dead code candidates...")
        
        for node in self.kg.graph.nodes:
            if not node.startswith('module:'):
                continue
            
            data = self.kg.graph.nodes[node]
            
            # Heuristic: module with no incoming imports might be dead
            # But also check if it's an entry point (main, __init__, etc.)
            in_degree = data.get('in_degree', 0)
            out_degree = data.get('out_degree', 0)
            pagerank = data.get('pagerank', 0)
            
            # Entry points often have no imports but are still important
            is_entry_point = False
            path = data.get('path', '')
            if path.endswith('__init__.py') or path.endswith('__main__.py'):
                is_entry_point = True
            if path.endswith('main.py') or 'cli' in path:
                is_entry_point = True
            
            # Also check if it's a config file (often dead code candidates are not configs)
            is_config = path.endswith(('.yaml', '.yml', '.json', '.toml', '.ini'))
            
            # Dead code candidate: very low PageRank, no incoming imports, not entry point
            if (pagerank < 0.001 or in_degree == 0) and out_degree > 0 and not is_entry_point and not is_config:
                self.kg.graph.nodes[node]['is_dead_code_candidate'] = True
                logger.debug(f"Dead code candidate: {path} (pagerank: {pagerank:.6f})")
    
    def _detect_circular_deps(self):
        """Detect circular dependencies in the import graph."""
        logger.info("Detecting circular dependencies...")
        
        try:
            # Find strongly connected components (SCCs) with more than 1 node
            module_nodes = [n for n in self.kg.graph.nodes if n.startswith('module:')]
            if len(module_nodes) < 2:
                return
            
            subgraph = self.kg.graph.subgraph(module_nodes)
            sccs = list(nx.strongly_connected_components(subgraph))
            
            # Filter SCCs with size > 1 (these are circular dependencies)
            circular_deps = [list(scc) for scc in sccs if len(scc) > 1]
            
            # Store in graph metadata
            self.kg.graph.graph['circular_dependencies'] = circular_deps
            
            if circular_deps:
                logger.info(f"Found {len(circular_deps)} circular dependencies")
                for i, cycle in enumerate(circular_deps[:3]):  # Show first 3
                    paths = [self.kg.graph.nodes[n].get('path', n) for n in cycle]
                    logger.info(f"  Cycle {i+1}: {' -> '.join(paths)}")
            else:
                logger.info("No circular dependencies found")
        except Exception as e:
            logger.warning(f"Error detecting circular dependencies: {e}")
    
    def get_high_impact_modules(self, top_n: int = 10) -> List[Dict]:
        """
        Get the highest impact modules based on PageRank.
        
        Args:
            top_n: Number of modules to return
            
        Returns:
            List of module info dictionaries
        """
        modules = []
        for node in self.kg.graph.nodes:
            if node.startswith('module:'):
                data = self.kg.graph.nodes[node]
                modules.append({
                    'path': data.get('path'),
                    'pagerank': data.get('pagerank', 0),
                    'in_degree': data.get('in_degree', 0),
                    'out_degree': data.get('out_degree', 0),
                    'change_velocity': data.get('change_velocity_30d', 0)
                })
        
        # Sort by PageRank (descending)
        modules.sort(key=lambda x: x['pagerank'], reverse=True)
        return modules[:top_n]
    
    def save_results(self, output_dir: str = ".cartography"):
        """
        Save surveyor results to files.
        
        Args:
            output_dir: Directory to save results
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Save knowledge graph
        self.kg.serialize_graph(output_path)
        
        # Save module graph specifically (for easy access)
        module_graph = {
            "nodes": [],
            "edges": []
        }
        
        # Add module nodes
        for node in self.kg.graph.nodes:
            if node.startswith('module:'):
                data = self.kg.graph.nodes[node]
                module_graph["nodes"].append({
                    "id": node,
                    "path": data.get("path"),
                    "language": data.get("language"),
                    "complexity": data.get("complexity_score"),
                    "change_velocity": data.get("change_velocity_30d"),
                    "pagerank": data.get("pagerank", 0),
                    "in_degree": data.get("in_degree", 0),
                    "out_degree": data.get("out_degree", 0),
                    "is_dead_code": data.get("is_dead_code_candidate", False)
                })
        
        # Add import edges
        for u, v, data in self.kg.graph.edges(data=True):
            if data.get("edge_type") == "IMPORTS":
                module_graph["edges"].append({
                    "source": u,
                    "target": v,
                    "weight": data.get("weight", 1)
                })
        
        # Save to file
        module_graph_path = output_path / "module_graph.json"
        with open(module_graph_path, 'w') as f:
            json.dump(module_graph, f, indent=2)
        
        logger.info(f"Saved module graph to {module_graph_path}")
        
        # Save PageRank results separately
        pagerank_path = output_path / "pagerank_results.json"
        with open(pagerank_path, 'w') as f:
            json.dump(self.pagerank_stats, f, indent=2)
        
        logger.info(f"Saved PageRank results to {pagerank_path}")
        
        # Save surveyor summary
        summary = {
            "repo_path": str(self.repo_path),
            "repo_name": self.repo_name,
            "analyzed_at": datetime.now().isoformat(),
            "files_analyzed": len(self.file_analyses),
            "graph_stats": self.kg.summary(),
            "pagerank_stats": self.pagerank_stats,
            "high_impact_modules": self.get_high_impact_modules(5),
            "circular_dependencies": self.kg.graph.graph.get('circular_dependencies', [])
        }
        
        summary_path = output_path / "surveyor_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Saved surveyor summary to {summary_path}")


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Surveyor Agent - Static codebase analysis")
    parser.add_argument('--repo', type=str, required=True,
                       help='Path to repository to analyze')
    parser.add_argument('--output', type=str, default='.cartography',
                       help='Output directory (default: .cartography)')
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
    
    # Run surveyor
    try:
        surveyor = SurveyorAgent(args.repo)
        surveyor.analyze()
        surveyor.save_results(args.output)
        
        print(f"\n✅ Surveyor analysis complete!")
        print(f"📊 Analyzed {len(surveyor.file_analyses)} files")
        print(f"📁 Results saved to {args.output}/")
        
        # Show PageRank summary again at the end
        surveyor._log_pagerank_summary()
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    # Need numpy for statistics
    try:
        import numpy as np
    except ImportError:
        print("⚠️  numpy not installed. Run: pip install numpy")
        sys.exit(1)
    main()