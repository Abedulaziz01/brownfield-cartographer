#!/usr/bin/env python3
"""Navigator Agent - Enhanced query interface with semantic search and multi-step chaining.

This agent provides tools to query the knowledge graph:
- find_implementation: Semantic vector search for concepts
- trace_lineage: Trace data lineage with line numbers
- blast_radius: Find what breaks with evidence citations
- explain_module: Get explanations with analysis method tags
- Multi-step tool chaining for complex queries
"""

import os
import sys
import json
import argparse
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Optional, Any, Tuple, Union
import logging
import hashlib

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from graph.knowledge_graph import KnowledgeGraph
from llm.client import LLMClient
from llm.prompts import get_system_prompt, format_prompt
from llm.model_router import ModelRouter

# Try to import sentence-transformers for semantic search
try:
    from sentence_transformers import SentenceTransformer
    SEMANTIC_AVAILABLE = True
except ImportError:
    SEMANTIC_AVAILABLE = False
    print("⚠️  sentence-transformers not installed. Install with: pip install sentence-transformers")

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SemanticSearch:
    """Semantic vector search over code and purposes."""
    
    def __init__(self):
        self.encoder = None
        self.embeddings = {}
        self.texts = []
        self.paths = []
        self.index_built = False
        
        if SEMANTIC_AVAILABLE:
            try:
                # Use a small, fast model for embeddings
                self.encoder = SentenceTransformer('all-MiniLM-L6-v2')
                logger.info("✅ Semantic search initialized with all-MiniLM-L6-v2")
            except Exception as e:
                logger.warning(f"Failed to load sentence transformer: {e}")
    
    def build_index(self, modules: Dict[str, Dict], purposes: Dict[str, str]):
        """Build semantic index from modules and purposes."""
        if not self.encoder:
            logger.warning("Semantic search not available - encoder not loaded")
            return False
        
        self.texts = []
        self.paths = []
        
        for path, module in modules.items():
            # Combine multiple sources for rich text representation
            text_parts = []
            
            # Add purpose statement (most important)
            purpose = purposes.get(path, "")
            if purpose:
                text_parts.append(f"Purpose: {purpose}")
            
            # Add module path (contains keywords)
            text_parts.append(f"Path: {path}")
            
            # Add functions if available
            functions = module.get('functions', [])
            if functions:
                func_names = [f.get('name', '') for f in functions if isinstance(f, dict)]
                if func_names:
                    text_parts.append(f"Functions: {', '.join(func_names[:5])}")
            
            # Add classes if available
            classes = module.get('classes', [])
            if classes:
                text_parts.append(f"Classes: {', '.join(classes[:3])}")
            
            # Combine all text
            full_text = "\n".join(text_parts)
            self.texts.append(full_text)
            self.paths.append(path)
        
        if not self.texts:
            logger.warning("No texts to index")
            return False
        
        # Generate embeddings
        try:
            logger.info(f"Generating embeddings for {len(self.texts)} texts...")
            embeddings = self.encoder.encode(self.texts, show_progress_bar=False)
            
            # Store embeddings
            for i, path in enumerate(self.paths):
                self.embeddings[path] = embeddings[i]
            
            self.index_built = True
            logger.info(f"✅ Built semantic index with {len(self.embeddings)} embeddings")
            return True
            
        except Exception as e:
            logger.warning(f"Failed to generate embeddings: {e}")
            return False
    
    def search(self, query: str, top_k: int = 10) -> List[Dict]:
        """Search for modules semantically similar to query."""
        if not self.encoder or not self.index_built:
            return self._keyword_fallback(query, top_k)
        
        try:
            # Encode query
            query_embedding = self.encoder.encode([query])[0]
            
            # Calculate similarities
            similarities = []
            for path, emb in self.embeddings.items():
                # Cosine similarity
                sim = np.dot(query_embedding, emb) / (np.linalg.norm(query_embedding) * np.linalg.norm(emb))
                similarities.append((path, float(sim)))
            
            # Sort by similarity
            similarities.sort(key=lambda x: x[1], reverse=True)
            
            # Format results
            results = []
            for path, score in similarities[:top_k]:
                if score > 0.3:  # Only return if decent similarity
                    results.append({
                        "path": path,
                        "similarity": round(score, 3),
                        "match_type": "semantic"
                    })
            
            return results
            
        except Exception as e:
            logger.warning(f"Semantic search failed: {e}, using keyword fallback")
            return self._keyword_fallback(query, top_k)
    
    def _keyword_fallback(self, query: str, top_k: int = 10) -> List[Dict]:
        """Fallback keyword search when semantic not available."""
        results = []
        query_lower = query.lower()
        
        for path in self.paths:
            score = 0
            # Check if query words appear in path
            for word in query_lower.split():
                if word in path.lower():
                    score += 0.1
            
            if score > 0:
                results.append({
                    "path": path,
                    "similarity": min(score, 1.0),
                    "match_type": "keyword"
                })
        
        results.sort(key=lambda x: x['similarity'], reverse=True)
        return results[:top_k]


class ResponseBuilder:
    """Builds responses with evidence citations and method tags."""
    
    @staticmethod
    def add_evidence(response: Dict, 
                     file_path: str, 
                     line_range: str, 
                     method: str,
                     confidence: float) -> Dict:
        """Add evidence citation to response."""
        if 'evidence' not in response:
            response['evidence'] = []
        
        response['evidence'].append({
            "file": file_path,
            "line_range": line_range,
            "analysis_method": method,  # "static", "llm", "semantic"
            "confidence": confidence,
            "timestamp": datetime.now().isoformat()
        })
        
        return response
    
    @staticmethod
    def format_response(data: Any, method: str) -> Dict:
        """Format any response with method tag."""
        return {
            "data": data,
            "metadata": {
                "analysis_method": method,
                "timestamp": datetime.now().isoformat(),
                "version": "2.0"
            }
        }


class NavigatorAgent:
    """
    Enhanced Navigator Agent with semantic search and multi-step chaining.
    
    Features:
    - Semantic vector search for find_implementation
    - Evidence citations with file paths and line numbers
    - Analysis method tags (static vs LLM)
    - Multi-step tool chaining
    - Conversation memory
    """
    
    def __init__(self, cartography_dir: str = ".cartography", repo_path: Optional[str] = None):
        """
        Initialize the Navigator Agent.
        
        Args:
            cartography_dir: Directory containing analysis results
            repo_path: Optional path to repository (for reading files)
        """
        self.cartography_dir = Path(cartography_dir)
        self.repo_path = Path(repo_path) if repo_path else None
        
        # Load all analysis results
        self._load_data()
        
        # Initialize semantic search
        self.semantic_search = SemanticSearch()
        if len(self.modules) > 0:
            self.semantic_search.build_index(self.modules, self.purposes)
        
        # Initialize LLM for explanations
        self.model_router = ModelRouter()
        
        # Conversation memory for multi-step chaining
        self.conversation_history = []
        self.last_results = {}
        
        # Tool registry for chaining
        self.tools = {
            "find_implementation": self.find_implementation,
            "trace_lineage": self.trace_lineage,
            "blast_radius": self.blast_radius,
            "explain_module": self.explain_module
        }
        
        logger.info(f"✅ Navigator Agent initialized with {len(self.modules)} modules")
        logger.info(f"   Semantic search: {'✅' if self.semantic_search.index_built else '❌'}")
    
    def _load_data(self):
        """Load all analysis data from cartography directory."""
        self.modules = {}  # path -> node data
        self.datasets = {}  # name -> node data
        self.functions = {}  # name -> node data
        self.transformations = []  # list of transformations
        self.lineage_edges = []  # list of edges with line numbers
        self.purposes = {}  # path -> purpose statement
        
        # Find nodes JSON file
        nodes_files = list(self.cartography_dir.glob("*_nodes.json"))
        if nodes_files:
            nodes_file = nodes_files[0]
            try:
                with open(nodes_file, 'r', encoding='utf-8') as f:
                    nodes_data = json.load(f)
                
                for node_id, node_data in nodes_data.items():
                    if node_id.startswith('module:'):
                        path = node_data.get('path', node_id.split(':',1)[1])
                        self.modules[path] = node_data
                    elif node_id.startswith('dataset:'):
                        name = node_data.get('name', node_id.split(':',1)[1])
                        self.datasets[name] = node_data
                    elif node_id.startswith('function:'):
                        name = node_data.get('qualified_name', node_id.split(':',1)[1])
                        self.functions[name] = node_data
                
                logger.info(f"Loaded {len(self.modules)} modules from {nodes_file.name}")
            except Exception as e:
                logger.warning(f"Failed to load nodes: {e}")
        
        # Load lineage graph with line numbers
        lineage_file = self.cartography_dir / "lineage_graph.json"
        if lineage_file.exists():
            try:
                with open(lineage_file, 'r', encoding='utf-8') as f:
                    lineage = json.load(f)
                
                self.lineage_edges = lineage.get('edges', [])
                self.datasets.update(lineage.get('datasets', {}))
                
                logger.info(f"Loaded lineage graph with {len(self.lineage_edges)} edges")
                
                # Count edges with line numbers
                with_lines = sum(1 for e in self.lineage_edges if e.get('line_range'))
                logger.info(f"   {with_lines} edges have line numbers")
                
            except Exception as e:
                logger.warning(f"Failed to load lineage: {e}")
        
        # Load purpose statements
        purposes_file = self.cartography_dir / "purpose_statements.json"
        if purposes_file.exists():
            try:
                with open(purposes_file, 'r', encoding='utf-8') as f:
                    self.purposes = json.load(f)
                logger.info(f"Loaded {len(self.purposes)} purpose statements")
            except:
                self.purposes = {}
    
    # ==================== TOOL 1: Find Implementation with Semantic Search ====================
    
    def find_implementation(self, concept: str, use_semantic: bool = True) -> Dict[str, Any]:
        """
        Find where a concept is implemented using semantic search.
        
        Args:
            concept: What to search for (e.g., "user authentication")
            use_semantic: Whether to use semantic search
            
        Returns:
            Dictionary with matching modules and evidence
        """
        logger.info(f"🔍 Finding implementation for: '{concept}'")
        
        # Store query in history for chaining
        self.conversation_history.append({
            "role": "user",
            "tool": "find_implementation",
            "query": concept
        })
        
        results = []
        
        # Try semantic search first
        if use_semantic and self.semantic_search.index_built:
            semantic_results = self.semantic_search.search(concept, top_k=15)
            
            for r in semantic_results:
                path = r['path']
                results.append({
                    "path": path,
                    "match_type": r['match_type'],
                    "confidence": r['similarity'],
                    "purpose": self.purposes.get(path, ""),
                    "evidence": [{
                        "file": path,
                        "analysis_method": "semantic",
                        "confidence": r['similarity']
                    }]
                })
        
        # Fallback to keyword search
        if not results:
            concept_lower = concept.lower()
            for path in self.modules.keys():
                # Check path
                if concept_lower in path.lower():
                    results.append({
                        "path": path,
                        "match_type": "path",
                        "confidence": 0.7,
                        "purpose": self.purposes.get(path, ""),
                        "evidence": [{
                            "file": path,
                            "analysis_method": "static",
                            "confidence": 0.7
                        }]
                    })
                
                # Check purpose
                purpose = self.purposes.get(path, "").lower()
                if purpose and concept_lower in purpose:
                    results.append({
                        "path": path,
                        "match_type": "purpose",
                        "confidence": 0.9,
                        "purpose": self.purposes.get(path, ""),
                        "evidence": [{
                            "file": path,
                            "analysis_method": "llm",
                            "confidence": 0.9
                        }]
                    })
        
        # Deduplicate and sort
        unique_results = {}
        for r in results:
            if r['path'] not in unique_results or r['confidence'] > unique_results[r['path']]['confidence']:
                unique_results[r['path']] = r
        
        final_results = sorted(unique_results.values(), key=lambda x: x['confidence'], reverse=True)[:10]
        
        # Store for chaining
        self.last_results['find_implementation'] = final_results
        
        return {
            "query": concept,
            "results": final_results,
            "count": len(final_results),
            "metadata": {
                "tool": "find_implementation",
                "analysis_method": "semantic" if use_semantic else "keyword",
                "timestamp": datetime.now().isoformat()
            }
        }
    
    # ==================== TOOL 2: Trace Lineage with Line Numbers ====================
    
    def trace_lineage(self, dataset: str, direction: str = "both", max_depth: int = 5) -> Dict[str, Any]:
        """
        Trace data lineage with line number evidence.
        
        Args:
            dataset: Name of the dataset to trace
            direction: 'upstream', 'downstream', or 'both'
            max_depth: Maximum depth to traverse
            
        Returns:
            Dictionary with lineage information and evidence
        """
        logger.info(f"🔄 Tracing lineage for '{dataset}' ({direction})")
        
        # Store in history
        self.conversation_history.append({
            "role": "user",
            "tool": "trace_lineage",
            "dataset": dataset,
            "direction": direction
        })
        
        # Check if dataset exists
        if dataset not in self.datasets:
            similar = self._find_similar_datasets(dataset)
            if similar:
                return {
                    "error": f"Dataset '{dataset}' not found",
                    "suggestion": f"Did you mean: {similar[0]}?",
                    "similar_datasets": similar[:5],
                    "metadata": {"tool": "trace_lineage", "success": False}
                }
        
        # Build graph
        graph = {}
        reverse_graph = {}
        
        for edge in self.lineage_edges:
            source = edge.get('source')
            target = edge.get('target')
            
            if source and target:
                # Forward graph
                if source not in graph:
                    graph[source] = []
                graph[source].append({
                    "target": target,
                    "file": edge.get('file', 'unknown'),
                    "line_range": edge.get('line_range', ''),
                    "type": edge.get('type', 'unknown'),
                    "method": "static"
                })
                
                # Reverse graph
                if target not in reverse_graph:
                    reverse_graph[target] = []
                reverse_graph[target].append({
                    "source": source,
                    "file": edge.get('file', 'unknown'),
                    "line_range": edge.get('line_range', ''),
                    "type": edge.get('type', 'unknown'),
                    "method": "static"
                })
        
        result = {
            "dataset": dataset,
            "direction": direction,
            "upstream": [],
            "downstream": [],
            "evidence": []
        }
        
        # Trace upstream
        if direction in ["upstream", "both"]:
            visited = set()
            queue = [(dataset, 0)]
            
            while queue:
                current, depth = queue.pop(0)
                if depth >= max_depth:
                    continue
                
                for edge in reverse_graph.get(current, []):
                    source = edge['source']
                    if source not in visited:
                        visited.add(source)
                        
                        # Add to result
                        result["upstream"].append({
                            "dataset": source,
                            "depth": depth + 1,
                            "via": {
                                "file": edge['file'],
                                "line_range": edge['line_range']
                            }
                        })
                        
                        # Add evidence
                        result["evidence"].append({
                            "file": edge['file'],
                            "line_range": edge['line_range'],
                            "description": f"{source} → {current}",
                            "analysis_method": edge['method'],
                            "confidence": 1.0
                        })
                        
                        queue.append((source, depth + 1))
        
        # Trace downstream
        if direction in ["downstream", "both"]:
            visited = set()
            queue = [(dataset, 0)]
            
            while queue:
                current, depth = queue.pop(0)
                if depth >= max_depth:
                    continue
                
                for edge in graph.get(current, []):
                    target = edge['target']
                    if target not in visited:
                        visited.add(target)
                        
                        result["downstream"].append({
                            "dataset": target,
                            "depth": depth + 1,
                            "via": {
                                "file": edge['file'],
                                "line_range": edge['line_range']
                            }
                        })
                        
                        result["evidence"].append({
                            "file": edge['file'],
                            "line_range": edge['line_range'],
                            "description": f"{current} → {target}",
                            "analysis_method": edge['method'],
                            "confidence": 1.0
                        })
                        
                        queue.append((target, depth + 1))
        
        # Add metadata
        result["upstream_count"] = len(result["upstream"])
        result["downstream_count"] = len(result["downstream"])
        result["metadata"] = {
            "tool": "trace_lineage",
            "analysis_method": "static",
            "timestamp": datetime.now().isoformat()
        }
        
        # Store for chaining
        self.last_results['trace_lineage'] = result
        
        return result
    
    # ==================== TOOL 3: Blast Radius with Evidence ====================
    
    def blast_radius(self, module_path: str) -> Dict[str, Any]:
        """
        Calculate blast radius with evidence citations.
        
        Args:
            module_path: Path to the module
            
        Returns:
            Dictionary with dependent modules and evidence
        """
        logger.info(f"💥 Calculating blast radius for: {module_path}")
        
        # Store in history
        self.conversation_history.append({
            "role": "user",
            "tool": "blast_radius",
            "module": module_path
        })
        
        # Find module if partial path
        if module_path not in self.modules:
            matches = [p for p in self.modules.keys() if module_path in p]
            if matches:
                module_path = matches[0]
                logger.info(f"Found match: {module_path}")
            else:
                return {
                    "error": f"Module '{module_path}' not found",
                    "suggestion": "Try a different module name",
                    "metadata": {"tool": "blast_radius", "success": False}
                }
        
        # Load module graph
        import_graph = {}
        module_graph_file = self.cartography_dir / "module_graph.json"
        
        if module_graph_file.exists():
            try:
                with open(module_graph_file, 'r') as f:
                    module_graph = json.load(f)
                
                for edge in module_graph.get('edges', []):
                    source = edge.get('source', '')
                    target = edge.get('target', '')
                    
                    # Extract paths
                    if source.startswith('module:'):
                        source_path = source.split(':',1)[1]
                    else:
                        source_path = source
                    
                    if target.startswith('module:'):
                        target_path = target.split(':',1)[1]
                    else:
                        target_path = target
                    
                    if source_path not in import_graph:
                        import_graph[source_path] = []
                    import_graph[source_path].append({
                        "target": target_path,
                        "file": edge.get('file', source_path),
                        "method": "static"
                    })
            except Exception as e:
                logger.warning(f"Could not load module graph: {e}")
        
        # Find downstream dependents
        downstream_modules = []
        evidence = []
        
        # Direct dependents
        for imp in import_graph.get(module_path, []):
            downstream_modules.append({
                "path": imp['target'],
                "depth": 1,
                "via": {
                    "file": imp['file'],
                    "relationship": "imports"
                }
            })
            evidence.append({
                "file": imp['file'],
                "description": f"{imp['target']} imports {module_path}",
                "analysis_method": imp['method'],
                "confidence": 1.0
            })
        
        # Indirect dependents (BFS)
        visited = {module_path}
        queue = [m['path'] for m in downstream_modules]
        depth = 2
        
        while queue and depth <= 5:
            next_queue = []
            for current in queue:
                if current in visited:
                    continue
                visited.add(current)
                
                for imp in import_graph.get(current, []):
                    if imp['target'] not in visited:
                        downstream_modules.append({
                            "path": imp['target'],
                            "depth": depth,
                            "via": {
                                "file": imp['file'],
                                "relationship": "transitive import"
                            }
                        })
                        evidence.append({
                            "file": imp['file'],
                            "description": f"{imp['target']} imports {current} (depends on {module_path})",
                            "analysis_method": imp['method'],
                            "confidence": 0.9
                        })
                        next_queue.append(imp['target'])
            
            queue = next_queue
            depth += 1
        
        # Find affected datasets
        affected_datasets = set()
        for edge in self.lineage_edges:
            file = edge.get('file', '')
            if module_path in file or any(m['path'] in file for m in downstream_modules):
                affected_datasets.add(edge.get('source'))
                affected_datasets.add(edge.get('target'))
        
        affected_datasets = {d for d in affected_datasets if d}
        
        result = {
            "module": module_path,
            "direct_dependents": [m['path'] for m in downstream_modules if m['depth'] == 1],
            "all_downstream_modules": [m['path'] for m in downstream_modules],
            "downstream_count": len(downstream_modules),
            "affected_datasets": list(affected_datasets),
            "dataset_count": len(affected_datasets),
            "evidence": evidence,
            "warning": f"⚠️  Changes to {module_path} could affect {len(downstream_modules)} modules and {len(affected_datasets)} datasets",
            "metadata": {
                "tool": "blast_radius",
                "analysis_method": "static",
                "timestamp": datetime.now().isoformat()
            }
        }
        
        # Store for chaining
        self.last_results['blast_radius'] = result
        
        return result
    
    # ==================== TOOL 4: Explain Module with Method Tags ====================
    
    def explain_module(self, module_path: str, use_llm: bool = True) -> Dict[str, Any]:
        """
        Explain what a module does with method tags.
        
        Args:
            module_path: Path to the module
            use_llm: Whether to use LLM for explanation
            
        Returns:
            Dictionary with explanation and evidence
        """
        logger.info(f"📖 Explaining module: {module_path}")
        
        # Store in history
        self.conversation_history.append({
            "role": "user",
            "tool": "explain_module",
            "module": module_path
        })
        
        # Find module
        if module_path not in self.modules:
            matches = [p for p in self.modules.keys() if module_path in p]
            if matches:
                module_path = matches[0]
            else:
                return {
                    "error": f"Module '{module_path}' not found",
                    "metadata": {"tool": "explain_module", "success": False}
                }
        
        # Get module data
        module_data = self.modules[module_path]
        purpose = self.purposes.get(module_path, "No purpose statement available")
        
        # Build response with evidence
        result = {
            "path": module_path,
            "language": module_data.get('language', 'unknown'),
            "purpose": purpose,
            "complexity": module_data.get('complexity_score', 0),
            "change_velocity": module_data.get('change_velocity_30d', 0),
            "functions": module_data.get('functions', [])[:10],
            "classes": module_data.get('classes', [])[:5],
            "imports": module_data.get('imports', [])[:10],
            "evidence": [
                {
                    "file": module_path,
                    "analysis_method": "static",
                    "description": "Module metadata from static analysis",
                    "confidence": 1.0
                },
                {
                    "file": module_path,
                    "analysis_method": "llm" if purpose != "No purpose statement available" else "none",
                    "description": "Purpose statement from LLM analysis",
                    "confidence": 0.8 if purpose != "No purpose statement available" else 0.0
                }
            ]
        }
        
        # Add LLM explanation if requested
        if use_llm and (os.getenv("OPENROUTER_API_KEY") or os.getenv("OPENAI_API_KEY")):
            try:
                # Read file content
                content = ""
                if self.repo_path:
                    full_path = self.repo_path / module_path
                    if full_path.exists():
                        with open(full_path, 'r', encoding='utf-8') as f:
                            content = f.read()[:2000]
                
                # Create LLM client
                client = LLMClient(provider="openrouter", model="google/gemini-flash-1.5")
                
                # Prepare prompt
                functions_str = "\n".join([f"- {f.get('name', 'unknown')}" for f in result['functions'][:5]])
                classes_str = "\n".join([f"- {c}" for c in result['classes'][:5]])
                
                system_prompt = "You are an expert developer explaining code to a teammate."
                user_prompt = f"""
Explain this module:

File: {module_path}
Purpose: {purpose}
Language: {result['language']}

Key Functions:
{functions_str or 'None'}

Key Classes:
{classes_str or 'None'}

Please provide:
1. A one-paragraph summary of what this module does
2. How it fits into the larger system
3. Any important details or gotchas
"""
                
                response = client.complete(
                    prompt=user_prompt,
                    system_prompt=system_prompt,
                    max_tokens=300,
                    temperature=0.3
                )
                
                if response.get("content"):
                    result["llm_explanation"] = response["content"]
                    result["evidence"].append({
                        "file": module_path,
                        "analysis_method": "llm",
                        "description": "Generated explanation",
                        "confidence": 0.7
                    })
                    
            except Exception as e:
                logger.warning(f"LLM explanation failed: {e}")
        
        result["metadata"] = {
            "tool": "explain_module",
            "analysis_method": "hybrid",
            "timestamp": datetime.now().isoformat()
        }
        
        # Store for chaining
        self.last_results['explain_module'] = result
        
        return result
    
    # ==================== MULTI-STEP TOOL CHAINING ====================
    
    def chain_tools(self, steps: List[Dict]) -> Dict[str, Any]:
        """
        Execute multiple tools in sequence, passing results between them.
        
        Args:
            steps: List of tool calls, each with tool name and params
                  Can use {previous.result} syntax to reference previous results
        
        Returns:
            Combined results from all steps
        """
        logger.info(f"🔗 Executing multi-step chain with {len(steps)} steps")
        
        chain_results = []
        context = {}
        
        for i, step in enumerate(steps):
            tool = step.get('tool')
            params = step.get('params', {})
            
            # Resolve parameter references
            resolved_params = {}
            for key, value in params.items():
                if isinstance(value, str) and '{' in value:
                    # Replace {previous.result.X} with actual values
                    for prev_result in chain_results:
                        placeholder = f"{{previous.result.{key}}}"
                        if placeholder in value:
                            # This is simplified - in production, use proper templating
                            value = value.replace(placeholder, str(prev_result.get(key, '')))
                resolved_params[key] = value
            
            # Execute tool
            if tool in self.tools:
                logger.info(f"  Step {i+1}: {tool} with {resolved_params}")
                result = self.tools[tool](**resolved_params)
                
                chain_results.append({
                    "step": i+1,
                    "tool": tool,
                    "params": resolved_params,
                    "result": result
                })
                
                # Update context
                context[f"step_{i+1}"] = result
                context[f"last_result"] = result
            else:
                return {
                    "error": f"Unknown tool: {tool}",
                    "metadata": {"chain": steps}
                }
        
        return {
            "chain": steps,
            "results": chain_results,
            "final_result": chain_results[-1]['result'] if chain_results else None,
            "metadata": {
                "tool": "chain",
                "steps": len(steps),
                "timestamp": datetime.now().isoformat()
            }
        }
    
    def interactive_chain(self, question: str) -> Dict[str, Any]:
        """
        Parse a complex question and execute appropriate tool chain.
        
        Args:
            question: Natural language question that might require multiple tools
            
        Returns:
            Combined results
        """
        question_lower = question.lower()
        
        # Complex query patterns that need chaining
        
        # Pattern: "find X and explain it"
        if "find" in question_lower and "explain" in question_lower:
            # Extract concept
            import re
            match = re.search(r'find\s+(.+?)\s+and\s+explain', question_lower)
            if match:
                concept = match.group(1)
                
                steps = [
                    {
                        "tool": "find_implementation",
                        "params": {"concept": concept}
                    },
                    {
                        "tool": "explain_module",
                        "params": {"module_path": "{previous.result.results[0].path}"}
                    }
                ]
                return self.chain_tools(steps)
        
        # Pattern: "trace X and show blast radius"
        elif "trace" in question_lower and "blast" in question_lower:
            match = re.search(r'trace\s+[\'"]?([^\'"]+)[\'"]?', question_lower)
            if match:
                dataset = match.group(1)
                
                steps = [
                    {
                        "tool": "trace_lineage",
                        "params": {"dataset": dataset, "direction": "both"}
                    },
                    {
                        "tool": "blast_radius",
                        "params": {"module_path": "{previous.result.transformations[0].file}"}
                    }
                ]
                return self.chain_tools(steps)
        
        # Pattern: "what produces X and where is the code"
        elif "produces" in question_lower and "code" in question_lower:
            match = re.search(r'produces\s+[\'"]?([^\'"]+)[\'"]?', question_lower)
            if match:
                dataset = match.group(1)
                
                steps = [
                    {
                        "tool": "trace_lineage",
                        "params": {"dataset": dataset, "direction": "upstream"}
                    },
                    {
                        "tool": "find_implementation",
                        "params": {"concept": "{previous.result.transformations[0].file}"}
                    }
                ]
                return self.chain_tools(steps)
        
        # Default to single tool
        return self.query(question)
    
    def query(self, question: str) -> Dict[str, Any]:
        """
        Answer a natural language question.
        
        Args:
            question: Natural language question
            
        Returns:
            Dictionary with answer
        """
        question_lower = question.lower()
        
        # Check if it's a complex chain
        if any(word in question_lower for word in ["then", "and then", "followed by"]):
            return self.interactive_chain(question)
        
        # Route to appropriate tool
        
        # Find implementation
        if any(word in question_lower for word in ["where is", "find", "locate", "code for"]):
            for prefix in ["where is the ", "where is ", "find the ", "find ", "code for "]:
                if prefix in question_lower:
                    concept = question_lower.split(prefix)[-1].strip(' ?')
                    result = self.find_implementation(concept)
                    return {
                        "tool": "find_implementation",
                        "question": question,
                        "answer": result
                    }
        
        # Trace lineage
        if any(word in question_lower for word in ["lineage", "produces", "consumes", "depends on"]):
            import re
            quote_match = re.search(r'[\'"]([^\'"]+)[\'"]', question)
            if quote_match:
                dataset = quote_match.group(1)
                
                if "upstream" in question_lower:
                    direction = "upstream"
                elif "downstream" in question_lower:
                    direction = "downstream"
                else:
                    direction = "both"
                
                result = self.trace_lineage(dataset, direction)
                return {
                    "tool": "trace_lineage",
                    "question": question,
                    "answer": result
                }
        
        # Blast radius
        if any(word in question_lower for word in ["blast radius", "what breaks", "impact"]):
            match = re.search(r'[\'"]([^\'"]+)[\'"]', question)
            if match:
                module = match.group(1)
                result = self.blast_radius(module)
                return {
                    "tool": "blast_radius",
                    "question": question,
                    "answer": result
                }
        
        # Explain module
        if any(word in question_lower for word in ["explain", "what does", "how does"]):
            match = re.search(r'[\'"]([^\'"]+)[\'"]', question)
            if match:
                module = match.group(1)
                result = self.explain_module(module)
                return {
                    "tool": "explain_module",
                    "question": question,
                    "answer": result
                }
        
        # Default to find_implementation
        words = question_lower.split()
        stopwords = ['the', 'a', 'an', 'is', 'are', 'was', 'were', 'how', 'what', 'where']
        keywords = [w for w in words if w not in stopwords and len(w) > 2]
        
        if keywords:
            result = self.find_implementation(keywords[0])
            if result.get('count', 0) > 0:
                return {
                    "tool": "find_implementation",
                    "question": question,
                    "answer": result,
                    "note": f"Interpreted as searching for '{keywords[0]}'"
                }
        
        return {
            "tool": "unknown",
            "question": question,
            "answer": {
                "error": "Could not understand question",
                "examples": [
                    "where is user authentication?",
                    "what produces 'daily_active_users'?",
                    "blast radius for 'src/main.py'",
                    "explain 'src/utils/helpers.py'",
                    "find data processing and explain it"
                ]
            }
        }
    
    def _find_similar_datasets(self, name: str) -> List[str]:
        """Find datasets with similar names."""
        similar = []
        name_lower = name.lower()
        
        for ds in self.datasets.keys():
            ds_lower = ds.lower()
            if name_lower in ds_lower or ds_lower in name_lower:
                similar.append(ds)
        
        return similar[:5]


# ==================== CLI Interface ====================

def interactive_mode(navigator: NavigatorAgent):
    """Run in interactive mode with multi-step support."""
    print("\n" + "="*60)
    print("🚀 BROWNFIELD CARTOGRAPHER NAVIGATOR v2.0")
    print("="*60)
    print("\n🔍 Semantic Search: " + ("✅ ACTIVE" if navigator.semantic_search.index_built else "❌ UNAVAILABLE"))
    print("🔄 Multi-step chaining: ✅ ACTIVE")
    print("\nType your questions or 'quit' to exit.")
    print("\nExamples:")
    print("  • where is user authentication?")
    print("  • what produces 'daily_active_users'?")
    print("  • blast radius for 'src/main.py'")
    print("  • explain 'src/utils/helpers.py'")
    print("  • find data processing and explain it (multi-step)")
    print("  • trace 'raw_events' then show blast radius (multi-step)")
    print("\n" + "-"*60)
    
    while True:
        try:
            question = input("\n❓ ").strip()
            
            if question.lower() in ['quit', 'exit', 'q']:
                print("Goodbye! 👋")
                break
            
            if not question:
                continue
            
            print("\n🔍 Processing...")
            result = navigator.query(question)
            
            print("\n" + "="*60)
            print(f"Tool: {result.get('tool', 'unknown')}")
            if 'note' in result:
                print(f"Note: {result['note']}")
            print("="*60)
            
            # Pretty print
            if 'answer' in result:
                answer = result['answer']
                
                if 'error' in answer:
                    print(f"\n❌ Error: {answer['error']}")
                    if 'suggestion' in answer:
                        print(f"💡 {answer['suggestion']}")
                
                elif result['tool'] == 'find_implementation':
                    print(f"\n📁 Found {answer.get('count', 0)} matches:\n")
                    for i, r in enumerate(answer.get('results', [])[:5], 1):
                        print(f"{i}. {r['path']}")
                        print(f"   Confidence: {r.get('confidence', 0):.2f} ({r.get('match_type', 'unknown')})")
                        print(f"   Purpose: {r.get('purpose', '')[:100]}...")
                        
                        # Show evidence
                        for e in r.get('evidence', []):
                            print(f"   📍 {e.get('file')} [{e.get('analysis_method')}]")
                        print()
                
                elif result['tool'] == 'trace_lineage':
                    print(f"\n🔄 Lineage for '{answer.get('dataset')}':\n")
                    print(f"Upstream: {answer.get('upstream_count', 0)}")
                    for u in answer.get('upstream', [])[:3]:
                        print(f"  • {u['dataset']} (depth {u['depth']})")
                        via = u.get('via', {})
                        print(f"    via {via.get('file')}:{via.get('line_range', '')}")
                    
                    print(f"\nDownstream: {answer.get('downstream_count', 0)}")
                    for d in answer.get('downstream', [])[:3]:
                        print(f"  • {d['dataset']} (depth {d['depth']})")
                        via = d.get('via', {})
                        print(f"    via {via.get('file')}:{via.get('line_range', '')}")
                    
                    if answer.get('evidence'):
                        print(f"\n📋 Evidence:")
                        for e in answer['evidence'][:3]:
                            print(f"  • {e.get('file')}:{e.get('line_range', '')} [{e.get('analysis_method')}]")
                
                elif result['tool'] == 'blast_radius':
                    print(f"\n💥 {answer.get('warning', '')}\n")
                    print(f"Direct dependents: {len(answer.get('direct_dependents', []))}")
                    for d in answer.get('direct_dependents', [])[:5]:
                        print(f"  • {d}")
                    
                    print(f"\nTotal affected modules: {answer.get('downstream_count', 0)}")
                    print(f"Affected datasets: {answer.get('dataset_count', 0)}")
                
                elif result['tool'] == 'explain_module':
                    print(f"\n📖 Module: {answer.get('path')}\n")
                    print(f"Purpose: {answer.get('purpose')}")
                    print(f"Language: {answer.get('language')}")
                    
                    if answer.get('llm_explanation'):
                        print(f"\n🤖 {answer['llm_explanation']}")
                    
                    print(f"\n📋 Evidence:")
                    for e in answer.get('evidence', []):
                        print(f"  • {e.get('description')} [{e.get('analysis_method')}]")
            
            elif 'chain' in result:
                print(f"\n🔗 Multi-step chain with {len(result.get('results', []))} steps:\n")
                for step in result.get('results', []):
                    print(f"Step {step['step']}: {step['tool']} → ✅")
            
            print("\n" + "-"*60)
            
        except KeyboardInterrupt:
            print("\n\nGoodbye! 👋")
            break
        except Exception as e:
            print(f"\n❌ Error: {e}")
            logger.debug(traceback.format_exc())


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Navigator Agent - Query codebase intelligence")
    parser.add_argument('--cartography', type=str, default='.cartography',
                       help='Cartography directory')
    parser.add_argument('--repo', type=str,
                       help='Path to repository (for reading files)')
    parser.add_argument('--query', type=str,
                       help='Single query to run')
    parser.add_argument('--verbose', action='store_true',
                       help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    if not os.path.exists(args.cartography):
        print(f"❌ Cartography directory not found: {args.cartography}")
        print("Run analysis first: python src/cli.py analyze --repo <path>")
        sys.exit(1)
    
    # Initialize navigator
    navigator = NavigatorAgent(args.cartography, args.repo)
    
    # Run single query or interactive
    if args.query:
        result = navigator.query(args.query)
        print(json.dumps(result, indent=2))
    else:
        interactive_mode(navigator)


if __name__ == "__main__":
    main()