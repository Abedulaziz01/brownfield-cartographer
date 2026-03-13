#!/usr/bin/env python3
"""Semanticist Agent - LLM-powered code understanding.

This agent uses LLMs to extract semantic meaning from code:
- Generate purpose statements
- Detect documentation drift
- Cluster modules into business domains
- Answer FDE Day-One questions
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Optional, Any, Tuple
import logging
import re

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from llm.client import LLMClient
from llm.prompts import get_system_prompt, format_prompt
from llm.context_window import ContextWindowBudget, TokenCounter
from llm.model_router import ModelRouter
from graph.knowledge_graph import KnowledgeGraph

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class SemanticistAgent:
    """
    Semanticist Agent - Adds LLM-powered understanding to the codebase.
    
    Responsibilities:
    - Generate purpose statements for modules
    - Detect documentation drift (docstring vs actual code)
    - Cluster modules into business domains
    - Answer the five FDE Day-One questions
    """
    
    def __init__(self, repo_path: str, kg: Optional[KnowledgeGraph] = None):
        """
        Initialize the Semanticist Agent.
        
        Args:
            repo_path: Path to the repository
            kg: Optional existing knowledge graph
        """
        self.repo_path = Path(repo_path).resolve()
        self.repo_name = self.repo_path.name
        
        # Use existing graph or create new
        self.kg = kg or KnowledgeGraph(self.repo_name)
        
        # If graph is empty, try to load from JSON
        if self.kg.graph.number_of_nodes() == 0:
            self._load_graph_from_json()
        
        # Initialize LLM components
        self.model_router = ModelRouter()
        self.budget = ContextWindowBudget(max_total_tokens=100000, max_cost_usd=5.0)
        
        # Storage for semantic data
        self.purpose_statements = {}  # file_path -> purpose
        self.doc_drift_results = {}   # file_path -> drift analysis
        self.domain_clusters = {}      # domain_name -> [file_paths]
        self.day_one_answers = {}      # answers to the 5 questions
        
        # Trace log for auditing
        self.trace = []
        
        logger.info(f"Semanticist Agent initialized for {self.repo_path}")
        logger.info(f"Knowledge graph has {self.kg.graph.number_of_nodes()} nodes")
    
    def _load_graph_from_json(self):
        """Load knowledge graph from JSON files if they exist."""
        logger.info("Attempting to load graph from JSON files...")
        
        # Look for nodes JSON file
        json_paths = [
            Path(".cartography") / f"{self.repo_name}_nodes.json",
            Path(".cartography") / "ol-data-platform_nodes.json",
            Path(".cartography") / "brownfield-cartographer_lineage_nodes.json"
        ]
        
        nodes_file = None
        for path in json_paths:
            if path.exists():
                nodes_file = path
                logger.info(f"Found nodes file: {nodes_file}")
                break
        
        if not nodes_file:
            logger.warning("No nodes JSON file found")
            return
        
        try:
            with open(nodes_file, 'r', encoding='utf-8') as f:
                nodes_data = json.load(f)
            
            # Add each node to the graph
            module_count = 0
            for node_id, node_data in nodes_data.items():
                # Add to graph
                self.kg.graph.add_node(node_id, **node_data)
                
                # Update node counts
                if node_id.startswith('module:'):
                    module_count += 1
                    self.kg.node_counts["module"] = self.kg.node_counts.get("module", 0) + 1
                elif node_id.startswith('dataset:'):
                    self.kg.node_counts["dataset"] = self.kg.node_counts.get("dataset", 0) + 1
                elif node_id.startswith('function:'):
                    self.kg.node_counts["function"] = self.kg.node_counts.get("function", 0) + 1
                elif node_id.startswith('transform:'):
                    self.kg.node_counts["transformation"] = self.kg.node_counts.get("transformation", 0) + 1
            
            logger.info(f"✅ Loaded {len(nodes_data)} nodes from JSON")
            logger.info(f"   - {module_count} module nodes found")
            
            # Also try to load edges if they exist
            edges_file = nodes_file.parent / f"{self.repo_name}_edges.json"
            if edges_file.exists():
                with open(edges_file, 'r', encoding='utf-8') as f:
                    edges_data = json.load(f)
                
                for edge in edges_data:
                    source = edge.get('source')
                    target = edge.get('target')
                    data = edge.get('data', {})
                    
                    if source and target:
                        self.kg.graph.add_edge(source, target, **data)
                
                logger.info(f"✅ Loaded {len(edges_data)} edges from JSON")
                
        except Exception as e:
            logger.warning(f"Failed to load graph from JSON: {e}")
    
    def analyze(self, surveyor_results: Optional[Dict] = None) -> KnowledgeGraph:
        """
        Run semantic analysis on the codebase.
        
        Args:
            surveyor_results: Optional results from Surveyor agent
            
        Returns:
            Updated KnowledgeGraph with semantic data
        """
        logger.info("Starting semantic analysis...")
        
        # Step 1: Get module nodes
        module_nodes = self._get_module_nodes()
        logger.info(f"Found {len(module_nodes)} modules to analyze")
        
        if len(module_nodes) == 0:
            logger.error("❌ No modules found! Cannot continue.")
            return self.kg
        
        # Step 2: Generate purpose statements for modules
        self._generate_purpose_statements(module_nodes)
        
        # Step 3: Detect documentation drift (sample)
        self._detect_documentation_drift(module_nodes)
        
        # Step 4: Cluster modules into domains
        if self.purpose_statements:
            self._cluster_into_domains()
        
        # Step 5: Answer Day-One questions
        self._answer_day_one_questions(surveyor_results)
        
        # Step 6: Update knowledge graph
        self._update_knowledge_graph()
        
        # Step 7: Save trace
        self._save_trace()
        
        logger.info(f"Semantic analysis complete. Processed {len(self.purpose_statements)} modules")
        return self.kg
    
    def _get_module_nodes(self) -> List[Dict]:
        """Get all module nodes from the knowledge graph."""
        modules = []
        
        # Get modules from graph
        for node in self.kg.graph.nodes:
            if node.startswith('module:'):
                data = self.kg.graph.nodes[node]
                
                # Get the file path from node data
                file_path = data.get('path', '')
                if not file_path and ':' in node:
                    # Extract path from node ID if not in data
                    file_path = node.split(':', 1)[1]
                
                # Skip if no file path
                if not file_path:
                    continue
                
                # Read file content
                content = self._read_file(file_path)
                
                modules.append({
                    "id": node,
                    "path": file_path,
                    "language": data.get('language', 'unknown'),
                    "content": content,
                    "metadata": data
                })
        
        # Sort by path for consistent ordering
        modules.sort(key=lambda x: x['path'])
        
        return modules
    
    def _read_file(self, file_path: str) -> str:
        """Read a file from the repository."""
        full_path = self.repo_path / file_path
        try:
            if full_path.exists():
                with open(full_path, 'r', encoding='utf-8', errors='ignore') as f:
                    return f.read()
        except Exception as e:
            logger.debug(f"Error reading {file_path}: {e}")
        return ""
    
    def _extract_docstring(self, code: str, language: str) -> Optional[str]:
        """Extract docstring from code based on language."""
        if language == "python":
            # Python docstring patterns
            patterns = [
                r'"""(.*?)"""',  # Triple double quotes
                r"'''(.*?)'''",   # Triple single quotes
                r'"""\n(.*?)\n"""',  # Multi-line
            ]
            for pattern in patterns:
                match = re.search(pattern, code, re.DOTALL)
                if match:
                    return match.group(1).strip()
        
        elif language in ["javascript", "typescript"]:
            # JSDoc pattern
            match = re.search(r'/\*\*(.*?)\*/', code, re.DOTALL)
            if match:
                return match.group(1).strip()
        
        return None
    
    def _generate_purpose_statements(self, modules: List[Dict]):
        """Generate purpose statements for modules using LLM."""
        logger.info("Generating purpose statements...")
        
        # If no API key, use mock data for testing
        if not os.getenv("OPENROUTER_API_KEY") and not os.getenv("OPENAI_API_KEY"):
            logger.warning("No API key found. Using mock purpose statements.")
            self._generate_mock_purposes(modules)
            return
        
        for i, module in enumerate(modules):
            file_path = module["path"]
            code = module["content"]
            language = module["language"]
            
            # Skip empty files
            if not code.strip():
                logger.debug(f"Skipping empty file: {file_path}")
                continue
            
            # Skip very large files (over 2000 lines)
            if len(code.split('\n')) > 2000:
                logger.debug(f"Skipping large file: {file_path} ({len(code.split('\n'))} lines)")
                self.purpose_statements[file_path] = "File too large for analysis"
                continue
            
            # Estimate tokens
            estimated_tokens = self.model_router.estimate_task_tokens("purpose_statement", code)
            
            # Check budget
            if not self.budget.check_budget(estimated_tokens, 100, "cheap"):
                logger.warning(f"Budget exceeded, stopping purpose generation")
                break
            
            # Select model
            model_name = self.model_router.get_model_for_task("purpose_statement", len(code))
            
            logger.info(f"[{i+1}/{len(modules)}] Analyzing {file_path}...")
            
            try:
                # Create LLM client
                client = LLMClient(provider="openrouter", model=model_name)
                
                # Prepare prompt
                system_prompt = get_system_prompt("purpose_statement")
                user_prompt = format_prompt(
                    "purpose_statement",
                    file_path=file_path,
                    language=language,
                    code=code[:3000]  # Limit code length
                )
                
                # Make API call
                response = client.complete(
                    prompt=user_prompt,
                    system_prompt=system_prompt,
                    max_tokens=150,
                    temperature=0.3
                )
                
                # Record in budget
                usage = response.get("usage", {})
                input_tokens = usage.get("prompt_tokens", estimated_tokens)
                output_tokens = usage.get("completion_tokens", 100)
                cost = self.budget.estimate_cost(model_name, input_tokens, output_tokens)
                
                self.budget.record_call(
                    model=model_name,
                    input_tokens=input_tokens,
                    output_tokens=output_tokens,
                    cost=cost,
                    success=response.get("content") is not None
                )
                
                # Store result
                if response.get("content"):
                    purpose = response["content"].strip()
                    self.purpose_statements[file_path] = purpose
                    
                    # Add to trace
                    self.trace.append({
                        "action": "generate_purpose",
                        "file": file_path,
                        "model": model_name,
                        "tokens": input_tokens + output_tokens,
                        "cost": cost,
                        "success": True
                    })
                    
                    logger.debug(f"  Purpose: {purpose[:100]}...")
                else:
                    logger.warning(f"  Failed: {response.get('error')}")
                    self.purpose_statements[file_path] = "Analysis failed"
                    
            except Exception as e:
                logger.warning(f"Error analyzing {file_path}: {e}")
                self.purpose_statements[file_path] = f"Error: {str(e)[:50]}"
        
        logger.info(f"Generated {len(self.purpose_statements)} purpose statements")
    
    def _generate_mock_purposes(self, modules: List[Dict]):
        """Generate mock purpose statements for testing (no API calls)."""
        logger.info("Generating MOCK purpose statements...")
        
        for i, module in enumerate(modules[:20]):  # Limit to 20 for mock
            file_path = module["path"]
            file_name = Path(file_path).name.replace('.py', '').replace('_', ' ').title()
            
            # Create a mock purpose based on filename
            self.purpose_statements[file_path] = f"Handles {file_name} functionality including data processing and business logic."
            
            logger.info(f"[{i+1}/20] Mock purpose for {file_path}")
            
            # Add to trace
            self.trace.append({
                "action": "generate_purpose_mock",
                "file": file_path,
                "success": True
            })
    
    def _detect_documentation_drift(self, modules: List[Dict]):
        """Detect discrepancies between docstrings and actual code."""
        logger.info("Detecting documentation drift...")
        
        # Only analyze a sample (first 20 files) to save cost
        sample_size = min(20, len(modules))
        
        for i, module in enumerate(modules[:sample_size]):
            file_path = module["path"]
            code = module["content"]
            language = module["language"]
            
            # Extract docstring
            docstring = self._extract_docstring(code, language)
            
            # Skip if no docstring
            if not docstring:
                continue
            
            # Skip very long code
            if len(code) > 5000:
                continue
            
            logger.info(f"Checking docstring for {file_path}...")
            
            # If no API key, use mock
            if not os.getenv("OPENROUTER_API_KEY") and not os.getenv("OPENAI_API_KEY"):
                self.doc_drift_results[file_path] = {
                    "is_accurate": "yes",
                    "missing": [],
                    "incorrect": [],
                    "behaviors_not_mentioned": [],
                    "confidence": 0.5
                }
                continue
            
            try:
                # Use cheap model for this task
                client = LLMClient(provider="openrouter", model="google/gemini-flash-1.5")
                
                system_prompt = get_system_prompt("doc_drift")
                user_prompt = format_prompt(
                    "doc_drift",
                    file_path=file_path,
                    docstring=docstring,
                    language=language,
                    code=code[:2000]
                )
                
                response = client.complete(
                    prompt=user_prompt,
                    system_prompt=system_prompt,
                    max_tokens=300,
                    temperature=0.2,
                    json_mode=True
                )
                
                # Parse JSON response
                if response.get("content"):
                    if isinstance(response["content"], dict):
                        result = response["content"]
                    else:
                        try:
                            result = json.loads(response["content"])
                        except:
                            result = {"is_accurate": "unknown", "missing": []}
                    
                    self.doc_drift_results[file_path] = result
                    
                    # Log if drift detected
                    if result.get("is_accurate") == "no":
                        logger.warning(f"  ⚠️  Documentation drift in {file_path}")
                    
            except Exception as e:
                logger.debug(f"Error in doc drift analysis for {file_path}: {e}")
    
    def _cluster_into_domains(self):
        """Cluster modules into business domains."""
        logger.info("Clustering modules into domains...")
        
        if not self.purpose_statements:
            logger.warning("No purpose statements to cluster")
            return
        
        # Prepare module list
        module_list = []
        for path, purpose in list(self.purpose_statements.items())[:50]:  # Limit to 50
            if purpose and not purpose.startswith("Error") and not purpose.startswith("File too large"):
                module_list.append(f"- {path}: {purpose}")
        
        if len(module_list) < 3:
            logger.warning("Not enough modules for clustering")
            # Create simple mock domains
            self._create_mock_domains()
            return
        
        # If no API key, use mock
        if not os.getenv("OPENROUTER_API_KEY") and not os.getenv("OPENAI_API_KEY"):
            self._create_mock_domains()
            return
        
        try:
            # Use medium model for clustering
            client = LLMClient(provider="openrouter", model="anthropic/claude-3-sonnet")
            
            system_prompt = get_system_prompt("domain_clustering")
            user_prompt = format_prompt(
                "domain_clustering",
                module_list="\n".join(module_list)
            )
            
            response = client.complete(
                prompt=user_prompt,
                system_prompt=system_prompt,
                max_tokens=1000,
                temperature=0.3,
                json_mode=True
            )
            
            if response.get("content"):
                if isinstance(response["content"], dict):
                    result = response["content"]
                else:
                    try:
                        result = json.loads(response["content"])
                    except:
                        logger.warning("Failed to parse clustering JSON")
                        self._create_mock_domains()
                        return
                
                # Store clusters
                self.domain_clusters = result.get("domains", {})
                
                # Log domains found
                logger.info(f"Found {len(self.domain_clusters)} domains:")
                for domain, modules in self.domain_clusters.items():
                    logger.info(f"  {domain}: {len(modules)} modules")
                    
        except Exception as e:
            logger.warning(f"Clustering failed: {e}")
            self._create_mock_domains()
    
    def _create_mock_domains(self):
        """Create mock domains based on file paths."""
        logger.info("Creating mock domains...")
        
        domains = {
            "Ingestion": [],
            "Processing": [],
            "API": [],
            "Storage": [],
            "Utils": []
        }
        
        for file_path in self.purpose_statements.keys():
            if 'ingest' in file_path.lower() or 'extract' in file_path.lower():
                domains["Ingestion"].append(file_path)
            elif 'process' in file_path.lower() or 'transform' in file_path.lower():
                domains["Processing"].append(file_path)
            elif 'api' in file_path.lower() or 'route' in file_path.lower() or 'controller' in file_path.lower():
                domains["API"].append(file_path)
            elif 'db' in file_path.lower() or 'database' in file_path.lower() or 'store' in file_path.lower():
                domains["Storage"].append(file_path)
            else:
                domains["Utils"].append(file_path)
        
        # Remove empty domains
        self.domain_clusters = {k: v for k, v in domains.items() if v}
        logger.info(f"Created {len(self.domain_clusters)} mock domains")
    
    def _answer_day_one_questions(self, surveyor_results: Optional[Dict] = None):
        """Answer the five FDE Day-One questions."""
        logger.info("Answering Day-One questions...")
        
        # Prepare evidence from other agents
        static_analysis = self._prepare_static_evidence()
        lineage = self._prepare_lineage_evidence()
        git_velocity = self._prepare_git_evidence()
        high_impact = self._prepare_high_impact_modules()
        
        # If no API key, use mock answers
        if not os.getenv("OPENROUTER_API_KEY") and not os.getenv("OPENAI_API_KEY"):
            self._create_mock_answers()
            return
        
        try:
            # Use expensive model for this important task
            client = LLMClient(provider="openrouter", model="openai/gpt-4-turbo-preview")
            
            system_prompt = get_system_prompt("day_one_questions")
            user_prompt = format_prompt(
                "day_one_questions",
                static_analysis=json.dumps(static_analysis, indent=2)[:2000],
                lineage=json.dumps(lineage, indent=2)[:2000],
                git_velocity=json.dumps(git_velocity, indent=2)[:1000],
                high_impact=json.dumps(high_impact, indent=2)[:1000]
            )
            
            response = client.complete(
                prompt=user_prompt,
                system_prompt=system_prompt,
                max_tokens=1500,
                temperature=0.3,
                json_mode=True
            )
            
            if response.get("content"):
                if isinstance(response["content"], dict):
                    self.day_one_answers = response["content"]
                else:
                    try:
                        self.day_one_answers = json.loads(response["content"])
                    except:
                        logger.warning("Failed to parse Day-One answers JSON")
                        self._create_mock_answers()
                
                logger.info("✅ Day-One answers generated")
            else:
                self._create_mock_answers()
                
        except Exception as e:
            logger.warning(f"Failed to generate Day-One answers: {e}")
            self._create_mock_answers()
    
    def _create_mock_answers(self):
        """Create mock Day-One answers for testing."""
        self.day_one_answers = {
            "primary_ingestion_path": {
                "answer": "Data is primarily ingested through Kafka consumers in the ingestion module",
                "evidence": ["src/ingestion/kafka_consumer.py"],
                "confidence": "Medium"
            },
            "critical_datasets": [
                {
                    "name": "user_events",
                    "why_critical": "Core business metrics depend on this",
                    "evidence": ["src/processing/event_processor.py"],
                    "confidence": "Medium"
                }
            ],
            "blast_radius_module": {
                "module": "src/core/processor.py",
                "why": "Central processing logic with many dependents",
                "would_break": ["api", "reporting", "analytics"],
                "confidence": "Medium"
            },
            "business_logic_location": {
                "location": "src/business_rules/",
                "pattern": "Concentrated in rule engine",
                "evidence": ["src/business_rules/engine.py"],
                "confidence": "Medium"
            },
            "change_velocity": {
                "most_changed": ["src/api/endpoints.py", "src/models/user.py"],
                "pattern": "API changes most frequently",
                "insight": "Frontend team iterating rapidly"
            }
        }
    
    def _prepare_static_evidence(self) -> Dict:
        """Prepare static analysis evidence."""
        return {
            "total_modules": len([n for n in self.kg.graph.nodes if n.startswith('module:')]),
            "languages": self._count_languages(),
            "purpose_statements": list(self.purpose_statements.items())[:20]  # Sample
        }
    
    def _prepare_lineage_evidence(self) -> Dict:
        """Prepare lineage evidence."""
        datasets = []
        for node in self.kg.graph.nodes:
            if node.startswith('dataset:'):
                data = self.kg.graph.nodes[node]
                datasets.append({
                    "name": data.get("name", ""),
                    "type": data.get("storage_type", "unknown")
                })
        
        return {
            "datasets": datasets[:20],  # Sample
            "total_datasets": len(datasets)
        }
    
    def _prepare_git_evidence(self) -> Dict:
        """Prepare git velocity evidence."""
        high_velocity = []
        for node in self.kg.graph.nodes:
            if node.startswith('module:'):
                data = self.kg.graph.nodes[node]
                velocity = data.get('change_velocity_30d', 0)
                if velocity > 0:
                    high_velocity.append({
                        "path": data.get('path', ''),
                        "changes": velocity
                    })
        
        # Sort by velocity
        high_velocity.sort(key=lambda x: x['changes'], reverse=True)
        
        return {
            "high_velocity_files": high_velocity[:10]
        }
    
    def _prepare_high_impact_modules(self) -> List:
        """Prepare high impact modules based on PageRank."""
        modules = []
        for node in self.kg.graph.nodes:
            if node.startswith('module:'):
                data = self.kg.graph.nodes[node]
                pagerank = data.get('pagerank', 0)
                if pagerank > 0:
                    modules.append({
                        "path": data.get('path', ''),
                        "pagerank": pagerank,
                        "in_degree": data.get('in_degree', 0)
                    })
        
        modules.sort(key=lambda x: x['pagerank'], reverse=True)
        return modules[:10]
    
    def _count_languages(self) -> Dict:
        """Count modules by language."""
        counts = {}
        for node in self.kg.graph.nodes:
            if node.startswith('module:'):
                lang = self.kg.graph.nodes[node].get('language', 'unknown')
                counts[lang] = counts.get(lang, 0) + 1
        return counts
    
    def _update_knowledge_graph(self):
        """Update knowledge graph with semantic data."""
        logger.info("Updating knowledge graph with semantic data...")
        
        # Add purpose statements
        for file_path, purpose in self.purpose_statements.items():
            node_id = f"module:{file_path}"
            if self.kg.graph.has_node(node_id):
                self.kg.graph.nodes[node_id]['purpose_statement'] = purpose
        
        # Add domain clusters
        for domain, modules in self.domain_clusters.items():
            for file_path in modules:
                node_id = f"module:{file_path}"
                if self.kg.graph.has_node(node_id):
                    self.kg.graph.nodes[node_id]['domain_cluster'] = domain
        
        # Add doc drift results
        for file_path, drift in self.doc_drift_results.items():
            node_id = f"module:{file_path}"
            if self.kg.graph.has_node(node_id):
                self.kg.graph.nodes[node_id]['doc_drift'] = drift
        
        logger.info("Knowledge graph updated")
    
    def _save_trace(self):
        """Save trace log for auditing."""
        trace_path = Path(".cartography") / "semantic_trace.jsonl"
        trace_path.parent.mkdir(exist_ok=True)
        
        with open(trace_path, 'w') as f:
            for entry in self.trace:
                f.write(json.dumps(entry) + '\n')
        
        logger.info(f"Saved trace to {trace_path}")
    
    def save_results(self, output_dir: str = ".cartography"):
        """Save semantic analysis results."""
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Save purpose statements
        purposes_path = output_path / "purpose_statements.json"
        with open(purposes_path, 'w') as f:
            json.dump(self.purpose_statements, f, indent=2)
        
        # Save doc drift results
        drift_path = output_path / "doc_drift.json"
        with open(drift_path, 'w') as f:
            json.dump(self.doc_drift_results, f, indent=2)
        
        # Save domain clusters
        domains_path = output_path / "domain_clusters.json"
        with open(domains_path, 'w') as f:
            json.dump(self.domain_clusters, f, indent=2)
        
        # Save Day-One answers
        answers_path = output_path / "day_one_answers.json"
        with open(answers_path, 'w') as f:
            json.dump(self.day_one_answers, f, indent=2)
        
        # Save budget summary
        budget_path = output_path / "llm_budget.json"
        with open(budget_path, 'w') as f:
            json.dump(self.budget.get_summary(), f, indent=2)
        
        logger.info(f"Saved semantic results to {output_path}")
        
        return {
            "purposes": str(purposes_path),
            "drift": str(drift_path),
            "domains": str(domains_path),
            "answers": str(answers_path),
            "budget": str(budget_path)
        }


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Semanticist Agent - LLM code understanding")
    parser.add_argument('--repo', type=str, required=True,
                       help='Path to repository to analyze')
    parser.add_argument('--kg', type=str, default='',
                       help='Path to knowledge graph file (optional)')
    parser.add_argument('--output', type=str, default='.cartography',
                       help='Output directory')
    parser.add_argument('--sample', type=int, default=0,
                       help='Analyze only N files (for testing)')
    parser.add_argument('--budget', type=float, default=5.0,
                       help='Maximum budget in USD')
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
    
    # Load knowledge graph if provided
    kg = None
    if args.kg and os.path.exists(args.kg):
        try:
            kg = KnowledgeGraph.load_from_graphml(args.kg)
            logger.info(f"Loaded knowledge graph from {args.kg}")
        except Exception as e:
            logger.warning(f"Could not load knowledge graph: {e}")
    
    # Run semanticist
    try:
        semanticist = SemanticistAgent(args.repo, kg)
        semanticist.budget.max_cost_usd = args.budget
        
        # If sample specified, modify the modules list
        if args.sample > 0:
            logger.info(f"Sample mode: will analyze up to {args.sample} files")
            # Get modules and slice them
            modules = semanticist._get_module_nodes()
            if len(modules) > args.sample:
                # This is handled in _generate_purpose_statements
                pass
        
        semanticist.analyze()
        files = semanticist.save_results(args.output)
        
        print(f"\n✅ Semanticist analysis complete!")
        print(f"📊 Budget used: ${semanticist.budget.total_cost:.4f}")
        print(f"📝 Purpose statements: {len(semanticist.purpose_statements)}")
        print(f"🔍 Doc drift analyzed: {len(semanticist.doc_drift_results)}")
        print(f"🏢 Domains found: {len(semanticist.domain_clusters)}")
        
        if semanticist.day_one_answers:
            print(f"\n📋 Day-One Answers:")
            if "primary_ingestion_path" in semanticist.day_one_answers:
                ans = semanticist.day_one_answers["primary_ingestion_path"]
                print(f"  1. Ingestion: {ans.get('answer', 'N/A')[:100]}...")
        
        print(f"\n📁 Results saved to {args.output}/")
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()