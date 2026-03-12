#!/usr/bin/env python3
"""Hydrologist Agent - Data lineage analysis for codebases.

This agent builds the data lineage graph showing how data flows
through the system: from source tables to transformations to output datasets.
"""

import os
import sys
import json
import re
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Set, Optional, Any, Tuple
import logging
from collections import defaultdict, deque

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from graph.knowledge_graph import KnowledgeGraph
from analyzers.sql_lineage import SQLLineageAnalyzer
from analyzers.dag_config_parser import DAGConfigParser
from analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from analyzers.language_router import LanguageRouter

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class HydrologistAgent:
    """
    Hydrologist Agent - Maps data lineage through the system.
    
    Responsibilities:
    - Extract data lineage from SQL files
    - Parse DAG configurations (Airflow, dbt)
    - Detect data sources/sinks in Python code
    - Build unified data lineage graph
    - Implement blast radius analysis
    """
    
    def __init__(self, repo_path: str):
        """
        Initialize the Hydrologist Agent.
        
        Args:
            repo_path: Path to the repository to analyze
        """
        self.repo_path = Path(repo_path).resolve()
        self.repo_name = self.repo_path.name
        
        # Initialize analyzers
        self.sql_analyzer = SQLLineageAnalyzer()
        self.dag_parser = DAGConfigParser()
        self.tree_sitter = TreeSitterAnalyzer()
        self.language_router = LanguageRouter()
        
        # Initialize knowledge graph (will be merged with Surveyor's later)
        self.kg = KnowledgeGraph(f"{self.repo_name}_lineage")
        
        # Data lineage storage
        self.lineage_graph = {
            "nodes": {},  # dataset_name -> node_info
            "edges": [],  # source -> target transformations
            "transformations": []  # transformation nodes
        }
        
        # Track datasets
        self.datasets = {}  # name -> metadata
        self.transformations = []  # list of transformations
        
        logger.info(f"Hydrologist Agent initialized for {self.repo_path}")
    
    def analyze(self) -> KnowledgeGraph:
        """
        Run full data lineage analysis on the repository.
        
        Returns:
            Populated KnowledgeGraph with lineage data
        """
        logger.info(f"Starting data lineage analysis of {self.repo_path}")
        
        # Step 1: Find all relevant files
        sql_files = []
        config_files = []
        python_files = []
        
        for root, dirs, files in os.walk(self.repo_path):
            dirs[:] = [d for d in dirs if d not in 
                      ['.git', '__pycache__', 'node_modules', 'venv', 'env', '.pytest_cache', 'dist']]
            
            for file in files:
                file_path = os.path.join(root, file)
                
                if file.endswith('.sql'):
                    sql_files.append(file_path)
                elif file.endswith(('.yml', '.yaml', '.json')):
                    config_files.append(file_path)
                elif file.endswith('.py'):
                    python_files.append(file_path)
        
        logger.info(f"Found {len(sql_files)} SQL files, {len(config_files)} config files, {len(python_files)} Python files")
        
        # Step 2: Analyze SQL files for lineage
        self._analyze_sql_files(sql_files)
        
        # Step 3: Parse DAG configurations
        self._parse_config_files(config_files)
        
        # Step 4: Detect data operations in Python
        self._analyze_python_files(python_files)
        
        # Step 5: Build unified lineage graph
        self._build_lineage_graph()
        
        # Step 6: Add to knowledge graph
        self._populate_knowledge_graph()
        
        logger.info(f"Lineage analysis complete. Found {len(self.datasets)} datasets, "
                   f"{len(self.transformations)} transformations")
        
        return self.kg
    
    def _analyze_sql_files(self, sql_files: List[str]):
        """Analyze SQL files for data lineage."""
        logger.info("Analyzing SQL files...")
        
        for file_path in sql_files:
            rel_path = os.path.relpath(file_path, self.repo_path)
            
            try:
                # Read the SQL file
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    sql_content = f.read()
                
                # Extract table names using regex
                tables = self._extract_table_names(sql_content)
                
                if tables:
                    # For CREATE TABLE ... AS SELECT statements, the first table is the target
                    # and the rest are sources
                    if 'CREATE TABLE' in sql_content.upper() and 'AS SELECT' in sql_content.upper():
                        if len(tables) >= 1:
                            transformation = {
                                "id": f"sql:{rel_path}",
                                "file": rel_path,
                                "type": "sql",
                                "sources": tables[1:] if len(tables) > 1 else [],
                                "targets": [tables[0]] if tables else [],
                                "intermediate": [],
                                "sql": sql_content[:500]  # Store first 500 chars as sample
                            }
                    else:
                        # For other SQL statements, try to determine based on keywords
                        transformation = {
                            "id": f"sql:{rel_path}",
                            "file": rel_path,
                            "type": "sql",
                            "sources": self._find_source_tables(sql_content, tables),
                            "targets": self._find_target_tables(sql_content, tables),
                            "intermediate": [],
                            "sql": sql_content[:500]
                        }
                    
                    self.transformations.append(transformation)
                    
                    # Add to lineage graph
                    for source in transformation["sources"]:
                        self._add_dataset(source, "source", rel_path)
                    
                    for target in transformation["targets"]:
                        self._add_dataset(target, "target", rel_path)
                    
                    # Create edges
                    for source in transformation["sources"]:
                        for target in transformation["targets"]:
                            self.lineage_graph["edges"].append({
                                "source": source,
                                "target": target,
                                "transformation": transformation["id"],
                                "file": rel_path,
                                "type": "sql"
                            })
            
            except Exception as e:
                logger.debug(f"Error analyzing SQL file {rel_path}: {e}")
        
        logger.info(f"Analyzed {len(sql_files)} SQL files, found {len(self.transformations)} transformations")
    
    def _extract_table_names(self, sql: str) -> List[str]:
        """
        Extract table names from SQL using regex.
        
        Args:
            sql: SQL query string
            
        Returns:
            List of table names
        """
        tables = []
        
        # Match CREATE TABLE statements
        create_pattern = r'CREATE\s+TABLE\s+(\w+)'
        creates = re.findall(create_pattern, sql, re.IGNORECASE)
        tables.extend(creates)
        
        # Match FROM clauses
        from_pattern = r'FROM\s+(\w+)'
        froms = re.findall(from_pattern, sql, re.IGNORECASE)
        tables.extend(froms)
        
        # Match JOIN clauses
        join_pattern = r'JOIN\s+(\w+)'
        joins = re.findall(join_pattern, sql, re.IGNORECASE)
        tables.extend(joins)
        
        # Match INSERT INTO
        insert_pattern = r'INSERT\s+INTO\s+(\w+)'
        inserts = re.findall(insert_pattern, sql, re.IGNORECASE)
        tables.extend(inserts)
        
        # Match UPDATE
        update_pattern = r'UPDATE\s+(\w+)'
        updates = re.findall(update_pattern, sql, re.IGNORECASE)
        tables.extend(updates)
        
        # Match WITH clause CTEs
        with_pattern = r'WITH\s+(\w+)\s+AS'
        ctes = re.findall(with_pattern, sql, re.IGNORECASE)
        
        # Remove CTEs from tables list (they're not actual tables)
        tables = [t for t in tables if t not in ctes]
        
        # Remove duplicates but preserve order
        unique_tables = []
        for table in tables:
            if table not in unique_tables:
                unique_tables.append(table)
        
        return unique_tables
    
    def _find_source_tables(self, sql: str, all_tables: List[str]) -> List[str]:
        """Find source tables (tables being read from)."""
        sources = []
        
        # Tables in FROM and JOIN clauses are sources
        from_pattern = r'FROM\s+(\w+)'
        join_pattern = r'JOIN\s+(\w+)'
        
        from_matches = re.findall(from_pattern, sql, re.IGNORECASE)
        join_matches = re.findall(join_pattern, sql, re.IGNORECASE)
        
        sources.extend(from_matches)
        sources.extend(join_matches)
        
        # Remove duplicates
        return list(set(sources))
    
    def _find_target_tables(self, sql: str, all_tables: List[str]) -> List[str]:
        """Find target tables (tables being written to)."""
        targets = []
        
        # Tables in CREATE TABLE, INSERT INTO, UPDATE are targets
        create_pattern = r'CREATE\s+TABLE\s+(\w+)'
        insert_pattern = r'INSERT\s+INTO\s+(\w+)'
        update_pattern = r'UPDATE\s+(\w+)'
        
        create_matches = re.findall(create_pattern, sql, re.IGNORECASE)
        insert_matches = re.findall(insert_pattern, sql, re.IGNORECASE)
        update_matches = re.findall(update_pattern, sql, re.IGNORECASE)
        
        targets.extend(create_matches)
        targets.extend(insert_matches)
        targets.extend(update_matches)
        
        # Remove duplicates
        return list(set(targets))
    
    def _parse_config_files(self, config_files: List[str]):
        """Parse DAG configuration files."""
        logger.info("Parsing configuration files...")
        
        # Look for dbt schema files
        for file_path in config_files:
            if 'schema.yml' in file_path or 'schema.yaml' in file_path:
                rel_path = os.path.relpath(file_path, self.repo_path)
                
                try:
                    with open(file_path, 'r', encoding='utf-8') as f:
                        import yaml
                        config = yaml.safe_load(f)
                    
                    # Extract models (targets)
                    if 'models' in config:
                        for model in config['models']:
                            if 'name' in model:
                                self._add_dataset(model['name'], "target", rel_path)
                    
                    # Extract sources
                    if 'sources' in config:
                        for source in config['sources']:
                            if 'tables' in source:
                                for table in source['tables']:
                                    if 'name' in table:
                                        self._add_dataset(f"{source.get('name', 'raw')}.{table['name']}", 
                                                        "source", rel_path)
                
                except Exception as e:
                    logger.debug(f"Error parsing config {rel_path}: {e}")
        
        logger.info(f"Parsed {len(config_files)} config files")
    
    def _analyze_python_files(self, python_files: List[str]):
        """Analyze Python files for data operations."""
        logger.info("Analyzing Python files for data operations...")
        
        # Patterns for data operations
        data_patterns = {
            "read_csv": r'(?:pd\.)?read_csv\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "read_sql": r'(?:pd\.)?read_sql\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "read_parquet": r'(?:pd\.)?read_parquet\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "read_json": r'(?:pd\.)?read_json\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "to_csv": r'\.to_csv\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "to_parquet": r'\.to_parquet\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "to_sql": r'\.to_sql\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "to_json": r'\.to_json\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "spark_read": r'spark\.read\.\w+\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "spark_write": r'\.write\.\w+\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "open_read": r'open\s*\(\s*[\'"]([^\'"]+)[\'"]\s*,\s*[\'"]r[\'"]',
            "open_write": r'open\s*\(\s*[\'"]([^\'"]+)[\'"]\s*,\s*[\'"]w[\'"]'
        }
        
        for file_path in python_files:
            rel_path = os.path.relpath(file_path, self.repo_path)
            
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                
                sources = []
                targets = []
                
                # Find data operations
                for op_name, pattern in data_patterns.items():
                    for match in re.finditer(pattern, content, re.IGNORECASE):
                        dataset = match.group(1)
                        # Clean up the dataset name (remove quotes, etc.)
                        dataset = dataset.strip('\'"')
                        
                        if 'read' in op_name or 'spark_read' in op_name or 'open_read' in op_name:
                            if dataset not in sources:
                                sources.append(dataset)
                                self._add_dataset(dataset, "source", rel_path)
                        else:
                            if dataset not in targets:
                                targets.append(dataset)
                                self._add_dataset(dataset, "target", rel_path)
                
                # Create transformation if we found anything
                if sources or targets:
                    # Find all operations used
                    operations_found = []
                    for op_name in data_patterns.keys():
                        if re.search(data_patterns[op_name], content, re.IGNORECASE):
                            operations_found.append(op_name)
                    
                    transformation = {
                        "id": f"python:{rel_path}",
                        "file": rel_path,
                        "type": "python",
                        "sources": sources,
                        "targets": targets,
                        "operations": list(set(operations_found))
                    }
                    self.transformations.append(transformation)
                    
                    # Create edges
                    if sources and targets:
                        for source in sources:
                            for target in targets:
                                self.lineage_graph["edges"].append({
                                    "source": source,
                                    "target": target,
                                    "transformation": transformation["id"],
                                    "file": rel_path,
                                    "type": "python"
                                })
            
            except Exception as e:
                logger.debug(f"Error analyzing Python file {rel_path}: {e}")
        
        logger.info(f"Analyzed {len(python_files)} Python files")
    
    def _add_dataset(self, name: str, source_type: str, file_path: str):
        """Add a dataset to the lineage graph."""
        if not name or name.strip() == '':
            return
            
        if name not in self.datasets:
            self.datasets[name] = {
                "name": name,
                "type": "unknown",
                "first_seen": file_path,
                "references": []
            }
        
        # Check if this reference already exists
        ref_exists = False
        for ref in self.datasets[name]["references"]:
            if ref["file"] == file_path and ref["type"] == source_type:
                ref_exists = True
                break
        
        if not ref_exists:
            self.datasets[name]["references"].append({
                "file": file_path,
                "type": source_type
            })
        
        # Try to infer dataset type
        if name.endswith(('.csv', '.parquet', '.json', '.xml', '.txt')):
            self.datasets[name]["type"] = "file"
        elif '.' in name and not name.startswith(('.', '/')):
            # Could be schema.table or file with extension
            if name.split('.')[-1] in ['csv', 'parquet', 'json', 'xml', 'txt', 'sqlite']:
                self.datasets[name]["type"] = "file"
            else:
                self.datasets[name]["type"] = "table"
        elif '/' in name or '\\' in name:
            self.datasets[name]["type"] = "file_path"
        else:
            self.datasets[name]["type"] = "dataset"
    
    def _build_lineage_graph(self):
        """Build the unified lineage graph."""
        logger.info("Building unified lineage graph...")
        
        # Add all datasets as nodes
        self.lineage_graph["nodes"] = self.datasets
        
        # Remove duplicate edges
        unique_edges = []
        seen = set()
        
        for edge in self.lineage_graph["edges"]:
            key = f"{edge['source']}->{edge['target']}"
            if key not in seen:
                unique_edges.append(edge)
                seen.add(key)
        
        self.lineage_graph["edges"] = unique_edges
        
        logger.info(f"Lineage graph: {len(self.datasets)} datasets, {len(self.lineage_graph['edges'])} edges")
    
    def _populate_knowledge_graph(self):
        """Add lineage data to the knowledge graph."""
        logger.info("Populating knowledge graph with lineage data...")
        
        # Add dataset nodes
        for name, metadata in self.datasets.items():
            self.kg.add_dataset_node(
                name=name,
                storage_type=metadata.get("type", "unknown"),
                schema_snapshot={},  # Would need schema inference
                owner=None,
                is_source_of_truth=False,
                references=metadata.get("references", [])
            )
        
        # Add transformation nodes
        for trans in self.transformations:
            trans_id = self.kg.add_transformation_node(
                source_datasets=trans.get("sources", []),
                target_datasets=trans.get("targets", []),
                transformation_type=trans.get("type", "unknown"),
                source_file=trans.get("file", ""),
                sql_query=trans.get("sql", "")
            )
            
            # Add edges from transformation to datasets
            for source in trans.get("sources", []):
                self.kg.add_consumes_edge(trans_id, source)
            
            for target in trans.get("targets", []):
                self.kg.add_produces_edge(trans_id, target)
    
    def blast_radius(self, dataset_name: str) -> Dict[str, Any]:
        """
        Calculate blast radius for a dataset.
        
        Args:
            dataset_name: Name of the dataset to analyze
            
        Returns:
            Dictionary with downstream dependencies
        """
        logger.info(f"Calculating blast radius for dataset: {dataset_name}")
        
        if dataset_name not in self.datasets:
            return {"error": f"Dataset '{dataset_name}' not found"}
        
        # Build dependency graph
        graph = defaultdict(list)
        reverse_graph = defaultdict(list)
        
        for edge in self.lineage_graph["edges"]:
            graph[edge["source"]].append(edge["target"])
            reverse_graph[edge["target"]].append(edge["source"])
        
        # Find all downstream dependencies
        downstream = set()
        queue = deque([dataset_name])
        
        while queue:
            current = queue.popleft()
            for dep in graph.get(current, []):
                if dep not in downstream and dep != dataset_name:
                    downstream.add(dep)
                    queue.append(dep)
        
        return {
            "dataset": dataset_name,
            "direct_dependents": graph.get(dataset_name, []),
            "all_downstream": list(downstream),
            "impact_count": len(downstream)
        }
    
    def find_sources(self) -> List[str]:
        """Find all source datasets (no incoming edges)."""
        targets = set()
        for edge in self.lineage_graph["edges"]:
            targets.add(edge["target"])
        
        sources = [d for d in self.datasets.keys() if d not in targets]
        return sources
    
    def find_sinks(self) -> List[str]:
        """Find all sink datasets (no outgoing edges)."""
        sources = set()
        for edge in self.lineage_graph["edges"]:
            sources.add(edge["source"])
        
        sinks = [d for d in self.datasets.keys() if d not in sources]
        return sinks
    
    def get_lineage(self, dataset_name: str, direction: str = "both") -> Dict[str, Any]:
        """
        Get full lineage for a dataset.
        
        Args:
            dataset_name: Name of the dataset
            direction: 'upstream', 'downstream', or 'both'
            
        Returns:
            Dictionary with upstream and downstream dependencies
        """
        if dataset_name not in self.datasets:
            return {"error": f"Dataset '{dataset_name}' not found"}
        
        result = {
            "dataset": dataset_name,
            "metadata": self.datasets[dataset_name],
            "upstream": [],
            "downstream": []
        }
        
        # Build forward and reverse graphs
        forward = defaultdict(list)
        reverse = defaultdict(list)
        
        for edge in self.lineage_graph["edges"]:
            forward[edge["source"]].append({
                "target": edge["target"],
                "via": edge.get("file", "unknown"),
                "type": edge.get("type", "unknown")
            })
            reverse[edge["target"]].append({
                "source": edge["source"],
                "via": edge.get("file", "unknown"),
                "type": edge.get("type", "unknown")
            })
        
        # Find upstream (what produces this)
        if direction in ["upstream", "both"]:
            queue = deque([dataset_name])
            seen = set()
            
            while queue:
                current = queue.popleft()
                for dep_info in reverse.get(current, []):
                    dep = dep_info["source"]
                    if dep not in seen and dep != dataset_name:
                        seen.add(dep)
                        queue.append(dep)
                        result["upstream"].append({
                            "dataset": dep,
                            "via": [dep_info]
                        })
        
        # Find downstream (what consumes this)
        if direction in ["downstream", "both"]:
            queue = deque([dataset_name])
            seen = set()
            
            while queue:
                current = queue.popleft()
                for dep_info in forward.get(current, []):
                    dep = dep_info["target"]
                    if dep not in seen and dep != dataset_name:
                        seen.add(dep)
                        queue.append(dep)
                        result["downstream"].append({
                            "dataset": dep,
                            "via": [dep_info]
                        })
        
        return result
    
    def save_results(self, output_dir: str = ".cartography"):
        """
        Save hydrologist results to files.
        
        Args:
            output_dir: Directory to save results
        """
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        # Save knowledge graph
        self.kg.serialize_graph(output_path)
        
        # Save lineage graph
        lineage_path = output_path / "lineage_graph.json"
        with open(lineage_path, 'w') as f:
            json.dump({
                "datasets": self.datasets,
                "transformations": self.transformations,
                "edges": self.lineage_graph["edges"],
                "sources": self.find_sources(),
                "sinks": self.find_sinks()
            }, f, indent=2)
        
        logger.info(f"Saved lineage graph to {lineage_path}")
        
        # Save summary
        summary = {
            "repo_path": str(self.repo_path),
            "repo_name": self.repo_name,
            "analyzed_at": datetime.now().isoformat(),
            "datasets_found": len(self.datasets),
            "transformations_found": len(self.transformations),
            "edges_found": len(self.lineage_graph["edges"]),
            "sources": self.find_sources(),
            "sinks": self.find_sinks()
        }
        
        summary_path = output_path / "hydrologist_summary.json"
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Saved hydrologist summary to {summary_path}")


def main():
    """Command-line entry point."""
    parser = argparse.ArgumentParser(description="Hydrologist Agent - Data lineage analysis")
    parser.add_argument('--repo', type=str, required=True,
                       help='Path to repository to analyze')
    parser.add_argument('--output', type=str, default='.cartography',
                       help='Output directory (default: .cartography)')
    parser.add_argument('--dataset', type=str,
                       help='Get lineage for specific dataset')
    parser.add_argument('--blast', type=str,
                       help='Calculate blast radius for dataset')
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
    
    # Run hydrologist
    try:
        hydrologist = HydrologistAgent(args.repo)
        hydrologist.analyze()
        hydrologist.save_results(args.output)
        
        print(f"\n✅ Hydrologist analysis complete!")
        print(f"📊 Found {len(hydrologist.datasets)} datasets")
        print(f"🔄 Found {len(hydrologist.transformations)} transformations")
        print(f"📁 Results saved to {args.output}/")
        
        # Show sources and sinks
        sources = hydrologist.find_sources()
        sinks = hydrologist.find_sinks()
        
        print(f"\n📥 Source datasets (no inputs): {len(sources)}")
        if sources:
            print(f"  First 5: {sources[:5]}")
        
        print(f"\n📤 Sink datasets (no outputs): {len(sinks)}")
        if sinks:
            print(f"  First 5: {sinks[:5]}")
        
        # Handle specific queries
        if args.dataset:
            lineage = hydrologist.get_lineage(args.dataset)
            print(f"\n🔍 Lineage for '{args.dataset}':")
            print(f"  Upstream: {len(lineage.get('upstream', []))} datasets")
            print(f"  Downstream: {len(lineage.get('downstream', []))} datasets")
        
        if args.blast:
            radius = hydrologist.blast_radius(args.blast)
            print(f"\n💥 Blast radius for '{args.blast}':")
            print(f"  Impact count: {radius.get('impact_count', 0)} datasets")
            print(f"  Direct dependents: {radius.get('direct_dependents', [])}")
        
    except Exception as e:
        logger.error(f"Analysis failed: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()