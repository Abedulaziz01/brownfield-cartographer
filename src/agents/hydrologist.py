#!/usr/bin/env python3
"""Hydrologist Agent - Data lineage analysis for codebases."""

import re
import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any
import logging
from collections import defaultdict, deque

sys.path.insert(0, str(Path(__file__).parent.parent))

from graph.knowledge_graph import KnowledgeGraph
from analyzers.sql_lineage import SQLLineageAnalyzer
from analyzers.dag_config_parser import DAGConfigParser
from analyzers.tree_sitter_analyzer import TreeSitterAnalyzer
from analyzers.language_router import LanguageRouter

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HydrologistAgent:
    """Hydrologist Agent - Maps data lineage through the system."""

    def __init__(self, repo_path: str):
        self.repo_path = Path(repo_path).resolve()
        self.repo_name = self.repo_path.name

        self.sql_analyzer = SQLLineageAnalyzer(dialect="postgres")
        self.dag_parser = DAGConfigParser()
        self.tree_sitter = TreeSitterAnalyzer()
        self.language_router = LanguageRouter()

        self.kg = KnowledgeGraph(f"{self.repo_name}_lineage")
        self.lineage_graph = {"nodes": {}, "edges": [], "transformations": []}
        self.datasets = {}
        self.transformations = []

        logger.info(f"Hydrologist Agent initialized for {self.repo_path}")

    def analyze(self) -> KnowledgeGraph:
        logger.info(f"Starting analysis of {self.repo_path}")
        sql_files, config_files, python_files = [], [], []

        for root, dirs, files in os.walk(self.repo_path):
            dirs[:] = [d for d in dirs if d not in ['.git', '__pycache__', 'node_modules', 'venv', 'env']]
            for file in files:
                file_path = os.path.join(root, file)
                if file.endswith('.sql'):
                    sql_files.append(file_path)
                elif file.endswith(('.yml', '.yaml', '.json')):
                    config_files.append(file_path)
                elif file.endswith('.py'):
                    python_files.append(file_path)

        self._analyze_sql_files(sql_files)
        self._parse_config_files(config_files)
        self._analyze_python_files(python_files)
        self._build_lineage_graph()
        self._populate_knowledge_graph()

        logger.info(f"Analysis complete: {len(self.datasets)} datasets, {len(self.transformations)} transformations")
        return self.kg

    # ---------------- SQL Analysis ---------------- #
    def _analyze_sql_files(self, sql_files: List[str]):
        logger.info("Analyzing SQL files...")
        for file_path in sql_files:
            rel_path = os.path.relpath(file_path, self.repo_path)
            try:
                result = self.sql_analyzer.analyze_sql_file(file_path, str(self.repo_path))
            except Exception as e:
                logger.warning(f"Failed to analyze SQL file {rel_path}: {e}")
                continue

            if result.get("sources") or result.get("targets"):
                trans = {
                    "id": f"sql:{rel_path}",
                    "file": rel_path,
                    "type": "sql",
                    "sources": result.get("sources", []),
                    "targets": result.get("targets", []),
                    "intermediate": result.get("intermediate", []),
                    "dialect": self.sql_analyzer.dialect
                }
                self.transformations.append(trans)

                for source in trans["sources"]:
                    self._add_dataset(source, "source", rel_path)
                for target in trans["targets"]:
                    self._add_dataset(target, "target", rel_path)
                for source in trans["sources"]:
                    for target in trans["targets"]:
                        self.lineage_graph["edges"].append({
                            "source": source,
                            "target": target,
                            "transformation": trans["id"],
                            "file": rel_path,
                            "type": "sql"
                        })

    # ---------------- Config Analysis ---------------- #
    def _parse_config_files(self, config_files: List[str]):
        logger.info("Parsing config files...")
        pipeline_graph = self.dag_parser.build_pipeline_graph(config_files, str(self.repo_path))

        for dataset in pipeline_graph.get("datasets", []):
            self._add_dataset(dataset, "config", "multiple")

        for edge in pipeline_graph.get("edges", []):
            trans = {
                "id": f"config:{edge['file']}:{edge['from']}->{edge['to']}",
                "file": edge['file'],
                "type": "pipeline_dependency",
                "sources": [edge['from']],
                "targets": [edge['to']],
                "edge_type": edge.get('type', 'unknown')
            }
            self.transformations.append(trans)
            self.lineage_graph["edges"].append({
                "source": edge['from'],
                "target": edge['to'],
                "transformation": trans["id"],
                "file": edge['file'],
                "type": "pipeline"
            })

    # ---------------- Python Analysis ---------------- #
    def _analyze_python_files(self, python_files: List[str]):
        logger.info("Analyzing Python files...")
        patterns = {
            "read_csv": r'(?:pd\.)?read_csv\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "read_sql": r'(?:pd\.)?read_sql\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "read_parquet": r'(?:pd\.)?read_parquet\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "to_csv": r'\.to_csv\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "to_parquet": r'\.to_parquet\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "to_sql": r'\.to_sql\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "spark_read": r'spark\.read\.\w+\s*\(\s*[\'"]([^\'"]+)[\'"]',
            "spark_write": r'\.write\.\w+\s*\(\s*[\'"]([^\'"]+)[\'"]'
        }

        for file_path in python_files:
            rel_path = os.path.relpath(file_path, self.repo_path)
            try:
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()

                sources, targets = [], []
                for op, pat in patterns.items():
                    for match in re.finditer(pat, content):
                        dataset = match.group(1)
                        if 'read' in op:
                            sources.append(dataset)
                            self._add_dataset(dataset, "source", rel_path)
                        else:
                            targets.append(dataset)
                            self._add_dataset(dataset, "target", rel_path)

                if sources or targets:
                    trans = {
                        "id": f"python:{rel_path}",
                        "file": rel_path,
                        "type": "python",
                        "sources": sources,
                        "targets": targets,
                        "operations": list(set([op for op in patterns if op in content]))
                    }
                    self.transformations.append(trans)
                    for s in sources:
                        for t in targets:
                            self.lineage_graph["edges"].append({
                                "source": s,
                                "target": t,
                                "transformation": trans["id"],
                                "file": rel_path,
                                "type": "python"
                            })
            except Exception as e:
                logger.debug(f"Error analyzing {rel_path}: {e}")

    # ---------------- Dataset / Lineage ---------------- #
    def _add_dataset(self, name: str, source_type: str, file_path: str):
        if name not in self.datasets:
            self.datasets[name] = {"name": name, "type": "unknown", "first_seen": file_path, "references": []}
        self.datasets[name]["references"].append({"file": file_path, "type": source_type})

        if name.endswith(('.csv', '.parquet', '.json')):
            self.datasets[name]["type"] = "file"
        elif '.' in name:
            self.datasets[name]["type"] = "table"
        else:
            self.datasets[name]["type"] = "dataset"

    def _build_lineage_graph(self):
        self.lineage_graph["nodes"] = self.datasets
        unique_edges, seen = [], set()
        for edge in self.lineage_graph["edges"]:
            key = f"{edge['source']}->{edge['target']}"
            if key not in seen:
                unique_edges.append(edge)
                seen.add(key)
        self.lineage_graph["edges"] = unique_edges

    def _populate_knowledge_graph(self):
        for name, meta in self.datasets.items():
            self.kg.add_dataset_node(
                name=name,
                storage_type=meta.get("type", "unknown"),
                schema_snapshot={},
                owner=None,
                is_source_of_truth=False,
                references=meta.get("references", [])
            )
        for trans in self.transformations:
            trans_id = self.kg.add_transformation_node(
                source_datasets=trans.get("sources", []),
                target_datasets=trans.get("targets", []),
                transformation_type=trans.get("type", "unknown"),
                source_file=trans.get("file", ""),
                sql_query=trans.get("sql", "")
            )
            for s in trans.get("sources", []):
                self.kg.add_consumes_edge(trans_id, s)
            for t in trans.get("targets", []):
                self.kg.add_produces_edge(trans_id, t)

    # ---------------- Public Methods ---------------- #
    def find_sources(self) -> List[str]:
        targets = set(e['target'] for e in self.lineage_graph["edges"])
        return [d for d in self.datasets if d not in targets]

    def find_sinks(self) -> List[str]:
        sources = set(e['source'] for e in self.lineage_graph["edges"])
        return [d for d in self.datasets if d not in sources]

    def get_lineage(self, dataset_name: str, direction: str = "both") -> Dict[str, Any]:
        if dataset_name not in self.datasets:
            return {"error": f"Dataset '{dataset_name}' not found"}

        result = {"dataset": dataset_name, "metadata": self.datasets[dataset_name], "upstream": [], "downstream": []}
        forward, reverse = defaultdict(list), defaultdict(list)
        for edge in self.lineage_graph["edges"]:
            forward[edge["source"]].append(edge["target"])
            reverse[edge["target"]].append(edge["source"])

        if direction in ["upstream", "both"]:
            queue, seen = deque([dataset_name]), set()
            while queue:
                current = queue.popleft()
                for dep in reverse.get(current, []):
                    if dep not in seen:
                        seen.add(dep)
                        queue.append(dep)
                        result["upstream"].append({"dataset": dep, "via": [e for e in self.lineage_graph["edges"] if e["target"] == current and e["source"] == dep]})

        if direction in ["downstream", "both"]:
            queue, seen = deque([dataset_name]), set()
            while queue:
                current = queue.popleft()
                for dep in forward.get(current, []):
                    if dep not in seen:
                        seen.add(dep)
                        queue.append(dep)
                        result["downstream"].append({"dataset": dep, "via": [e for e in self.lineage_graph["edges"] if e["source"] == current and e["target"] == dep]})

        return result

    def blast_radius(self, dataset_name: str) -> Dict[str, Any]:
        if dataset_name not in self.datasets:
            return {"error": f"Dataset '{dataset_name}' not found"}

        graph = defaultdict(list)
        for e in self.lineage_graph["edges"]:
            graph[e["source"]].append(e["target"])

        downstream, queue = set(), deque([dataset_name])
        while queue:
            current = queue.popleft()
            for dep in graph.get(current, []):
                if dep not in downstream:
                    downstream.add(dep)
                    queue.append(dep)

        transformations = [e for e in self.lineage_graph["edges"] if e["source"] == dataset_name or e["source"] in downstream]

        return {"dataset": dataset_name, "direct_dependents": graph.get(dataset_name, []), "all_downstream": list(downstream), "transformations": transformations, "impact_count": len(downstream)}

    def save_results(self, output_dir: str = ".cartography"):
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)

        self.kg.serialize_graph(output_path)

        lineage_path = output_path / "lineage_graph.json"
        with open(lineage_path, 'w') as f:
            json.dump({"datasets": self.datasets, "transformations": self.transformations, "edges": self.lineage_graph["edges"], "sources": self.find_sources(), "sinks": self.find_sinks()}, f, indent=2)

        summary_path = output_path / "hydrologist_summary.json"
        summary = {"repo_path": str(self.repo_path), "repo_name": self.repo_name, "analyzed_at": datetime.now().isoformat(), "datasets_found": len(self.datasets), "transformations_found": len(self.transformations), "edges_found": len(self.lineage_graph["edges"]), "sources": self.find_sources(), "sinks": self.find_sinks()}
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)

        logger.info(f"Saved lineage graph and summary to {output_path}")

