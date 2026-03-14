"""Python Data Flow Analyzer - Detects pandas, PySpark, SQLAlchemy operations."""

import ast
import logging
from typing import Dict, List, Set, Any, Optional
from pathlib import Path

logger = logging.getLogger(__name__)

class PythonDataFlowAnalyzer:
    """Analyzes Python files for data operations (pandas, PySpark, SQLAlchemy)."""
    
    def __init__(self):
        self.data_patterns = {
            'pandas': {
                'read': ['read_csv', 'read_excel', 'read_json', 'read_sql', 'read_parquet'],
                'write': ['to_csv', 'to_excel', 'to_json', 'to_sql', 'to_parquet']
            },
            'pyspark': {
                'read': ['spark.read.csv', 'spark.read.json', 'spark.read.parquet', 'spark.read.table'],
                'write': ['write.csv', 'write.json', 'write.parquet', 'write.saveAsTable']
            },
            'sqlalchemy': {
                'read': ['pd.read_sql', 'read_sql_query', 'engine.execute'],
                'write': ['to_sql', 'df.write']
            }
        }
        
        self.dynamic_refs = []  # Track unresolved dynamic references
    
    def analyze_file(self, file_path: str, repo_root: str = "") -> Dict[str, Any]:
        """Analyze a Python file for data operations."""
        rel_path = Path(file_path).relative_to(repo_root) if repo_root else file_path
        
        result = {
            "file": str(rel_path),
            "sources": [],
            "targets": [],
            "transformations": [],
            "dynamic_refs": [],
            "line_numbers": {}
        }
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
            
            tree = ast.parse(content)
            
            # Walk the AST
            for node in ast.walk(tree):
                # Look for function calls
                if isinstance(node, ast.Call):
                    self._analyze_call(node, result)
                
                # Look for assignments (DataFrame operations)
                elif isinstance(node, ast.Assign):
                    self._analyze_assign(node, result)
                    
        except Exception as e:
            logger.warning(f"Error analyzing {file_path}: {e}")
            result["error"] = str(e)
        
        return result
    
    def _analyze_call(self, node: ast.Call, result: Dict):
        """Analyze a function call node."""
        if isinstance(node.func, ast.Attribute):
            # Handle method calls like df.to_csv()
            method_name = node.func.attr
            
            # Check pandas writes
            if method_name in self.data_patterns['pandas']['write']:
                for arg in node.args:
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                        result["targets"].append({
                            "path": arg.value,
                            "type": "pandas_write",
                            "line": node.lineno
                        })
                    else:
                        # Dynamic reference - can't resolve
                        result["dynamic_refs"].append({
                            "type": "pandas_write",
                            "line": node.lineno,
                            "note": "Dynamic file path - cannot resolve"
                        })
            
            # Check Spark operations
            elif 'write' in method_name and 'spark' in str(node.func).lower():
                result["transformations"].append({
                    "type": "spark_write",
                    "line": node.lineno
                })
        
        elif isinstance(node.func, ast.Name):
            # Handle function calls like pd.read_csv()
            func_name = node.func.id
            
            # Check pandas reads
            if func_name in self.data_patterns['pandas']['read']:
                for arg in node.args:
                    if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
                        result["sources"].append({
                            "path": arg.value,
                            "type": "pandas_read",
                            "line": node.lineno
                        })
                    else:
                        result["dynamic_refs"].append({
                            "type": "pandas_read",
                            "line": node.lineno,
                            "note": "Dynamic file path"
                        })
    
    def _analyze_assign(self, node: ast.Assign, result: Dict):
        """Analyze assignment nodes for DataFrame operations."""
        if isinstance(node.value, ast.Call):
            if isinstance(node.value.func, ast.Attribute):
                if 'spark' in str(node.value.func.value).lower():
                    if 'read' in node.value.func.attr:
                        result["transformations"].append({
                            "type": "spark_read",
                            "line": node.lineno
                        })
    
    def get_summary(self, results: Dict) -> Dict:
        """Get summary of data flow analysis."""
        return {
            "total_sources": len(results.get("sources", [])),
            "total_targets": len(results.get("targets", [])),
            "dynamic_refs": len(results.get("dynamic_refs", [])),
            "transformations": len(results.get("transformations", []))
        }