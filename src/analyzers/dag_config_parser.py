"""DAG Configuration Parser for Airflow, dbt, and other pipeline definitions."""

import os
import yaml
import json
from pathlib import Path
from typing import List, Dict, Set, Optional, Any, Tuple
import logging
import re

logger = logging.getLogger(__name__)


class DAGConfigParser:
    """
    Parses DAG configuration files (Airflow, dbt, etc.) to extract pipeline topology.
    
    Handles:
    - Airflow DAG Python files
    - dbt schema.yml files
    - dbt_project.yml
    - General YAML/JSON configs
    """
    
    def __init__(self):
        """Initialize the DAG config parser."""
        self.supported_formats = ['.py', '.yml', '.yaml', '.json']
    
    def parse_file(self, file_path: str, repo_root: str = "") -> Dict[str, Any]:
        """
        Parse a configuration file and extract DAG information.
        
        Args:
            file_path: Path to config file
            repo_root: Root directory of repository
            
        Returns:
            Dictionary with DAG information
        """
        rel_path = os.path.relpath(file_path, repo_root) if repo_root else file_path
        ext = os.path.splitext(file_path)[1].lower()
        
        result = {
            "file": rel_path,
            "type": "unknown",
            "dags": [],
            "tasks": [],
            "dependencies": [],
            "datasets": [],
            "errors": []
        }
        
        if not os.path.exists(file_path):
            result["errors"].append("File does not exist")
            return result
        
        try:
            if ext in ['.yml', '.yaml']:
                return self._parse_yaml(file_path, rel_path)
            elif ext == '.json':
                return self._parse_json(file_path, rel_path)
            elif ext == '.py':
                return self._parse_python_dag(file_path, rel_path)
            else:
                result["errors"].append(f"Unsupported file type: {ext}")
        except Exception as e:
            logger.warning(f"Error parsing {rel_path}: {e}")
            result["errors"].append(str(e))
        
        return result
    
    def _parse_yaml(self, file_path: str, rel_path: str) -> Dict[str, Any]:
        """Parse YAML configuration files."""
        result = {
            "file": rel_path,
            "type": "yaml",
            "dags": [],
            "tasks": [],
            "dependencies": [],
            "datasets": [],
            "errors": []
        }
        
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                data = yaml.safe_load(f)
            except yaml.YAMLError as e:
                result["errors"].append(f"YAML parsing error: {e}")
                return result
        
        if not data:
            return result
        
        # Check if it's a dbt schema file
        if 'models' in data or 'sources' in data:
            return self._parse_dbt_schema(data, rel_path)
        
        # Check if it's a dbt_project.yml
        if 'name' in data and 'profile' in data and 'models' in data:
            return self._parse_dbt_project(data, rel_path)
        
        # Generic YAML - look for DAG-like structures
        return self._parse_generic_config(data, rel_path)
    
    def _parse_json(self, file_path: str, rel_path: str) -> Dict[str, Any]:
        """Parse JSON configuration files."""
        result = {
            "file": rel_path,
            "type": "json",
            "dags": [],
            "tasks": [],
            "dependencies": [],
            "datasets": [],
            "errors": []
        }
        
        with open(file_path, 'r', encoding='utf-8') as f:
            try:
                data = json.load(f)
            except json.JSONDecodeError as e:
                result["errors"].append(f"JSON parsing error: {e}")
                return result
        
        return self._parse_generic_config(data, rel_path)
    
    def _parse_dbt_schema(self, data: Dict, rel_path: str) -> Dict[str, Any]:
        """Parse dbt schema.yml files."""
        result = {
            "file": rel_path,
            "type": "dbt_schema",
            "dags": [],
            "tasks": [],
            "dependencies": [],
            "datasets": [],
            "errors": []
        }
        
        # Extract models (these are transformations)
        if 'models' in data:
            for model in data['models']:
                model_name = model.get('name', 'unknown')
                result['tasks'].append({
                    'name': model_name,
                    'type': 'dbt_model',
                    'file': rel_path
                })
                
                # Extract dependencies from refs in description (basic)
                description = model.get('description', '')
                refs = re.findall(r"{{ ref\('([^']+)'\) }}", description)
                for ref in refs:
                    result['dependencies'].append({
                        'from': model_name,
                        'to': ref,
                        'type': 'ref'
                    })
                
                # Add as dataset
                result['datasets'].append({
                    'name': model_name,
                    'type': 'model',
                    'file': rel_path
                })
        
        # Extract sources (these are input datasets)
        if 'sources' in data:
            for source in data['sources']:
                source_name = source.get('name', 'unknown')
                for table in source.get('tables', []):
                    table_name = table.get('name', 'unknown')
                    full_name = f"{source_name}.{table_name}"
                    result['datasets'].append({
                        'name': full_name,
                        'type': 'source',
                        'file': rel_path
                    })
        
        return result
    
    def _parse_dbt_project(self, data: Dict, rel_path: str) -> Dict[str, Any]:
        """Parse dbt_project.yml files."""
        result = {
            "file": rel_path,
            "type": "dbt_project",
            "dags": [{
                'name': data.get('name', 'unknown'),
                'profile': data.get('profile', 'unknown')
            }],
            "tasks": [],
            "dependencies": [],
            "datasets": [],
            "errors": []
        }
        
        # Extract model paths
        if 'model-paths' in data:
            result['model_paths'] = data['model-paths']
        
        return result
    
    def _parse_python_dag(self, file_path: str, rel_path: str) -> Dict[str, Any]:
        """
        Parse Python files that might contain Airflow DAGs.
        
        This is a simplified parser - a production version would use AST.
        """
        result = {
            "file": rel_path,
            "type": "python_dag",
            "dags": [],
            "tasks": [],
            "dependencies": [],
            "datasets": [],
            "errors": []
        }
        
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Look for DAG definitions
        dag_pattern = r'DAG\s*\(\s*[\'"]([^\'"]+)[\'"]'
        for match in re.finditer(dag_pattern, content):
            dag_name = match.group(1)
            result['dags'].append({
                'name': dag_name,
                'file': rel_path
            })
        
        # Look for operators (tasks)
        operator_pattern = r'(\w+)\s*=\s*(\w+Operator)\s*\('
        for match in re.finditer(operator_pattern, content):
            task_name = match.group(1)
            operator_type = match.group(2)
            result['tasks'].append({
                'name': task_name,
                'type': operator_type,
                'file': rel_path
            })
        
        # Look for dependencies (>>, <<, set_upstream, set_downstream)
        dep_patterns = [
            r'(\w+)\s*>>\s*(\w+)',
            r'(\w+)\s*<<\s*(\w+)',
            r'(\w+)\.set_upstream\(\s*(\w+)\s*\)',
            r'(\w+)\.set_downstream\(\s*(\w+)\s*\)'
        ]
        
        for pattern in dep_patterns:
            for match in re.finditer(pattern, content):
                from_task = match.group(1)
                to_task = match.group(2)
                result['dependencies'].append({
                    'from': from_task,
                    'to': to_task,
                    'type': 'airflow'
                })
        
        # Look for data references (S3, tables, etc.)
        data_patterns = [
            r'(?:s3|gs|wasb)://[^\s\'"]+',
            r'(?:table|dataset|view)\s*=\s*[\'"]([^\'"]+)[\'"]',
            r'SELECT.*FROM\s+(\w+)',
        ]
        
        for pattern in data_patterns:
            for match in re.finditer(pattern, content, re.IGNORECASE):
                if match.groups():
                    result['datasets'].append({
                        'name': match.group(1),
                        'type': 'reference',
                        'file': rel_path
                    })
        
        return result
    
    def _parse_generic_config(self, data: Any, rel_path: str) -> Dict[str, Any]:
        """Parse generic configuration looking for DAG-like structures."""
        result = {
            "file": rel_path,
            "type": "generic",
            "dags": [],
            "tasks": [],
            "dependencies": [],
            "datasets": [],
            "errors": []
        }
        
        if isinstance(data, dict):
            # Look for common DAG patterns
            for key, value in data.items():
                if key.lower() in ['dag', 'dags', 'pipeline', 'pipelines']:
                    if isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict) and 'name' in item:
                                result['dags'].append({
                                    'name': item['name'],
                                    'config': item
                                })
                
                if key.lower() in ['tasks', 'steps', 'nodes']:
                    if isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict) and 'name' in item:
                                result['tasks'].append({
                                    'name': item['name'],
                                    'config': item
                                })
                
                if key.lower() in ['edges', 'dependencies', 'links']:
                    if isinstance(value, list):
                        for item in value:
                            if isinstance(item, dict):
                                if 'source' in item and 'target' in item:
                                    result['dependencies'].append({
                                        'from': item['source'],
                                        'to': item['target'],
                                        'type': 'config'
                                    })
        
        return result
    
    def build_pipeline_graph(self, config_files: List[str], repo_root: str = "") -> Dict[str, Any]:
        """
        Build a pipeline graph from multiple config files.
        
        Args:
            config_files: List of paths to config files
            repo_root: Root directory of repository
            
        Returns:
            Dictionary with nodes (pipelines/tasks) and edges (dependencies)
        """
        graph = {
            "pipelines": set(),
            "tasks": set(),
            "datasets": set(),
            "edges": [],
            "files": []
        }
        
        all_dags = []
        all_tasks = []
        all_deps = []
        
        for file_path in config_files:
            analysis = self.parse_file(file_path, repo_root)
            graph["files"].append(analysis)
            
            # Collect DAGs
            for dag in analysis.get("dags", []):
                dag_id = f"{analysis['file']}::{dag.get('name', 'unknown')}"
                graph["pipelines"].add(dag_id)
                all_dags.append(dag)
            
            # Collect tasks
            for task in analysis.get("tasks", []):
                task_id = f"{analysis['file']}::{task.get('name', 'unknown')}"
                graph["tasks"].add(task_id)
                all_tasks.append(task)
            
            # Collect dependencies
            for dep in analysis.get("dependencies", []):
                graph["edges"].append({
                    "from": f"{analysis['file']}::{dep.get('from', 'unknown')}",
                    "to": f"{analysis['file']}::{dep.get('to', 'unknown')}",
                    "type": dep.get('type', 'unknown'),
                    "file": analysis['file']
                })
                all_deps.append(dep)
            
            # Collect datasets
            for dataset in analysis.get("datasets", []):
                dataset_id = dataset.get('name', 'unknown')
                graph["datasets"].add(dataset_id)
        
        # Convert sets to lists for JSON
        graph["pipelines"] = list(graph["pipelines"])
        graph["tasks"] = list(graph["tasks"])
        graph["datasets"] = list(graph["datasets"])
        
        return graph


# Test function
def test_dag_config_parser():
    """Test the DAG Config Parser."""
    print("🧪 Testing DAG Config Parser...")
    
    parser = DAGConfigParser()
    
    import tempfile
    import os
    
    # Test 1: dbt schema.yml
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f:
        f.write("""
version: 2

models:
  - name: stg_customers
    description: "Staged customer data"
    columns:
      - name: customer_id
        tests:
          - unique
          - not_null
  
  - name: stg_orders
    description: "Staged orders data"
    columns:
      - name: order_id
        tests:
          - unique
          - not_null

sources:
  - name: raw
    tables:
      - name: customers
      - name: orders
      - name: payments
""")
        test_file = f.name
    
    try:
        result = parser.parse_file(test_file)
        print(f"\n  dbt schema.yml:")
        print(f"    Type: {result['type']}")
        print(f"    Tasks: {[t['name'] for t in result.get('tasks', [])]}")
        print(f"    Datasets: {[d['name'] for d in result.get('datasets', [])]}")
    finally:
        os.unlink(test_file)
    
    # Test 2: Airflow DAG Python
    with tempfile.NamedTemporaryFile(mode='w', suffix='.py', delete=False) as f:
        f.write("""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from datetime import datetime

default_args = {
    'owner': 'data_team',
    'start_date': datetime(2024, 1, 1)
}

dag = DAG(
    'etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily'
)

start = DummyOperator(
    task_id='start',
    dag=dag
)

extract = PythonOperator(
    task_id='extract_data',
    python_callable=lambda: print("Extracting"),
    dag=dag
)

transform = PythonOperator(
    task_id='transform_data',
    python_callable=lambda: print("Transforming"),
    dag=dag
)

load = PythonOperator(
    task_id='load_data',
    python_callable=lambda: print("Loading"),
    dag=dag
)

start >> extract >> transform >> load
""")
        test_file = f.name
    
    try:
        result = parser.parse_file(test_file)
        print(f"\n  Airflow DAG:")
        print(f"    Type: {result['type']}")
        print(f"    DAGs: {[d['name'] for d in result.get('dags', [])]}")
        print(f"    Tasks: {[t['name'] for t in result.get('tasks', [])]}")
        print(f"    Dependencies: {len(result.get('dependencies', []))}")
    finally:
        os.unlink(test_file)
    
    # Test 3: Build pipeline graph
    print(f"\n  Building pipeline graph...")
    test_files = []
    with tempfile.NamedTemporaryFile(mode='w', suffix='.yml', delete=False) as f1:
        f1.write("name: test_project\nprofile: test\nmodels:\n  - name: model1")
        test_files.append(f1.name)
    
    try:
        graph = parser.build_pipeline_graph(test_files)
        print(f"    Pipelines: {len(graph['pipelines'])}")
        print(f"    Tasks: {len(graph['tasks'])}")
        print(f"    Datasets: {len(graph['datasets'])}")
        print(f"    Edges: {len(graph['edges'])}")
    finally:
        for f in test_files:
            os.unlink(f)
    
    print("\n✅ All DAG config parser tests passed!")


if __name__ == "__main__":
    test_dag_config_parser()


    # Add to existing dag_config_parser.py

def parse_airflow_dag(self, file_path: str) -> Dict[str, Any]:
    """Parse Airflow DAG Python file."""
    result = {
        "type": "airflow",
        "dags": [],
        "tasks": [],
        "dependencies": []
    }
    
    with open(file_path, 'r') as f:
        content = f.read()
    
    # Find DAG definitions
    import re
    dag_pattern = r'DAG\s*\(\s*[\'"]([^\'"]+)[\'"]'
    for match in re.finditer(dag_pattern, content):
        result["dags"].append({
            "name": match.group(1),
            "file": file_path
        })
    
    # Find operators (tasks)
    task_pattern = r'(\w+)\s*=\s*(\w+Operator)\s*\('
    for match in re.finditer(task_pattern, content):
        result["tasks"].append({
            "name": match.group(1),
            "type": match.group(2)
        })
    
    # Find dependencies (>> operator)
    dep_pattern = r'(\w+)\s*>>\s*(\w+)'
    for match in re.finditer(dep_pattern, content):
        result["dependencies"].append({
            "from": match.group(1),
            "to": match.group(2)
        })
    
    return result

def parse_dbt_schema(self, file_path: str) -> Dict[str, Any]:
    """Parse dbt schema.yml file."""
    import yaml
    
    result = {
        "type": "dbt",
        "models": [],
        "sources": [],
        "dependencies": []
    }
    
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
    
    # Extract models
    for model in data.get('models', []):
        model_name = model.get('name')
        result["models"].append({
            "name": model_name,
            "description": model.get('description', ''),
            "columns": model.get('columns', [])
        })
        
        # Look for ref() in description or tests
        description = model.get('description', '')
        import re
        refs = re.findall(r"ref\(['\"]([^'\"]+)['\"]\)", description)
        for ref in refs:
            result["dependencies"].append({
                "from": model_name,
                "to": ref,
                "type": "ref"
            })
    
    # Extract sources
    for source in data.get('sources', []):
        source_name = source.get('name')
        for table in source.get('tables', []):
            result["sources"].append({
                "name": f"{source_name}.{table.get('name')}",
                "source": source_name
            })
    
    return result