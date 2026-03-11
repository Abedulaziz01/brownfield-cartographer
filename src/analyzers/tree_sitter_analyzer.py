"""Tree-sitter AST analyzer for extracting code structure."""

import os
import re
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple, Set
import logging

from .language_router import LanguageRouter

logger = logging.getLogger(__name__)

class TreeSitterAnalyzer:
    """Analyzes code files using tree-sitter AST parsing."""
    
    def __init__(self):
        """Initialize the analyzer with language router."""
        self.router = LanguageRouter()
        
        # Query patterns for different languages - ENHANCED for better import detection
        self.queries = self._init_queries()
    
    def _init_queries(self) -> Dict[str, Dict[str, str]]:
        """Initialize tree-sitter query strings for each language."""
        return {
            "python": {
                "imports": """
                    ;; Simple import: import x
                    (import_statement
                        name: (dotted_name) @import) @import_stmt
                    
                    ;; Import with alias: import x as y
                    (import_statement
                        name: (dotted_name) @import
                        alias: (identifier) @alias) @import_alias
                    
                    ;; From import: from x import y
                    (import_from_statement
                        module_name: (dotted_name) @from
                        name: (dotted_name) @import) @from_import
                    
                    ;; From import with multiple: from x import a, b, c
                    (import_from_statement
                        module_name: (dotted_name) @from
                        (dotted_name) @import) @from_import_multi
                    
                    ;; Relative import: from .module import x
                    (import_from_statement
                        module_name: (relative_import
                            (dotted_name)?) @relative_from
                        name: (dotted_name) @import) @relative_import
                """,
                "functions": """
                    (function_definition
                        name: (identifier) @function.name
                        parameters: (parameters) @function.params
                        body: (block) @function.body) @function.def
                """,
                "classes": """
                    (class_definition
                        name: (identifier) @class.name
                        body: (block) @class.body) @class.def
                """,
                "decorators": """
                    (decorated_definition
                        decorator: (decorator) @decorator
                        definition: _ @decorated) @decorator.def
                """,
                "calls": """
                    (call
                        function: (identifier) @call.func) @call.simple
                    
                    (call
                        function: (attribute
                            object: (identifier) @call.obj
                            attribute: (identifier) @call.method)) @call.method_call
                """
            },
            "sql": {
                "tables": """
                    (select
                        (from
                            (table_expression
                                (table_name) @table))) @select_stmt
                    
                    (insert
                        (table_name) @table) @insert_stmt
                    
                    (update
                        (table_name) @table) @update_stmt
                    
                    (delete
                        (table_name) @table) @delete_stmt
                    
                    (create_table
                        name: (table_name) @table) @create_table
                    
                    (cte
                        name: (identifier) @cte.name
                        expression: (select) @cte.select) @cte_def
                """,
                "schemas": """
                    (create_schema
                        name: (identifier) @schema) @create_schema
                """
            },
            "yaml": {
                "references": """
                    (block_mapping_pair
                        key: (flow_node) @key
                        value: (flow_node) @value) @mapping
                """
            },
            "javascript": {
                "imports": """
                    (import_statement
                        source: (string) @source
                        (import_clause) @imports) @import
                    
                    (import
                        source: (string) @source) @dynamic_import
                    
                    (export_statement
                        source: (string) @source) @export
                """
            }
        }
    
    def analyze_file(self, file_path: str, repo_root: str = "") -> Dict[str, Any]:
        """
        Analyze a single file and extract structure.
        
        Args:
            file_path: Path to the file
            repo_root: Root directory of the repository
            
        Returns:
            Dictionary with extracted information
        """
        rel_path = os.path.relpath(file_path, repo_root) if repo_root else file_path
        result = {
            "path": rel_path,
            "language": self.router.get_language(file_path),
            "imports": [],
            "functions": [],
            "classes": [],
            "tables": [],
            "calls": [],
            "errors": [],
            "raw_imports": []  # Store raw import strings for debugging
        }
        
        # Check if file exists
        if not os.path.exists(file_path):
            result["errors"].append("File does not exist")
            return result
        
        # Read file content
        try:
            with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
            result["size"] = len(content)
            result["lines"] = len(content.splitlines())
        except Exception as e:
            result["errors"].append(f"Error reading file: {e}")
            return result
        
        # Try tree-sitter parsing
        if self.router.can_parse(file_path):
            ast_result = self._parse_with_treesitter(file_path, content)
            result.update(ast_result)
        else:
            # Fallback to regex parsing
            fallback_result = self._parse_with_fallback(file_path, content)
            result.update(fallback_result)
        
        # Always try regex fallback for imports if tree-sitter found none
        if not result.get("imports") and not result.get("raw_imports"):
            regex_imports = self._extract_imports_regex(content, result["language"])
            if regex_imports:
                result["imports"] = regex_imports
                result["raw_imports"] = regex_imports
                result["parse_method"] = "regex-fallback"
        
        return result
    
    def _parse_with_treesitter(self, file_path: str, content: str) -> Dict[str, Any]:
        """Parse file using tree-sitter."""
        result = {
            "imports": [],
            "functions": [],
            "classes": [],
            "tables": [],
            "calls": [],
            "raw_imports": [],
            "parse_method": "tree-sitter"
        }
        
        parser = self.router.get_parser(file_path)
        if not parser:
            return result
        
        try:
            # Parse the file
            tree = parser.parse(bytes(content, "utf8"))
            
            # Get language
            lang = self.router.get_language(file_path)
            
            # Apply language-specific queries
            if lang in self.queries:
                queries = self.queries[lang]
                
                # Extract imports - ENHANCED
                if "imports" in queries:
                    imports = self._extract_with_query(
                        tree, queries["imports"], content, 
                        ["import", "from", "alias", "source", "relative_from"]
                    )
                    result["raw_imports"] = imports
                    
                    # Process imports based on language
                    if lang == "python":
                        result["imports"] = self._process_python_imports(imports)
                    elif lang == "javascript":
                        result["imports"] = self._process_js_imports(imports)
                    else:
                        result["imports"] = self._process_generic_imports(imports)
                
                # Extract functions
                if "functions" in queries:
                    functions = self._extract_with_query(
                        tree, queries["functions"], content, 
                        ["function.name", "function.params"]
                    )
                    result["functions"] = self._format_functions(functions)
                
                # Extract classes
                if "classes" in queries:
                    classes = self._extract_with_query(
                        tree, queries["classes"], content, ["class.name"]
                    )
                    result["classes"] = [c.get("class.name", "") for c in classes]
                
                # Extract tables (for SQL)
                if "tables" in queries:
                    tables = self._extract_with_query(
                        tree, queries["tables"], content, ["table"]
                    )
                    result["tables"] = list(set([t.get("table", "") for t in tables]))
                
                # Extract function calls
                if "calls" in queries:
                    calls = self._extract_with_query(
                        tree, queries["calls"], content, 
                        ["call.func", "call.obj", "call.method"]
                    )
                    result["calls"] = calls
        
        except Exception as e:
            logger.warning(f"Tree-sitter parsing failed for {file_path}: {e}")
            result["errors"] = [str(e)]
        
        return result
    
    def _process_python_imports(self, imports: List[Dict]) -> List[str]:
        """Process Python imports into module paths."""
        processed = set()
        
        for imp in imports:
            # Simple import: import x
            if "import" in imp and "from" not in imp and "relative_from" not in imp:
                module = imp["import"]
                # Handle multiple imports in one line: import a, b, c
                if ',' in module:
                    parts = [p.strip() for p in module.split(',')]
                    for part in parts:
                        processed.add(part)
                else:
                    processed.add(module)
            
            # From import: from x import y
            elif "from" in imp and "import" in imp:
                from_module = imp["from"]
                import_name = imp["import"]
                
                # Handle "from x import y" -> x.y
                if from_module and import_name:
                    # If import_name has dots, it's a submodule
                    if '.' in import_name:
                        processed.add(f"{from_module}.{import_name}")
                    else:
                        processed.add(f"{from_module}.{import_name}")
                    
                    # Also add the base module
                    processed.add(from_module)
            
            # Relative import: from .x import y
            elif "relative_from" in imp and "import" in imp:
                rel_module = imp["relative_from"]
                import_name = imp.get("import", "")
                
                # Convert relative path to absolute (simplified)
                if rel_module.startswith('.'):
                    # Will be resolved later by path resolution
                    processed.add(f"relative:{rel_module}.{import_name}")
                else:
                    processed.add(rel_module)
            
            # Import with alias
            if "alias" in imp:
                # The actual module is still the import name
                pass
        
        return list(processed)
    
    def _process_js_imports(self, imports: List[Dict]) -> List[str]:
        """Process JavaScript imports."""
        processed = set()
        
        for imp in imports:
            if "source" in imp:
                # Remove quotes from import source
                source = imp["source"].strip('\'"')
                processed.add(source)
        
        return list(processed)
    
    def _process_generic_imports(self, imports: List[Dict]) -> List[str]:
        """Process generic imports."""
        processed = set()
        
        for imp in imports:
            for key, value in imp.items():
                if isinstance(value, str) and value:
                    processed.add(value)
        
        return list(processed)
    
    def _extract_imports_regex(self, content: str, language: str) -> List[str]:
        """Fallback regex import extraction."""
        imports = set()
        
        if language == "python":
            # Python import patterns
            patterns = [
                r'^import\s+(\w+(?:\.\w+)*)',
                r'^from\s+(\w+(?:\.\w+)*)\s+import',
                r'^from\s+(\.+[\w.]*)\s+import'
            ]
            
            for line in content.split('\n'):
                line = line.strip()
                if line.startswith('import ') or line.startswith('from '):
                    for pattern in patterns:
                        match = re.search(pattern, line)
                        if match:
                            module = match.group(1)
                            imports.add(module)
        
        elif language == "sql":
            # SQL table references
            patterns = [
                r'from\s+(\w+)',
                r'join\s+(\w+)',
                r'into\s+(\w+)',
                r'table\s+(\w+)'
            ]
            for pattern in patterns:
                for match in re.finditer(pattern, content, re.IGNORECASE):
                    imports.add(match.group(1))
        
        return list(imports)
    
    def _extract_with_query(self, tree, query_str: str, content: str, capture_names: List[str]) -> List[Dict]:
        """Extract data using a tree-sitter query."""
        try:
            from tree_sitter import Query, Node
            
            query = Query(self.router.LANGUAGE_LIB, query_str)
            captures = query.captures(tree.root_node)
            
            results = []
            current_match = {}
            
            # Process captures in order
            for node, capture_name in captures:
                if capture_name in capture_names:
                    # Get the text of this node
                    if isinstance(node, Node):
                        text = content[node.start_byte:node.end_byte]
                        current_match[capture_name] = text.strip()
                        
                        # Also capture line number
                        if 'line' not in current_match:
                            start_line = content[:node.start_byte].count('\n') + 1
                            current_match['line'] = start_line
                    
                    # If we have all captures we care about, save
                    if set(capture_names).issubset(set(current_match.keys())):
                        results.append(current_match.copy())
                        current_match = {}
            
            # Add any remaining match
            if current_match:
                results.append(current_match)
            
            return results
        except Exception as e:
            logger.debug(f"Query error: {e}")
            return []
    
    def _parse_with_fallback(self, file_path: str, content: str) -> Dict[str, Any]:
        """Fallback parsing using regex for when tree-sitter isn't available."""
        result = {
            "imports": [],
            "functions": [],
            "classes": [],
            "tables": [],
            "parse_method": "regex-fallback"
        }
        
        lang = self.router.get_language(file_path)
        lines = content.split('\n')
        
        if lang == "python":
            in_function = False
            in_class = False
            
            for i, line in enumerate(lines):
                line = line.strip()
                
                # Skip comments and empty lines
                if not line or line.startswith('#'):
                    continue
                
                # Find imports
                if line.startswith('import ') or line.startswith('from '):
                    import_parts = line.split()
                    if import_parts[0] == 'import':
                        result["imports"].append(import_parts[1].split(',')[0].strip())
                    elif import_parts[0] == 'from':
                        result["imports"].append(import_parts[1])
                
                # Find functions
                if line.startswith('def '):
                    func_name = line[4:].split('(')[0].strip()
                    result["functions"].append({
                        "name": func_name,
                        "line": i + 1
                    })
                
                # Find classes
                if line.startswith('class '):
                    class_name = line[6:].split('(')[0].split(':')[0].strip()
                    result["classes"].append(class_name)
        
        elif lang == "sql":
            # Find table references
            table_pattern = r'(?:from|join|into|update|table)\s+([`"\']?[\w.]+[`"\']?)'
            for match in re.finditer(table_pattern, content, re.IGNORECASE):
                table = match.group(1).strip('`"\'')
                if table and table not in result["tables"]:
                    result["tables"].append(table)
                    result["imports"].append(table)  # Treat tables as imports for lineage
        
        return result
    
    def _format_functions(self, functions: List[Dict]) -> List[Dict]:
        """Format function information."""
        formatted = []
        for func in functions:
            formatted.append({
                "name": func.get("function.name", ""),
                "params": func.get("function.params", ""),
                "line": func.get("line", 0)
            })
        return formatted
    
    def analyze_repository(self, repo_path: str) -> Dict[str, Any]:
        """
        Analyze all files in a repository.
        
        Args:
            repo_path: Path to repository root
            
        Returns:
            Dictionary mapping file paths to their analysis results
        """
        results = {}
        repo_path = Path(repo_path)
        
        # Count by language
        lang_counts = {}
        
        # Walk through all files
        for root, dirs, files in os.walk(repo_path):
            # Skip common ignored directories
            dirs[:] = [d for d in dirs if d not in 
                      ['.git', '__pycache__', 'node_modules', 'venv', 'env', '.venv', '.env', 'build', 'dist']]
            
            for file in files:
                file_path = os.path.join(root, file)
                
                # Only analyze supported files
                lang = self.router.get_language(file_path)
                if lang:
                    rel_path = os.path.relpath(file_path, repo_path)
                    logger.debug(f"Analyzing {rel_path}")
                    
                    try:
                        result = self.analyze_file(file_path, str(repo_path))
                        results[rel_path] = result
                        
                        # Count languages
                        lang_counts[lang] = lang_counts.get(lang, 0) + 1
                        
                        # Log progress periodically
                        if len(results) % 100 == 0:
                            logger.info(f"Analyzed {len(results)} files...")
                            
                    except Exception as e:
                        logger.warning(f"Error analyzing {rel_path}: {e}")
        
        logger.info(f"Analysis complete: {len(results)} files analyzed")
        logger.info(f"Language breakdown: {lang_counts}")
        
        return results