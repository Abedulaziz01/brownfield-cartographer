"""Language Router - Maps file extensions to appropriate parsers."""

import os
from pathlib import Path
from typing import Optional, Dict, Any
import logging

logger = logging.getLogger(__name__)

class LanguageRouter:
    """Routes files to appropriate language parsers based on extension."""
    
    # Extension to language mapping
    EXTENSION_MAP = {
        '.py': 'python',
        '.pyi': 'python',  # Python interface files
        '.sql': 'sql',
        '.yml': 'yaml',
        '.yaml': 'yaml',
        '.js': 'javascript',
        '.jsx': 'javascript',
        '.ipynb': 'notebook',  # Jupyter notebooks
    }
    
    # Languages we can parse with tree-sitter
    PARSABLE_LANGUAGES = {'python', 'sql', 'yaml', 'javascript'}
    
    def __init__(self):
        """Initialize the language router."""
        self.tree_sitter_available = False
        self.parsers = {}
        
        # Try to import tree-sitter
        try:
            from tree_sitter import Language, Parser
            self.tree_sitter_available = True
            self.Parser = Parser
            self.Language = Language
            
            # Try to load the compiled grammars
            self._load_grammars()
        except ImportError:
            logger.warning("tree-sitter not installed. Will use fallback methods.")
    
    def _load_grammars(self):
        """Load compiled tree-sitter grammars."""
        # Look for grammars in common locations
        possible_paths = [
            Path(__file__).parent.parent.parent / "scripts" / "build" / "languages.so",
            Path(__file__).parent.parent / "scripts" / "build" / "languages.so",
            Path.cwd() / "scripts" / "build" / "languages.so",
        ]
        
        grammar_path = None
        for path in possible_paths:
            if path.exists():
                grammar_path = path
                break
        
        if grammar_path:
            try:
                self.LANGUAGE_LIB = self.Language(str(grammar_path))
                
                # Create parsers for each language
                for lang in self.PARSABLE_LANGUAGES:
                    try:
                        parser = self.Parser()
                        parser.set_language(self.LANGUAGE_LIB.language(lang))
                        self.parsers[lang] = parser
                        logger.debug(f"Loaded parser for {lang}")
                    except Exception as e:
                        logger.warning(f"Could not load parser for {lang}: {e}")
                
                logger.info(f"Loaded tree-sitter grammars from {grammar_path}")
            except Exception as e:
                logger.warning(f"Could not load grammar library: {e}")
        else:
            logger.warning("No tree-sitter grammars found. Run scripts/download_grammars.py")
    
    def get_language(self, file_path: str) -> Optional[str]:
        """
        Determine the language of a file based on its extension.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Language name or None if unknown
        """
        ext = os.path.splitext(file_path)[1].lower()
        return self.EXTENSION_MAP.get(ext)
    
    def can_parse(self, file_path: str) -> bool:
        """
        Check if we can parse this file with tree-sitter.
        
        Args:
            file_path: Path to the file
            
        Returns:
            True if we have a parser for this language
        """
        lang = self.get_language(file_path)
        return lang in self.parsers if lang else False
    
    def get_parser(self, file_path: str):
        """
        Get the appropriate parser for a file.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Parser object or None if not available
        """
        lang = self.get_language(file_path)
        return self.parsers.get(lang) if lang else None
    
    def get_file_category(self, file_path: str) -> str:
        """
        Categorize file by its role in the codebase.
        
        Args:
            file_path: Path to the file
            
        Returns:
            Category: 'code', 'config', 'data', 'docs', or 'unknown'
        """
        ext = os.path.splitext(file_path)[1].lower()
        
        # Code files
        if ext in ['.py', '.js', '.jsx', '.ts', '.java', '.scala']:
            return 'code'
        
        # Config files
        if ext in ['.yml', '.yaml', '.json', '.toml', '.ini', '.cfg', '.conf']:
            return 'config'
        
        # Data files
        if ext in ['.sql', '.csv', '.parquet', '.jsonl']:
            return 'data'
        
        # Documentation
        if ext in ['.md', '.rst', '.txt', '.ipynb']:
            return 'docs'
        
        return 'unknown'
    
    def get_all_supported_extensions(self) -> list:
        """Get list of all supported file extensions."""
        return list(self.EXTENSION_MAP.keys())
    
    def __str__(self) -> str:
        """String representation."""
        return f"LanguageRouter(supported={list(self.EXTENSION_MAP.keys())})"


# Test function
def test_language_router():
    """Test the Language Router."""
    print("🧪 Testing Language Router...")
    
    router = LanguageRouter()
    
    # Test extension mapping
    test_files = [
        ("main.py", "python"),
        ("script.pyi", "python"),
        ("query.sql", "sql"),
        ("config.yml", "yaml"),
        ("config.yaml", "yaml"),
        ("app.js", "javascript"),
        ("component.jsx", "javascript"),
        ("notebook.ipynb", "notebook"),
        ("unknown.xyz", None),
    ]
    
    for file_path, expected_lang in test_files:
        lang = router.get_language(file_path)
        assert lang == expected_lang, f"{file_path}: expected {expected_lang}, got {lang}"
        print(f"  ✅ {file_path} -> {lang}")
    
    # Test categorization
    test_categories = [
        ("main.py", "code"),
        ("config.yml", "config"),
        ("data.sql", "data"),
        ("README.md", "docs"),
        ("script.js", "code"),
    ]
    
    for file_path, expected_category in test_categories:
        category = router.get_file_category(file_path)
        assert category == expected_category, f"{file_path}: expected {expected_category}, got {category}"
        print(f"  ✅ {file_path} category -> {category}")
    
    # Test parseability
    print(f"\n  Tree-sitter available: {router.tree_sitter_available}")
    print(f"  Parsers loaded: {list(router.parsers.keys())}")
    
    print("\n✅ All language router tests passed!")


if __name__ == "__main__":
    test_language_router()