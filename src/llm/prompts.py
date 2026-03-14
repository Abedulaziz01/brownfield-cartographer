# Prompt templates for LLM interactions

# -----------------------------
# System Prompts
# -----------------------------

SYSTEM_PROMPTS = {
    "purpose_statement": """You are an expert software engineer analyzing code.
Explain the purpose of the module in 2-3 sentences.
Focus on the business purpose, not implementation details.
""",

    "doc_drift": """You are a code quality analyst.
Compare the docstring with the actual code and identify differences.
""",

    "domain_clustering": """You are a software architect.
Group modules into business domains like Ingestion, Processing, API, Storage.
""",

    "day_one_questions": """You are a Forward Deployed Engineer analyzing a new codebase.
Answer the five discovery questions about the system.
"""
}


# -----------------------------
# User Prompt Templates
# -----------------------------

USER_PROMPTS = {
    "purpose_statement": """Analyze this code and explain its purpose.

File: {file_path}
Language: {language}

Code:
{code}

What is the main purpose of this module? Answer in 2-3 sentences.
""",

    "doc_drift": """Compare the docstring with the code.

File: {file_path}

DOCSTRING:
{docstring}

CODE:
{code}
"""
}


# -----------------------------
# Helper Functions
# -----------------------------

def format_prompt(prompt_name, **kwargs):
    if prompt_name not in USER_PROMPTS:
        raise ValueError("Unknown prompt")

    return USER_PROMPTS[prompt_name].format(**kwargs)


def get_system_prompt(prompt_name):
    if prompt_name not in SYSTEM_PROMPTS:
        raise ValueError("Unknown system prompt")

    return SYSTEM_PROMPTS[prompt_name]


# -----------------------------
# Test
# -----------------------------

def test_prompts():
    print("Testing prompts...")

    for p in SYSTEM_PROMPTS:
        print("System:", p)

    for p in USER_PROMPTS:
        print("User:", p)


if __name__ == "__main__":
    test_prompts()
    # In src/llm/prompts.py - REPLACE the purpose_statement prompt

"purpose_statement": """You are analyzing code to understand its true purpose.

IMPORTANT: DO NOT read or use any existing docstrings or comments.
Base your analysis ONLY on the actual code implementation.

Analyze this code and explain its purpose:

File: {file_path}
Language: {language}
Code:
```{language}
{code}