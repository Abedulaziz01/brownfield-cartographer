import sqlglot
from sqlglot import exp


class SQLLineageAnalyzer:
    def __init__(self, dialect: str = "postgres"):
        self.dialect = dialect

    def _extract_table_name(self, table_node: exp.Table) -> str:
        """
        Extract clean table name (no schema duplication or alias).
        """
        if table_node.this:
            return table_node.this.name.lower()
        return ""

    def analyze_sql_file(self, content: str) -> dict:
        """
        Analyze SQL and extract lineage info.
        """
        result = {
            "sources": set(),
            "targets": set(),
            "intermediate": set()
        }

        statements = sqlglot.parse(content, dialect=self.dialect)

        for stmt in statements:
            if not stmt:
                continue

            stmt_targets = set()
            stmt_ctes = set()

            # 1️⃣ Capture CTEs
            for cte in stmt.find_all(exp.CTE):
                name = cte.alias.lower()
                stmt_ctes.add(name)
                result["intermediate"].add(name)

            # 2️⃣ Identify target tables
            if isinstance(stmt, (exp.Create, exp.Insert, exp.Update, exp.Delete, exp.Merge)):

                table = stmt.find(exp.Table)
                if table:
                    name = self._extract_table_name(table)
                    if name:
                        stmt_targets.add(name)
                        result["targets"].add(name)

            # 3️⃣ Collect sources
            for table in stmt.find_all(exp.Table):

                name = self._extract_table_name(table)

                if (
                    name
                    and name not in stmt_ctes
                    and name not in stmt_targets
                ):
                    result["sources"].add(name)

        return result


def run_tests():
    analyzer = SQLLineageAnalyzer()

    test_cases = [
        (
            "Simple SELECT",
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id",
            {'users', 'orders'},
            set()
        ),

        (
            "CREATE TABLE AS",
            "CREATE TABLE active_users AS SELECT * FROM users WHERE status = 'active'",
            {'users'},
            {'active_users'}
        ),

        (
            "INSERT INTO",
            "INSERT INTO daily_metrics SELECT * FROM events",
            {'events'},
            {'daily_metrics'}
        ),

        (
            "WITH CTE",
            """
            WITH user_orders AS (
                SELECT * FROM orders
            )
            SELECT * FROM users
            JOIN user_orders ON users.id = user_orders.id
            """,
            {'users', 'orders'},
            set(),
            {'user_orders'}
        ),

        (
            "UPDATE",
            "UPDATE products SET price = 10",
            set(),
            {'products'}
        ),

        (
            "Multiple statements",
            """
            CREATE TABLE temp AS SELECT * FROM raw_data;
            INSERT INTO processed SELECT * FROM temp;
            DROP TABLE temp;
            """,
            {'raw_data', 'temp'},
            {'processed', 'temp'}
        )
    ]

    for i, (name, sql, exp_sources, exp_targets, *exp_intermediate) in enumerate(test_cases, 1):

        res = analyzer.analyze_sql_file(sql)
        exp_intermediate = exp_intermediate[0] if exp_intermediate else set()

        print(f"\n  Test {i}: {name}")

        # Sources
        if res['sources'] == exp_sources:
            print(f"    ✅ Sources: {res['sources']}")
        else:
            print(f"    ❌ Sources: expected {exp_sources}, got {res['sources']}")

        # Targets
        if res['targets'] == exp_targets:
            print(f"    ✅ Targets: {res['targets']}")
        else:
            print(f"    ❌ Targets: expected {exp_targets}, got {res['targets']}")

        # Intermediate (only when expected)
        if exp_intermediate:
            if res['intermediate'] == exp_intermediate:
                print(f"    ✅ Intermediate: {res['intermediate']}")
            else:
                print(f"    ❌ Intermediate: expected {exp_intermediate}, got {res['intermediate']}")


if __name__ == "__main__":
    run_tests()
    # Add to existing sql_lineage.py - DIALECT SUPPORT

# At the top, add these dialects
SUPPORTED_DIALECTS = {
    "postgres": sqlglot.dialects.Postgres,
    "bigquery": sqlglot.dialects.BigQuery,
    "snowflake": sqlglot.dialects.Snowflake,
    "duckdb": sqlglot.dialects.DuckDB,
    "mysql": sqlglot.dialects.MySQL,
    "redshift": sqlglot.dialects.Redshift,
    "spark": sqlglot.dialects.Spark
}

def parse_sql_with_dialect_detection(self, sql_content: str, file_path: str) -> Dict:
    """Auto-detect and parse SQL with multiple dialect support."""
    results = {"sources": [], "targets": [], "dialect_used": "unknown"}
    
    # Try each dialect until one works
    for dialect_name, dialect_class in self.SUPPORTED_DIALECTS.items():
        try:
            parsed = sqlglot.parse_one(sql_content, dialect=dialect_name)
            if parsed:
                # Extract tables
                tables = parsed.find_all(exp.Table)
                for table in tables:
                    table_name = table.name
                    if self._is_source_table(parsed, table):
                        results["sources"].append(table_name)
                    else:
                        results["targets"].append(table_name)
                
                # Extract CTEs
                ctes = parsed.find_all(exp.CTE)
                for cte in ctes:
                    results["intermediate"].append(cte.alias)
                
                results["dialect_used"] = dialect_name
                break
        except:
            continue
    
    return results