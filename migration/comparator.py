from typing import Dict, List, Optional, Any
from db.db_components import Schema

class SchemaComparator:
    """Compares two database schemas and generates transformation operations."""

    def __init__(self, source_schema: Schema, target_schema: Schema):
        self.source_schema = source_schema
        self.target_schema = target_schema

    def compare_schemas(self) -> Dict[str, List[str]]:
        """Compares schemas and returns necessary operations."""
        operations = {
            "add_tables": [],
            "drop_tables": [],
            "add_columns": [],
            "drop_columns": [],
            "modify_columns": [],
        }

        # Find missing tables
        for table_name, source_table in self.source_schema.tables.items():
            if table_name not in self.target_schema.tables:
                operations["add_tables"].append(table_name)

        # Find extra tables
        for table_name in self.target_schema.tables.keys():
            if table_name not in self.source_schema.tables:
                operations["drop_tables"].append(table_name)

        # Compare existing tables
        for table_name, source_table in self.source_schema.tables.items():
            if table_name in self.target_schema.tables:
                target_table = self.target_schema.tables[table_name]

                # Compare columns
                for column_name, source_column in source_table.columns.items():
                    if column_name not in target_table.columns:
                        operations["add_columns"].append(
                            f"ALTER TABLE {table_name} ADD COLUMN {column_name} {source_column.col_type};"
                        )
                    elif target_table.columns[column_name].col_type != source_column.col_type:
                        operations["modify_columns"].append(
                            f"ALTER TABLE {table_name} MODIFY COLUMN {column_name} {source_column.col_type};"
                        )

                # Find extra columns in target
                for column_name in target_table.columns.keys():
                    if column_name not in source_table.columns:
                        operations["drop_columns"].append(
                            f"ALTER TABLE {table_name} DROP COLUMN {column_name};"
                        )

        return operations

    def generate_sql_migration(self) -> List[str]:
        """Generates SQL statements to transform the target schema into the source schema."""
        operations = self.compare_schemas()
        sql_statements = []

        for table in operations["add_tables"]:
            columns = ", ".join(
                f"{col.name} {col.col_type}" for col in self.source_schema.tables[table].columns.values()
            )
            sql_statements.append(f"CREATE TABLE {table} ({columns});")

        for table in operations["drop_tables"]:
            sql_statements.append(f"DROP TABLE {table};")

        sql_statements.extend(operations["add_columns"])
        sql_statements.extend(operations["drop_columns"])
        sql_statements.extend(operations["modify_columns"])

        return sql_statements