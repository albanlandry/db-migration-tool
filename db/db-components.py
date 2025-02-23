import json

class Column:
    """
    Represents a column in a table.
    """
    def __init__(self, field, col_type, nullable, key, default, extra):
        self.field = field          # Column name
        self.col_type = col_type    # Data type and size (e.g., 'int(11)', 'varchar(255)')
        self.nullable = nullable    # 'YES' or 'NO'
        self.key = key              # Key info (e.g., 'PRI' for primary key)
        self.default = default      # Default value if any
        self.extra = extra          # Extra info (e.g., 'auto_increment')

    def __str__(self):
        return f"{self.field} ({self.col_type})"

    def __repr__(self):
        return (f"Column(field={self.field!r}, col_type={self.col_type!r}, "
                f"nullable={self.nullable!r}, key={self.key!r}, "
                f"default={self.default!r}, extra={self.extra!r})")


class Table:
    """
    Represents a table within a schema.
    """
    def __init__(self, name, columns=None):
        self.name = name                   # Table name
        self.columns = columns or []       # List of Column objects

    def add_column(self, column):
        """Add a Column object to the table."""
        self.columns.append(column)

    def get_column(self, column_name):
        """Retrieve a column by its name."""
        for col in self.columns:
            if col.field == column_name:
                return col
        return None

    def __str__(self):
        cols = ", ".join(str(col) for col in self.columns)
        return f"Table '{self.name}': [{cols}]"

    def __repr__(self):
        return f"Table(name={self.name!r}, columns={self.columns!r})"


class Schema:
    """
    Represents a database schema that contains multiple tables.
    """
    def __init__(self, name, tables=None):
        self.name = name                       # Schema name
        self.tables = tables or {}             # Dictionary: table name -> Table object

    def add_table(self, table):
        """Add a Table object to the schema."""
        self.tables[table.name] = table

    def get_table(self, table_name):
        """Retrieve a table by its name."""
        return self.tables.get(table_name)

    def __str__(self):
        tables_str = "\n  ".join(str(table) for table in self.tables.values())
        return f"Schema '{self.name}':\n  {tables_str}"

    def __repr__(self):
        return f"Schema(name={self.name!r}, tables={list(self.tables.keys())!r})"


class Database:
    """
    Represents a database that can contain one or more schemas.
    For MySQL, a database is equivalent to a schema.
    """
    def __init__(self, name, schemas=None):
        self.name = name                       # Database name
        self.schemas = schemas or {}           # Dictionary: schema name -> Schema object

    def add_schema(self, schema):
        """Add a Schema object to the database."""
        self.schemas[schema.name] = schema

    def get_schema(self, schema_name):
        """Retrieve a schema by its name."""
        return self.schemas.get(schema_name)

    def load_from_json(self, json_file):
        """
        Load the database schema from a JSON file. 
        The JSON file should have the following structure:
        
            {
                "table_name": [
                    {
                        "Field": "id",
                        "Type": "int(11)",
                        "Null": "NO",
                        "Key": "PRI",
                        "Default": null,
                        "Extra": "auto_increment"
                    },
                    ...
                ],
                ...
            }
        
        This method creates a default schema (named the same as the database) and populates it.
        """
        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        default_schema = Schema(self.name)
        for table_name, columns in data.items():
            table = Table(table_name)
            for col in columns:
                column = Column(
                    field=col["Field"],
                    col_type=col["Type"],
                    nullable=col["Null"],
                    key=col["Key"],
                    default=col["Default"],
                    extra=col["Extra"]
                )
                table.add_column(column)
            default_schema.add_table(table)

        self.add_schema(default_schema)

    def __str__(self):
        schemas_str = "\n".join(str(schema) for schema in self.schemas.values())
        return f"Database '{self.name}':\n{schemas_str}"

    def __repr__(self):
        return f"Database(name={self.name!r}, schemas={list(self.schemas.keys())!r})"


# --- Usage Example ---
# if __name__ == "__main__":
#     # Assume 'mysql_schema.json' is the file from the previous export.
#     db = Database("my_database")
#     db.load_from_json("mysql_schema.json")
    
#     # Print the entire database structure
#     print(db)
    
#     # Access a specific table and its columns
#     schema = db.get_schema("my_database")
#     if schema:
#         table = schema.get_table("users")
#         if table:
#             print(f"\nColumns in table '{table.name}':")
#             for column in table.columns:
#                 print(f"  - {column}")
