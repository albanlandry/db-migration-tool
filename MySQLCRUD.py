import mysql.connector
import json

class MySQLCRUD:
    def __init__(self, dbconfig):
        """Initialize connection and load schema."""
        self.conn = mysql.connector.connect(**dbconfig)
        self.cursor = self.conn.cursor(dictionary=True)
        self.load_schema()

    def load_schema(self, schema):
        """Load schema from JSON file."""
        with open(schema, "r", encoding="utf-8") as f:
            self.schema = json.load(f)

    def create(self, table, data):
        """
        Insert a new record into a table.
        :param table: Table name
        :param data: Dictionary of column names and values
        """
        if table not in self.schema:
            raise ValueError(f"Table '{table}' does not exist.")

        columns = ", ".join(data.keys())
        placeholders = ", ".join(["%s"] * len(data))
        values = tuple(data.values())

        query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        self.cursor.execute(query, values)
        self.conn.commit()

    def read(self, table, filters=None):
        """
        Read records from a table with optional filters.
        :param table: Table name
        :param filters: Dictionary of column names and values for filtering (optional)
        :return: List of records
        """
        if table not in self.schema:
            raise ValueError(f"Table '{table}' does not exist.")

        query = f"SELECT * FROM {table}"
        values = []

        if filters:
            conditions = " AND ".join(f"{key}=%s" for key in filters.keys())
            query += f" WHERE {conditions}"
            values = list(filters.values())

        self.cursor.execute(query, values)
        return self.cursor.fetchall()

    def update(self, table, data, filters):
        """
        Update records in a table based on filters.
        :param table: Table name
        :param data: Dictionary of column names and new values
        :param filters: Dictionary of column names and values for filtering
        """
        if table not in self.schema:
            raise ValueError(f"Table '{table}' does not exist.")

        if not filters:
            raise ValueError("Filters are required for update.")

        set_clause = ", ".join(f"{key}=%s" for key in data.keys())
        where_clause = " AND ".join(f"{key}=%s" for key in filters.keys())

        values = list(data.values()) + list(filters.values())

        query = f"UPDATE {table} SET {set_clause} WHERE {where_clause}"
        self.cursor.execute(query, values)
        self.conn.commit()

    def delete(self, table, filters):
        """
        Delete records from a table based on filters.
        :param table: Table name
        :param filters: Dictionary of column names and values for filtering
        """
        if table not in self.schema:
            raise ValueError(f"Table '{table}' does not exist.")

        if not filters:
            raise ValueError("Filters are required for deletion.")

        where_clause = " AND ".join(f"{key}=%s" for key in filters.keys())
        values = list(filters.values())

        query = f"DELETE FROM {table} WHERE {where_clause}"
        self.cursor.execute(query, values)
        self.conn.commit()

    def delete_multiple(self, table, column, values):
        """
        Delete multiple rows where a column matches any value in the given list.
        :param table: Table name
        :param column: Column name
        :param values: List of values to match
        """
        if table not in self.schema:
            raise ValueError(f"Table '{table}' does not exist.")

        placeholders = ", ".join(["%s"] * len(values))
        query = f"DELETE FROM {table} WHERE {column} IN ({placeholders})"
        self.cursor.execute(query, values)
        self.conn.commit()

    def clear_table(self, table):
        """
        Delete all records from a table.
        :param table: Table name
        """
        if table not in self.schema:
            raise ValueError(f"Table '{table}' does not exist.")

        query = f"DELETE FROM {table}"
        self.cursor.execute(query)
        self.conn.commit()

    def close(self):
        """Close database connection."""
        self.cursor.close()
        self.conn.close()
