from typing import List, Dict, Any
from db.mappers import FieldMapper

class DataMigrator:
    """Handles migrating data from a source table to a target table using field mapping."""
    
    def __init__(self, connection):
        """
        :param connection: The MySQL connection object.
        """
        self.conn = connection

    def migrate_data(self, mapper: FieldMapper, data: List[Dict[str, Any]]):
        """
        Migrates data from the source table to the target table.
        If a row with the same ID exists, it updates the row instead.
        
        :param mapper: A FieldMapper object defining the source-target mapping.
        :param data: A list of dictionaries representing the source table's rows.
        """
        if not data:
            print("No data to migrate.")
            return

        target_fields = list(mapper.field_map.values())
        source_fields = list(mapper.field_map.keys())

        placeholders = ", ".join(["%s"] * len(target_fields))
        update_clause = ", ".join([f"{field} = VALUES({field})" for field in target_fields])

        query = f"""
        INSERT INTO {mapper.target_table} ({", ".join(target_fields)})
        VALUES ({placeholders})
        ON DUPLICATE KEY UPDATE {update_clause};
        """

        values = [
            tuple(row[source_field] for source_field in source_fields)
            for row in data
        ]

        try:
            with self.conn.cursor() as cursor:
                cursor.executemany(query, values)
            self.conn.commit()
            print(f"Successfully migrated {len(values)} records.")
        except Exception as e:
            self.conn.rollback()
            print(f"Error migrating data: {e}")