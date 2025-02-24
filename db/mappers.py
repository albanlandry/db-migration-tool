from typing import Dict, List, Optional, Any
from db_components import Table

class FieldMapper:
    """
    Maps fields from a source Table to a target Table.
    
    Attributes:
        source_table (Table): The source table object.
        target_table (Table): The target table object.
        mapping (Dict[str, str]): A dictionary mapping source field names to target field names.
    """
    def __init__(self, source_table: Table, target_table: Table, mapping: Optional[Dict[str, str]] = None) -> None:
        self.source_table: Table = source_table
        self.target_table: Table = target_table
        self.mapping: Dict[str, str] = mapping if mapping is not None else {}

    def add_mapping(self, source_field: str, target_field: str) -> None:
        """
        Add a mapping from a source field to a target field after verifying that
        both fields exist in their respective tables.

        :param source_field: Field name in the source table.
        :param target_field: Field name in the target table.
        :raises ValueError: If either the source field or target field does not exist.
        """
        if not self.source_table.get_column(source_field):
            raise ValueError(f"Source field '{source_field}' does not exist in table '{self.source_table.name}'.")
        if not self.target_table.get_column(target_field):
            raise ValueError(f"Target field '{target_field}' does not exist in table '{self.target_table.name}'.")
        self.mapping[source_field] = target_field

    def get_target_field(self, source_field: str) -> Optional[str]:
        """
        Retrieve the target field corresponding to the given source field.
        
        :param source_field: Field name in the source table.
        :return: The mapped target field name if exists; otherwise, None.
        """
        return self.mapping.get(source_field)

    def map_record(self, source_record: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert a record from the source table format to the target table format
        using the defined field mappings. Only fields present in the mapping are included.

        :param source_record: A dictionary representing a record from the source table.
        :return: A new dictionary representing the record mapped to the target table's fields.
        """
        mapped_record: Dict[str, Any] = {}
        for src_field, value in source_record.items():
            tgt_field = self.get_target_field(src_field)
            if tgt_field is not None:
                mapped_record[tgt_field] = value
        return mapped_record

    def __str__(self) -> str:
        return (f"Field mapping from table '{self.source_table.name}' to table '{self.target_table.name}': "
                f"{self.mapping}")

    def __repr__(self) -> str:
        return (f"FieldMapper(source_table={self.source_table!r}, "
                f"target_table={self.target_table!r}, mapping={self.mapping!r})")
