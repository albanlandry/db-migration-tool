from typing import Dict, List, Any, Optional, Union
import mysql.connector
from mysql.connector.connection import MySQLConnection
import pymongo
from pymongo.mongo_client import MongoClient

# --- Database Connection Factory ---

class DatabaseConnectionFactory:
    """
    Factory class to handle database connection creation for both MySQL and MongoDB.
    The configuration dictionary must include a 'db_type' key with a value of either 'mysql' or 'mongodb'.
    """
    def __init__(self, config: Dict[str, Any]) -> None:
        """
        Initialize the connection factory with a configuration dictionary.
        For MySQL, expected keys include: 'db_type', 'host', 'user', 'password', and 'database'.
        For MongoDB, expected keys include: 'db_type' and 'uri'. Optionally, a 'database' key.
        :param config: Dictionary containing database connection parameters.
        """
        self.config: Dict[str, Any] = config

    def get_connection(self) -> Union[MySQLConnection, MongoClient]:
        """
        Establish and return a new database connection.
        :return: A MySQLConnection object if db_type is 'mysql', or a MongoClient object if db_type is 'mongodb'.
        :raises ValueError: If an unsupported db_type is provided.
        """
        db_type: str = self.config.get("db_type", "mysql").lower()
        if db_type == "mysql":
            return mysql.connector.connect(
                host=self.config.get("host", "localhost"),
                user=self.config.get("user"),
                password=self.config.get("password"),
                database=self.config.get("database")
            )
        elif db_type == "mongodb":
            uri: Optional[str] = self.config.get("uri")
            if not uri:
                raise ValueError("MongoDB configuration requires a 'uri' key.")
            return pymongo.MongoClient(uri)
        else:
            raise ValueError(f"Unsupported db_type '{db_type}'. Supported types: 'mysql', 'mongodb'.")
