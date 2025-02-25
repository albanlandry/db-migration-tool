from typing import Dict, List, Any, Optional, Union
import mysql.connector
from mysql.connector.connection import MySQLConnection
import pymongo
import pymysql
from pymongo.mongo_client import MongoClient

class DatabaseConnection:
    """Represents a single database connection."""

    def __init__(self, db_type: str, connection: Union[pymysql.connections.Connection, pymongo.MongoClient]):
        self.db_type = db_type
        self.connection = connection

    def close(self):
        """Closes the database connection."""
        if self.db_type == "mysql":
            self.connection.close()
        elif self.db_type == "mongodb":
            self.connection.close()

class DatabaseFactory:
    """Factory class to manage multiple database connections."""

    connections: Dict[str, DatabaseConnection] = {}

    @classmethod
    def create_mysql_connection(cls, name: str, host: str, user: str, password: str, database: str, port: int = 3306):
        """Creates a MySQL connection and stores it."""
        connection = pymysql.connect(host=host, user=user, password=password, database=database, port=port)
        cls.connections[name] = DatabaseConnection("mysql", connection)

    @classmethod
    def create_mongodb_connection(cls, name: str, uri: str, database: str):
        """Creates a MongoDB connection and stores it."""
        client = pymongo.MongoClient(uri)
        cls.connections[name] = DatabaseConnection("mongodb", client[database])

    @classmethod
    def get_connection(cls, name: str) -> Optional[DatabaseConnection]:
        """Retrieves a stored database connection."""
        return cls.connections.get(name)

    @classmethod
    def close_connection(cls, name: str):
        """Closes a specific database connection."""
        if name in cls.connections:
            cls.connections[name].close()
            del cls.connections[name]

    @classmethod
    def close_all_connections(cls):
        """Closes all active database connections."""
        for name in list(cls.connections.keys()):
            cls.connections[name].close()
            del cls.connections[name]
 
# --- Database Connection Factory ---
'''
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
'''