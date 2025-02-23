import mysql.connector
import json

# Database connection settings
DB_CONFIG = {
    "host": "127.0.0.1",  # e.g., "localhost"
    "user": "tester",
    "password": "tester@12345",
    "database": "testdb",
    "port": 32768
}

OUTPUT_FILE = "mysql_schema.json"  # Output file name

def get_schema():
    try:
        # Connect to the database
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor(dictionary=True)

        # Get list of tables
        cursor.execute("SHOW TABLES;")
        tables = [row[f"Tables_in_{DB_CONFIG['database']}"] for row in cursor.fetchall()]

        schema = {}

        for table in tables:
            cursor.execute(f"DESCRIBE `{table}`;")
            columns = cursor.fetchall()

            schema[table] = []
            for column in columns:
                schema[table].append({
                    "Field": column["Field"],
                    "Type": column["Type"],
                    "Null": column["Null"],
                    "Key": column["Key"],
                    "Default": column["Default"],
                    "Extra": column["Extra"],
                })

        # Write schema to JSON file
        with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
            json.dump(schema, f, indent=4)

        print(f"Schema exported to {OUTPUT_FILE}")

        cursor.close()
        conn.close()
    except mysql.connector.Error as err:
        print(f"Error: {err}")

if __name__ == "__main__":
    get_schema()
