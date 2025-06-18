"""
    Databricks SQL connector logic to capture the schema from Unity Catalog
"""
from databricks import sql

def set_connection(server_hostname: str, http_path: str, access_token: str):
    """
        Establishes a connection to a Databricks SQL warehouse.
    """
    connection = sql.connect(
        server_hostname=server_hostname,
        http_path=http_path,
        access_token=access_token
    )
    return connection

def list_schemas(catalog_name, connection):
    cursor = connection.cursor()
    cursor.execute(f"SHOW SCHEMAS IN `{catalog_name}`")
    schemas = [row[0] for row in cursor.fetchall()]
    cursor.close()
    return schemas

def get_catalog_metadata(catalog_name, schema_names, connection):
    """
    Retrieves metadata for one or more schemas.
    """
    cursor = connection.cursor()
    result_str = f"Schemas and tables in catalog: {catalog_name}\n"
    for schema_name in schema_names:
        result_str += f"\nSchema: {schema_name}\n"
        cursor.execute(f"SHOW TABLES IN `{catalog_name}`.`{schema_name}`")
        tables = cursor.fetchall()
        for table in tables:
            table_name = table.tableName
            result_str += f"  Table: {table_name}\n"
            cursor.execute(
                f"""
                SELECT column_name, data_type, comment
                FROM `{catalog_name}`.information_schema.columns
                WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
                ORDER BY ordinal_position
                """
            )
            columns = cursor.fetchall()
            for col in columns:
                col_name = col.column_name
                data_type = col.data_type
                comment = col.comment or ""
                result_str += f"    - {col_name} ({data_type}) — {comment}\n"
    cursor.close()
    connection.close()
    return result_str
