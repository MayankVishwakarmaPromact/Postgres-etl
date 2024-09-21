import logging
from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta, timezone
import pandas as pd
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine
import os
import subprocess
import json  # Added import for JSON serialization

# File paths constants
DATA_DIR = '/opt/airflow/data/'
POSTGRES_CONN = 'postgresql://airflow:airflow@postgres:5432/mdbfiles'
LOG_TABLE = 'etl_logs'  # Table to store logs

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def initialize_log_table():
    """
    Initialize the log table in PostgreSQL if it doesn't exist.

    This function creates the `etl_logs` table in the PostgreSQL database
    to store logs related to the ETL process. If the table already exists,
    the function does nothing.
    """
    engine = create_engine(POSTGRES_CONN)
    with engine.connect() as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS {LOG_TABLE} (
                id SERIAL PRIMARY KEY,
                timestamp TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
                file_name VARCHAR,
                table_name VARCHAR,
                status VARCHAR,
                message TEXT,
                schema TEXT
            )
        """)


def log_to_db(file_name, table_name, status, message, schema=None):
    """
    Log the ETL process details to the PostgreSQL log table.

    Args:
        file_name (str): Name of the MDB file.
        table_name (str): Name of the table being processed.
        status (str): Status of the operation ('SUCCESS', 'ERROR', 'INFO').
        message (str): Detailed message or error.
        schema (str, optional): Schema of the table being processed in JSON format. Defaults to None.
    """
    engine = create_engine(POSTGRES_CONN)
    timestamp = datetime.now(timezone.utc)
    with engine.connect() as conn:
        conn.execute(f"""
            INSERT INTO {LOG_TABLE} (timestamp, file_name, table_name, status, message, schema)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (timestamp, file_name, table_name, status, message, schema))


def get_mdb_files():
    """
    Retrieve a list of all MDB files in the data directory.

    This function scans the specified `DATA_DIR` for files with the `.mdb` extension.

    Returns:
        list: A list of MDB file names found in the data directory.
    """
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.mdb')]
    logger.info(f"Files: {files}")
    return files


def get_tables_list(mdb_file):
    """
    Retrieve the list of tables from a given MDB file.

    Args:
        mdb_file (str): Name of the MDB file.

    Returns:
        list: A list of table names found in the MDB file.

    Raises:
        AirflowException: If there is an error fetching tables from the MDB file.
    """
    command = f"mdb-tables -1 {os.path.join(DATA_DIR, mdb_file)}"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Error fetching tables from {mdb_file}: {result.stderr}")
        log_to_db(mdb_file, None, 'ERROR', result.stderr)
        raise AirflowException(f"Failed to get tables from {mdb_file}")
    tables = result.stdout.strip().split('\n')
    logger.info(f"Tables in {mdb_file}: {tables}")
    return tables


def parse_create_table(schema_str):
    """
    Parse a CREATE TABLE statement and extract column names and types.

    Args:
        schema_str (str): The CREATE TABLE SQL statement.

    Returns:
        dict: A dictionary mapping column names to their types.
    """
    columns = {}
    lines = schema_str.splitlines()
    for line in lines:
        line = line.strip()
        if line.startswith('--') or line.startswith('/') or line.startswith('*') or not line:
            continue  # Skip comments and empty lines
        if line.upper().startswith('CREATE TABLE'):
            continue  # Skip the CREATE TABLE line
        if line.startswith('(') or line.startswith(')'):
            continue  # Skip opening and closing parentheses
        if line.endswith(','):
            line = line[:-1]
        # Assume format [ColumnName] Type (Size)
        if line.startswith('['):
            parts = line.split(']', 1)
            column_name = parts[0][1:].strip()
            type_part = parts[1].strip().rstrip(',')
            columns[column_name] = type_part
    return columns


def get_table_schema(mdb_file, table):
    """
    Retrieve the schema of a specific table from an MDB file.

    This function parses the CREATE TABLE statement to extract column names and their data types,
    storing them in a dictionary format.

    Args:
        mdb_file (str): Name of the MDB file.
        table (str): Name of the table whose schema is to be retrieved.

    Returns:
        str: The schema of the specified table in JSON format.

    Raises:
        AirflowException: If there is an error fetching the table schema.
    """
    command = f"mdb-schema -T {table} {os.path.join(DATA_DIR, mdb_file)}"
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    if result.returncode != 0:
        logger.error(f"Error fetching schema for table {table} in {mdb_file}: {result.stderr}")
        log_to_db(mdb_file, table, 'ERROR', result.stderr)
        raise AirflowException(f"Failed to get schema for table {table} in {mdb_file}")
    schema_str = result.stdout.strip()
    schema_dict = parse_create_table(schema_str)
    schema_json = json.dumps(schema_dict)  # Serialize dictionary to JSON string
    logger.info(f"Table Schema for {table}: {schema_json}")
    return schema_json


def get_mdb_conn_string(file_name):
    """
    Generate the ODBC connection string for a given MDB file.

    Args:
        file_name (str): Name of the MDB file.

    Returns:
        str: The ODBC connection string for the MDB file.
    """
    conn_str = (
        r'DRIVER={MDB};'
        rf'DBQ=/opt/airflow/data/{file_name};'
    )
    return conn_str


def rename_and_save_files():
    """
    Rename MDB files by replacing spaces with underscores and save them.

    This function scans for MDB files in the data directory and renames any
    files that contain spaces in their names by replacing the spaces with underscores.
    Successful renames are logged to the database. If an error occurs during
    the renaming process, the error is logged, and an AirflowException is raised.
    """
    try:
        files = get_mdb_files()
        for file in files:
            if " " in file:
                new_file_name = file.replace(" ", "_")
                original_path = os.path.join(DATA_DIR, file)
                new_path = os.path.join(DATA_DIR, new_file_name)
                os.rename(original_path, new_path)
                logger.info(f"Renamed {file} to {new_file_name}")
                log_to_db(new_file_name, None, 'SUCCESS', 'File renamed successfully')
    except Exception as e:
        logger.error(f"Error renaming files: {str(e)}")
        log_to_db(None, None, 'ERROR', str(e))
        raise AirflowException("File renaming failed")


def load_mdb_file_to_postgres(**kwargs):
    """
    Load data from MDB files into PostgreSQL.

    This function processes each MDB file by retrieving its tables, extracting
    data from each table, and loading the data into the PostgreSQL database.
    Successful loads and any errors encountered are logged to the database.

    Additionally, the schema of each table is retrieved and stored in the logs.

    Args:
        **kwargs: Additional keyword arguments passed by Airflow.
    """
    engine = create_engine(POSTGRES_CONN)
    mdb_file_list = get_mdb_files()
    for file_name in mdb_file_list:
        try:
            tables = get_tables_list(file_name)
            conn_str = get_mdb_conn_string(file_name)
            with pyodbc.connect(conn_str) as conn:
                for table in tables:
                    try:
                        # Retrieve and log schema
                        schema = get_table_schema(file_name, table)
                        
                        # Load data
                        query = f"SELECT * FROM {table}"
                        
                        # Read data an create a dataframe
                        df = pd.read_sql(query, conn)
                        table_name = f"{os.path.splitext(file_name)[0]}_{table}"
                        df.to_sql(table_name, engine, if_exists='replace', index=False)
                        
                        logger.info(f"Loaded table {table_name} successfully.")
                        log_to_db(file_name, table, 'SUCCESS', 'Table loaded successfully', schema)
                    except Exception as table_err:
                        logger.error(f"Error loading table {table} from {file_name}: {str(table_err)}")
                        log_to_db(file_name, table, 'ERROR', str(table_err), schema)
        except Exception as file_err:
            logger.error(f"Error processing file {file_name}: {str(file_err)}")
            log_to_db(file_name, None, 'ERROR', str(file_err))
            continue  # Continue with the next file


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 21),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'mdb_to_postgress_etl_dag',
    default_args=default_args,
    description='ETL DAG for extracting data from .mdb to Postgres',
    schedule_interval='@daily',
    catchup=False,
    max_active_tasks=5,  # Optimize parallelism
) as dag:

    initialize_log_table = PythonOperator(
        task_id='initialize_log_table',
        python_callable=initialize_log_table,
    )

    rename_and_save_files = PythonOperator(
        task_id='rename_and_save_files',
        python_callable=rename_and_save_files,
    )

    load_mdb_file_to_postgres = PythonOperator(
        task_id='load_mdb_file_to_postgres',
        python_callable=load_mdb_file_to_postgres,
        provide_context=True,
    )

    initialize_log_table >> rename_and_save_files >> load_mdb_file_to_postgres