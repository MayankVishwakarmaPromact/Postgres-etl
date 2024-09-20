from airflow.exceptions import AirflowException
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import pyodbc
import sqlalchemy
from sqlalchemy import create_engine

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

dag = DAG(
    'postgress_etl_dag',
    default_args=default_args,
    description='ETL DAG for .mdb, .sql, and .csv files',
    schedule_interval='@daily',
    catchup=False,
)


def extract_mdb(**kwargs):
    # Connection string to access the MDB file using MDBTools
    conn_str = (
        r'DRIVER={MDB};'
        r'DBQ=/opt/airflow/data/sample.mdb;'
    )

    # Establish connection
    conn = pyodbc.connect(conn_str)

    # Query the MDB file
    query = "SELECT * FROM Table1"

    # Read data into a pandas DataFrame
    df = pd.read_sql(query, conn)

    # Close the connection
    conn.close()

    return df

# def extract_sql(**kwargs):
#     engine = create_engine('mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server')
#     query = "SELECT * FROM your_table"
#     df = pd.read_sql(query, engine)
#     return df

# def extract_csv(**kwargs):
#     df = pd.read_csv('/opt/airflow/data/your_input.csv')
#     return df


def load_to_postgres(**kwargs):
    ti = kwargs['ti']
    mdb_df = ti.xcom_pull(task_ids='extract_mdb')
    sql_df = ti.xcom_pull(task_ids='extract_sql')
    csv_df = ti.xcom_pull(task_ids='extract_csv')

    engine = create_engine('postgresql://airflow:airflow@postgres/mdbfiles')

    # Load MDB data
    mdb_df.to_sql('mdb_table', engine, if_exists='replace', index=False)

    # # Load SQL data
    # sql_df.to_sql('sql_table', engine, if_exists='replace', index=False)

    # # Load CSV data
    # csv_df.to_sql('csv_table', engine, if_exists='replace', index=False)


extract_mdb_task = PythonOperator(
    task_id='extract_mdb',
    python_callable=extract_mdb,
    dag=dag,
)

# extract_sql_task = PythonOperator(
#     task_id='extract_sql',
#     python_callable=extract_sql,
#     dag=dag,
# )

# extract_csv_task = PythonOperator(
#     task_id='extract_csv',
#     python_callable=extract_csv,
#     dag=dag,
# )

load_to_postgres_task = PythonOperator(
    task_id='load_to_postgres',
    python_callable=load_to_postgres,
    dag=dag,
)

extract_mdb_task >> load_to_postgres_task
