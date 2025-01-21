from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime, timedelta
import pandas as pd

default_args = {
    'owner': 'Noey',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='noey_expenses_pipeline',
    default_args=default_args,
    start_date=datetime(2025, 1, 15),
    schedule_interval='@daily',
    catchup=False,
) as dag:
    # Step 1: Create a table in MSSQL
    def create_mssql_table():
        mssql_hook = MsSqlHook(mssql_conn_id='mssql_noey')
        sql = """ 
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='daily_expenses' AND xtype='U')
        CREATE TABLE daily_expenses (
            id INT IDENTITY(1,1) PRIMARY KEY,
            date DATE NOT NULL,
            category NVARCHAR(255) NOT NULL,
            amount FLOAT NOT NULL,
            remarks NVARCHAR(MAX)
        );
        """
        mssql_hook.run(sql)

    create_table = PythonOperator(
        task_id='create_mssql_table',
        python_callable=create_mssql_table,
    )

    # Step 2: Insert sample data into MSSQL
    def insert_sample_data():
        # Sample data
        data = [
            {'date': '2025-01-15', 'category': 'Food', 'amount': 50, 'remarks': 'Lunch'},
            {'date': '2025-01-15', 'category': 'Transport', 'amount': 30, 'remarks': 'Taxi'},
            {'date': '2025-01-16', 'category': 'Investment', 'amount': 200, 'remarks': 'Stocks'},
            {'date': '2025-01-16', 'category': 'Entertainment', 'amount': 100, 'remarks': 'Movies'},
        ]

        # Connect to MSSQL using Hook
        mssql_hook = MsSqlHook(mssql_conn_id='mssql_noey')
        
        # Insert data
        for row in data:
            sql = """ 
                INSERT INTO daily_expenses (date, category, amount, remarks)
                VALUES (%s, %s, %s, %s);
            """
            mssql_hook.run(sql, parameters=(row['date'], row['category'], row['amount'], row['remarks']))

    insert_data = PythonOperator(
        task_id='insert_sample_data',
        python_callable=insert_sample_data,
    )

    # Step 3: Migrate data from MSSQL to PostgreSQL
    def migrate_data_to_postgresql():
        # Connect to MSSQL
        mssql_hook = MsSqlHook(mssql_conn_id='mssql_noey')
        df = mssql_hook.get_pandas_df("SELECT * FROM daily_expenses;")

        # Connect to PostgreSQL
        postgres_hook = PostgresHook(postgres_conn_id='Noey_Interns')
        
        # Create table in PostgreSQL
        create_table_sql = """ 
        CREATE TABLE IF NOT EXISTS daily_expenses (
            id SERIAL PRIMARY KEY,
            date DATE NOT NULL,
            category VARCHAR(255) NOT NULL,
            amount FLOAT NOT NULL,
            remarks TEXT
        );
        """
        postgres_hook.run(create_table_sql)

        # Insert data into PostgreSQL
        for _, row in df.iterrows():
            insert_sql = """ 
                INSERT INTO daily_expenses (date, category, amount, remarks)
                VALUES (%s, %s, %s, %s);
            """
            postgres_hook.run(insert_sql, parameters=(row['date'], row['category'], row['amount'], row['remarks']))

    migrate_data = PythonOperator(
        task_id='migrate_data_to_postgresql',
        python_callable=migrate_data_to_postgresql,
    )

    # Step 4: Validate and print data from PostgreSQL
    def validate_postgresql_data():
        postgres_hook = PostgresHook(postgres_conn_id='Noey_Interns')
        records = postgres_hook.get_records("SELECT * FROM daily_expenses;")
        
        print("Data in PostgreSQL:")
        for row in records:
            print(row)

    validate_data = PythonOperator(
        task_id='validate_postgresql_data',
        python_callable=validate_postgresql_data,
    )

    # Set task dependencies
    create_table >> insert_data >> migrate_data >> validate_data