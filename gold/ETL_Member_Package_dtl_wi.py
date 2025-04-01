import logging
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

def extract_data_from_mssql(**kwargs):
    try:
        mssql_hook = MsSqlHook(mssql_conn_id="company_connection")
        sql_query = """
        SELECT * 
        FROM BWM_PLTDB.dbo.RT_MEMBER_PACKAGE
        WHERE STARTDATE >= CAST(DATEADD(DAY, -7, GETDATE()) AS DATE)
        AND STARTDATE <= CAST(GETDATE() AS DATE);
        """
        records = mssql_hook.get_records(sql_query)
        
        if not records:
            logging.info("No records found in source table")
        
        kwargs['ti'].xcom_push(key='extracted_data', value=records)
        return len(records)
    except Exception as e:
        logging.error(f"Error in extraction: {str(e)}")
        raise

def load_data_into_ods(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="SESAME-DB")
    records = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extract_data')
    
    if not records:
        logging.info("No records to load into ODS")
        return 0
    
    try:
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        insert_query = """
        INSERT INTO ods_member_package_dtl_wi 
        (id, member_id, package_group, purchase_schedule_id, start_date, expiry_date, remark, inc_date) 
        VALUES (%s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
        """
        cursor.executemany(insert_query, records)
        conn.commit()
        logging.info(f"Loaded {cursor.rowcount} records into ODS")
        return cursor.rowcount
    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading into ODS: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

def upsert_data_into_dwd(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="SESAME-DB")
    
    try:
        upsert_query = """
        WITH latest_ods_data AS (
            SELECT DISTINCT ON (member_id, package_group, start_date) *
            FROM ods_member_package_dtl_wi
            WHERE inc_date = (
                SELECT MAX(inc_date)
                FROM ods_member_package_dtl_wi
            )
            ORDER BY member_id, package_group, start_date, inc_date DESC
        )
        INSERT INTO dwd_member_package_dtl_wi AS dwd 
        (id, member_id, package_group, purchase_schedule_id, start_date, expiry_date, remark, inc_date)
        SELECT 
            id, member_id, package_group, purchase_schedule_id, 
            start_date, expiry_date, remark, start_date::TIMESTAMP AS inc_date
        FROM latest_ods_data
        ON CONFLICT (member_id, package_group, start_date) 
        DO UPDATE SET 
            purchase_schedule_id = EXCLUDED.purchase_schedule_id,
            expiry_date = EXCLUDED.expiry_date,
            remark = EXCLUDED.remark,
            inc_date = EXCLUDED.start_date::TIMESTAMP;
        """
        pg_hook.run(upsert_query)
        logging.info("Successfully upserted latest run ODS data into DWD")
    except Exception as e:
        logging.error(f"Error upserting into DWD: {str(e)}")
        raise


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="ETL_Member_Package_dtl_wi",
    default_args=default_args,
    schedule_interval="0 12 * * 1",
    catchup=False
) as dag:
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_from_mssql
    )
    
    load_ods = PythonOperator(
        task_id="load_ods",
        python_callable=load_data_into_ods
    )
    
    upsert_dwd = PythonOperator(
        task_id="upsert_dwd",
        python_callable=upsert_data_into_dwd
    )
    
    extract_data >> load_ods >> upsert_dwd
