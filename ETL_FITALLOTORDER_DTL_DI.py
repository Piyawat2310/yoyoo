from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime, timedelta
import logging
from psycopg2.extras import execute_values

def get_date_range():
    today = datetime.now().strftime("%Y%m%d")  
    from_date = (datetime.now() - timedelta(days=6)).strftime("%d/%m/%Y")  
    end_date = datetime.now().strftime("%d/%m/%Y")  
    return today, from_date, end_date

# Define the DAG
default_args = {
    "owner": "noey",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="etl_fitallotorder_dtl_di",
    default_args=default_args,
    description="Migrate data from MSSQL to PostgreSQL with validation",
    schedule_interval="* 12 * * *", 
    catchup=False,
    tags=["etl", "fit", "order", "mutual", "allotoorde", "noey"]

)


def ODS_fetch_and_insert_data():
    """Fetch data from MSSQL and insert into ODS_FITALLOTORDER_DTL_DI efficiently, preventing duplicates."""
    import logging
    from psycopg2.extras import execute_values

    # Get date range
    today, from_date, end_date = get_date_range()

    # Fetch data from MSSQL with date condition
    mssql_hook = MsSqlHook(mssql_conn_id="company_connection")
    mssql_conn = mssql_hook.get_conn()
    mssql_cursor = mssql_conn.cursor()

    mssql_query = """
    SELECT 
    f.FUND_CODE,
    ord.ORDER_ID,
    ord.AMOUNT,
    ab.AGENT_BRANCH_NAME_THAI,
    ord.CREATE_DATETIME,
    ord.ORDER_TYPE,
    ft.FUND_TYPE_NAME_ENG,
    ord.UNITHOLDER_ID,
    uh.INVESTOR_ID,
    CASE
        WHEN ord.STANDING_ORDER_ID IS NULL THEN 'NO'
        ELSE 'YES'
    END AS DCA
FROM BWM_FIT_DB.dbo.Orders ord 
INNER JOIN BWM_FIT_DB.dbo.FUNDS f ON ord.FUND_ID = f.FUND_ID
INNER JOIN BWM_FIT_DB.dbo.UNITHOLDERS uh ON uh.UNITHOLDER_ID = ord.UNITHOLDER_ID
INNER JOIN BWM_FIT_DB.dbo.MARKETINGS mkt ON uh.MARKETING_ID = mkt.MARKETING_ID
INNER JOIN BWM_FIT_DB.dbo.AGENT_BRANCHES ab ON mkt.AGENT_BRANCH_ID = ab.AGENT_BRANCH_ID
INNER JOIN BWM_FIT_DB.dbo.FUND_TYPES ft ON f.FUND_TYPE_ID = ft.FUND_TYPE_ID
WHERE 
    ord.ORDER_STATUS = 'ALLOCATED'
    AND ord.CREATE_DATETIME >= '2024-01-01';
    """
     
    mssql_cursor.execute(mssql_query, (from_date, end_date))
    rows = mssql_cursor.fetchall()
    mssql_cursor.close()
    mssql_conn.close()

    if not rows:
        logging.info("No new data to insert into ODS_FITALLOTORDER_DTL_DI.")
        return "No data to insert"

    # Add inc_day as the current date in YYMMDD format
    rows = [row + (today,) for row in rows]

    # Connect to PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id="SESAME-DB")
    postgres_conn = postgres_hook.get_conn()
    postgres_cursor = postgres_conn.cursor()

    # Check if today's records exist in ODS
    check_query = "SELECT COUNT(*) FROM ods_fitallotorder_dtl_di WHERE inc_day = %s;"
    postgres_cursor.execute(check_query, (today,))
    record_count = postgres_cursor.fetchone()[0]

    if record_count > 0:
        # Delete only if there are existing records for today
        delete_query = "DELETE FROM ods_fitallotorder_dtl_di WHERE inc_day = %s;"
        postgres_cursor.execute(delete_query, (today,))
        logging.info(f"Deleted {record_count} existing records in ods_fitallotorder_dtl_di.")

    # Insert new data using batch insert
    insert_query = """
    INSERT INTO ods_fitallotorder_dtl_di (
        FUND_CODE, ORDER_ID, AMOUNT, AGENT_BRANCH_NAME_THAI, CREATE_DATETIME, 
        ORDER_TYPE, FUND_TYPE_NAME_ENG, UNITHOLDER_ID, INVESTOR_ID, DCA, inc_day
    ) VALUES %s;
    """

    execute_values(postgres_cursor, insert_query, rows)

    postgres_conn.commit()
    postgres_cursor.close()
    postgres_conn.close()

    logging.info(f"Inserted {len(rows)} records into ods_fitallotorder_dtl_di")
    return f"Inserted {len(rows)} records into ods_fitallotorder_dtl_di"

def DWD_fetch_ODS_and_insert():
    """Fetch data from ODS and insert/update dwd_fitallotorder_dtl_di"""
    logging.info("Fetching ODS data and inserting/updating DWD.")

    pg_hook = PostgresHook(postgres_conn_id="SESAME-DB")
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        _, from_date, end_date = get_date_range()

        # Check if DWD table has any data
        cursor.execute("SELECT 1 FROM dwd_fitallotorder_dtl_di LIMIT 1")
        dwd_has_data = cursor.fetchone()

        if not dwd_has_data:
            logging.info("DWD is empty. Copying all records from ODS.")
            cursor.execute("""
                INSERT INTO dwd_fitallotorder_dtl_di (
                    FUND_CODE, ORDER_ID, AMOUNT, AGENT_BRANCH_NAME_THAI, CREATE_DATETIME, 
                    ORDER_TYPE, FUND_TYPE_NAME_ENG, UNITHOLDER_ID, INVESTOR_ID, DCA, inc_day
                )
                SELECT 
                    FUND_CODE, ORDER_ID, AMOUNT, AGENT_BRANCH_NAME_THAI, CREATE_DATETIME, 
                    ORDER_TYPE, FUND_TYPE_NAME_ENG, UNITHOLDER_ID, INVESTOR_ID, DCA, 
                    TO_CHAR(CREATE_DATETIME, 'YYYYMMDD') AS inc_day
                FROM ODS_FITALLOTORDER_DTL_DI
                WHERE CREATE_DATETIME BETWEEN TO_DATE(%s, 'DD/MM/YYYY') 
                AND TO_DATE(%s, 'DD/MM/YYYY')
            """, (from_date, end_date))
            logging.info(f"Inserted {cursor.rowcount} initial records into DWD.")

        else:
            logging.info("Processing new records and updates from ODS.")

            # Update existing records
            cursor.execute("""
                UPDATE dwd_fitallotorder_dtl_di d
                SET 
                    AMOUNT = o.AMOUNT,
                    AGENT_BRANCH_NAME_THAI = o.AGENT_BRANCH_NAME_THAI,
                    ORDER_TYPE = o.ORDER_TYPE,
                    FUND_TYPE_NAME_ENG = o.FUND_TYPE_NAME_ENG,
                    UNITHOLDER_ID = o.UNITHOLDER_ID,
                    INVESTOR_ID = o.INVESTOR_ID,
                    DCA = o.DCA,
                    inc_day = TO_CHAR(o.CREATE_DATETIME, 'YYYYMMDD')
                FROM ODS_FITALLOTORDER_DTL_DI o
                WHERE d.ORDER_ID = o.ORDER_ID
                AND d.CREATE_DATETIME = o.CREATE_DATETIME
                AND o.CREATE_DATETIME BETWEEN TO_DATE(%s, 'DD/MM/YYYY') 
                AND TO_DATE(%s, 'DD/MM/YYYY')
            """, (from_date, end_date))
            logging.info(f"Updated {cursor.rowcount} existing records in DWD.")

            # Insert new records
            cursor.execute("""
                INSERT INTO dwd_fitallotorder_dtl_di (
                    FUND_CODE, ORDER_ID, AMOUNT, AGENT_BRANCH_NAME_THAI, CREATE_DATETIME, 
                    ORDER_TYPE, FUND_TYPE_NAME_ENG, UNITHOLDER_ID, INVESTOR_ID, DCA, inc_day
                )
                SELECT 
                    o.FUND_CODE, o.ORDER_ID, o.AMOUNT, o.AGENT_BRANCH_NAME_THAI, o.CREATE_DATETIME, 
                    o.ORDER_TYPE, o.FUND_TYPE_NAME_ENG, o.UNITHOLDER_ID, o.INVESTOR_ID, o.DCA, 
                    TO_CHAR(o.CREATE_DATETIME, 'YYYYMMDD')
                FROM ODS_FITALLOTORDER_DTL_DI o
                WHERE NOT EXISTS (
                    SELECT 1 FROM dwd_fitallotorder_dtl_di d
                    WHERE d.ORDER_ID = o.ORDER_ID
                    AND d.CREATE_DATETIME = o.CREATE_DATETIME
                )
                AND o.CREATE_DATETIME BETWEEN TO_DATE(%s, 'DD/MM/YYYY') 
                AND TO_DATE(%s, 'DD/MM/YYYY')
            """, (from_date, end_date))
            logging.info(f"Inserted {cursor.rowcount} new records into DWD.")

        conn.commit()
        logging.info("DWD update completed successfully.")

    except Exception as e:
        conn.rollback()
        logging.error(f"Error during DWD update: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

    

ods_fetch_and_insert_task = PythonOperator(
    task_id="ODS_fetch_and_insert_data",
    python_callable=ODS_fetch_and_insert_data ,  
    dag=dag,
)
dwd_fetch_ODS_and_insert_task = PythonOperator(
    task_id="DWD_fetch_ODS_and_insert",
    python_callable=DWD_fetch_ODS_and_insert,
    dag=dag,
)

ods_fetch_and_insert_task >> dwd_fetch_ODS_and_insert_task 
