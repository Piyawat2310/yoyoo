from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd

def extract_data_from_mssql(**context):
    """
    Extract data from Microsoft SQL Server using the provided query
    """
    # สร้าง hook เชื่อมต่อกับ Microsoft SQL Server
    mssql_hook = MsSqlHook(mssql_conn_id='company_connection')
    
    # คิวรีดึงข้อมูล (ใช้คิวรีเดิมที่คุณให้มา)
    query = """
    
USE BWM_SA_DB;

SELECT FORMAT(GETDATE(), 'dd/MM/yyyy') AS AS_OF_DATE
    , SA_USER.MARKETING_CODE
    , ISNULL(CONVERT(VARCHAR(MAX), SA_CUSTOMER.CUSTOMER_ID), '') AS CUSTOMER_ID
    , ISNULL(DATEDIFF(YEAR, SA_CUSTOMER.BIRTH_DATE, GETDATE()), 0) AS AGE
    , ISNULL(
        CASE 
            WHEN DATEDIFF(YEAR, BIRTH_DATE, GETDATE()) <= 17 THEN '1) 0-17' 
            WHEN DATEDIFF(YEAR, BIRTH_DATE, GETDATE()) BETWEEN 18 AND 24 THEN '2) 18-24' 
            WHEN DATEDIFF(YEAR, BIRTH_DATE, GETDATE()) BETWEEN 25 AND 34 THEN '3) 25-34' 
            WHEN DATEDIFF(YEAR, BIRTH_DATE, GETDATE()) BETWEEN 35 AND 44 THEN '4) 35-44' 
            WHEN DATEDIFF(YEAR, BIRTH_DATE, GETDATE()) BETWEEN 45 AND 54 THEN '5) 45-54' 
            WHEN DATEDIFF(YEAR, BIRTH_DATE, GETDATE()) BETWEEN 55 AND 64 THEN '6) 55-64' 
            ELSE '7) 65+' 
        END, ''
    ) AS AGE_RANGE
    , ISNULL(investor.monthly_income, 0) AS MONTHLY_INCOME
    , ISNULL(SA_FUNDHOUSE.FUNDHOUSE_CODE, '') AS FUNDHOUSE_CODE
    , ISNULL(SA_UNIT_ACCOUNT.UNITHOLDER_NO, '') AS UNITHOLDER_NO
    , ISNULL(SA_FUND.FUND_CODE, '') AS FUND_CODE
    , ISNULL(SA_FUND_TYPE.TYPE_TH, '') AS TYPE_TH
    , CASE 
        WHEN SA_FUND_TYPE.TYPE_ENG IN ('Equity Fund', 'Exchange Traded Fund', 'Fund of Funds', 'Balanced Fund', 'Flexible Portfolio Fund') THEN '01.HIGH RISK (Equity,Exchange,Balanced,Flexible)'
        WHEN SA_FUND_TYPE.TYPE_ENG LIKE 'Fixed Income Fund%' THEN '02.FIX INCOME'
        WHEN SA_FUND_TYPE.TYPE_ENG = 'Money Market Fund' THEN '03.MONEY MARKET'
        WHEN SA_FUND_TYPE.TYPE_ENG IN ('Gold Fund', 'Oil Fund', 'Property Fund') THEN '04.ALTERNATIVE (GOLD,OIL,PROP)'
        WHEN SA_FUND_TYPE.TYPE_ENG = 'Long Term Equity Fund' THEN '05.LTF'
        WHEN SA_FUND_TYPE.TYPE_ENG LIKE 'Retirement Mutual Fund%' THEN '06.RMF'
        WHEN SA_FUND_TYPE.TYPE_ENG LIKE 'Foreign Investment Fund%' THEN '07.FOREIGN'
        WHEN SA_FUND_TYPE.TYPE_ENG = 'Super Saving Fund Extra' THEN '08.SSFX'
        WHEN SA_FUND_TYPE.TYPE_ENG = 'Super Saving Fund' THEN '09.SSF'
        WHEN SA_FUND_TYPE.TYPE_ENG = 'Others' THEN '10.OTHER'
        ELSE 'NOT IN RANGE!!! ' + SA_FUND_TYPE.TYPE_ENG 
    END AS FUND_TYPE_GROUP
    , ISNULL(FORMAT(CONVERT(DATE, SA_ORDER.ORDER_DATE), 'dd/MM/yyyy'), '') AS ORDER_DATE
    , ISNULL(FORMAT(CONVERT(DATE, SA_ORDER_DETAIL.AT_ALLOT_DATE), 'dd/MM/yyyy'), '') AS ALLOT_DATE
    , CASE SA_ORDER_DETAIL.ORDER_TYPE
        WHEN 1 THEN '1) SUBSCRIPTION'  
        WHEN 2 THEN '2) REDEMPTION' 
        WHEN 3 THEN '3) SWITCHING-IN' 
        WHEN 4 THEN '4) SWITCHING-OUT' 
        WHEN 6 THEN '6) TRANSFER-IN' 
        WHEN 7 THEN '7) TRANSFER-OUT' 
        WHEN 8 THEN '8) EX-SWITCHING-IN' 
        WHEN 9 THEN '9) EX-SWITCHING-OUT' 
        ELSE ISNULL(SA_ORDER_DETAIL.ORDER_TYPE, '') 
    END AS ORDER_TYPE
    , ISNULL(CASE WHEN SA_ORDER_DETAIL.ORDER_TYPE = 1 THEN SA_ORDER_DETAIL.TOTAL_COST ELSE 0 END, 0) AS SUBSCRIPTION
    , ISNULL(CASE WHEN SA_ORDER_DETAIL.ORDER_TYPE = 2 THEN -SA_ORDER_DETAIL.TOTAL_COST ELSE 0 END, 0) AS REDEMPTION
    , ISNULL(CASE WHEN SA_ORDER_DETAIL.ORDER_TYPE = 3 THEN SA_ORDER_DETAIL.TOTAL_COST ELSE 0 END, 0) AS SWITCHING_IN
    , ISNULL(CASE WHEN SA_ORDER_DETAIL.ORDER_TYPE = 4 THEN -SA_ORDER_DETAIL.TOTAL_COST ELSE 0 END, 0) AS SWITCHING_OUT
    , ISNULL(CASE WHEN SA_ORDER_DETAIL.ORDER_TYPE = 6 THEN SA_ORDER_DETAIL.TOTAL_COST ELSE 0 END, 0) AS TRANSFER_IN
    , ISNULL(CASE WHEN SA_ORDER_DETAIL.ORDER_TYPE = 7 THEN -SA_ORDER_DETAIL.TOTAL_COST ELSE 0 END, 0) AS TRANSFER_OUT
    , ISNULL(CASE WHEN SA_ORDER_DETAIL.ORDER_TYPE = 8 THEN SA_ORDER_DETAIL.TOTAL_COST ELSE 0 END, 0) AS EX_SWITCHING_IN
    , ISNULL(CASE WHEN SA_ORDER_DETAIL.ORDER_TYPE = 9 THEN -SA_ORDER_DETAIL.TOTAL_COST ELSE 0 END, 0) AS EX_SWITCHING_OUT
    , ISNULL(CASE WHEN SA_ORDER_DETAIL.ORDER_TYPE IN (2, 4, 7, 9) THEN -SA_ORDER_DETAIL.TOTAL_COST ELSE SA_ORDER_DETAIL.TOTAL_COST END, 0) AS TOTAL_COST
    , ISNULL(CASE WHEN SA_ORDER_DETAIL.ORDER_TYPE IN (2, 4, 7, 9) THEN -SA_ORDER_DETAIL.TOTAL_UNIT ELSE SA_ORDER_DETAIL.TOTAL_UNIT END, 0) AS TOTAL_UNIT  
FROM SA_USER 
    LEFT JOIN SA_UNIT_ACCOUNT ON SA_UNIT_ACCOUNT.MARKETING_ID = SA_USER.USER_ID
    LEFT JOIN SA_UNIT_HOLDERS ON SA_UNIT_HOLDERS.UNIT_ACCOUNT_ID = SA_UNIT_ACCOUNT.UNIT_ACCOUNT_ID
    LEFT JOIN SA_CUSTOMER ON SA_CUSTOMER.CUSTOMER_ID = SA_UNIT_HOLDERS.CUSTOMER_ID    
    LEFT JOIN SA_FUNDHOUSE ON SA_UNIT_ACCOUNT.FUNDHOUSE_ID = SA_FUNDHOUSE.FUNDHOUSE_ID
    LEFT JOIN SA_ORDER ON SA_ORDER.UNIT_ACCOUNT_ID = SA_UNIT_ACCOUNT.UNIT_ACCOUNT_ID 
        AND ORDER_STATUS NOT IN ('T', 'N') 
    LEFT JOIN SA_ORDER_DETAIL ON SA_ORDER_DETAIL.ORDER_ID = SA_ORDER.ORDER_ID
    LEFT JOIN SA_FUND ON SA_FUND.FUND_ID = SA_ORDER.FUND_ID
    LEFT JOIN SA_FUND_TYPE ON SA_FUND_TYPE.TYPE_ID = SA_FUND.FUND_TYPE
    LEFT JOIN SA_AGENT_BRANCH ON SA_AGENT_BRANCH.AGENT_BRANCH_ID = SA_USER.AGENT_BRANCH_ID
    LEFT JOIN (SELECT email, monthly_income FROM BWM_FIT_DB.DBO.INVESTORS) AS investor 
        ON investor.email = SA_CUSTOMER.email
WHERE SA_AGENT_BRANCH.AGENT_BRANCH_ID <> 1 
    AND SA_USER.ENABLE = 'Y'
    AND (
        (SA_ORDER.ORDER_DATE BETWEEN DATEADD(DAY, -30, GETDATE()) AND GETDATE())) 

ORDER BY SA_AGENT_BRANCH.BRANCH_NAME_TH, SA_USER.MARKETING_CODE, CUSTOMER_ID, SA_FUNDHOUSE.FUNDHOUSE_CODE, SA_FUND.FUND_CODE, SA_ORDER.ORDER_DATE;

    
    """
    
    # ประมวลผลคิวรีและดึงผลลัพธ์เป็น DataFrame
    df = mssql_hook.get_pandas_df(query)
    
    # ส่งข้อมูล DataFrame ไปยัง XCom สำหรับงานถัดไป
    context['ti'].xcom_push(key='mssql_data', value=df)
    
    return df

def preprocess_data(df):
    """
    Preprocess the data before loading to the database
    """
    # แปลงข้อมูลวันที่ก่อนโหลด
    df['ORDER_DATE'] = pd.to_datetime(df['ORDER_DATE'], format='%d/%m/%Y', errors='coerce')
    df['ALLOT_DATE'] = pd.to_datetime(df['ALLOT_DATE'], format='%d/%m/%Y', errors='coerce')
    
    # แทนที่ค่าว่างและค่าที่เป็น NaN ด้วย None
    df = df.where(pd.notnull(df), None)
    
    # แปลง DataFrame เป็น list ของ dictionary พร้อมแปลง timestamps เป็น strings
    data = []
    for _, row in df.iterrows():
        processed_row = {}
        for col, value in row.items():
            # จัดการกับ timestamps โดยแปลงเป็น string
            if isinstance(value, pd.Timestamp):
                processed_row[col] = value.strftime('%Y-%m-%d %H:%M:%S')
            # จัดการกับ NaT และ NaN
            elif pd.isna(value):
                processed_row[col] = None
            else:
                processed_row[col] = value
        data.append(processed_row)
    
    return data, list(df.columns)

def load_data_to_ods(**context):
    """
    Load data from Microsoft SQL Server to ODS table in PostgreSQL
    """
    # ดึง DataFrame จาก XCom
    df = context['ti'].xcom_pull(key='mssql_data')
    
    # ประมวลผลข้อมูล
    data, columns = preprocess_data(df)
    
    # สร้าง hook เชื่อมต่อกับ PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='SESAME-DB')
    
    # ชื่อตารางที่จะโหลดข้อมูล
    table_name = 'ods_Dealer_Transaction_Data'
    
    # สร้าง SQL เพื่อ insert 
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s']*len(columns))})"
    
    # เปิดการเชื่อมต่อและ insert
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                # ใช้ executemany กับ processed_row
                cur.executemany(sql, [[row.get(col, None) for col in columns] for row in data])
                conn.commit()
                print(f"บันทึกข้อมูลลงในตาราง {table_name} จำนวน {len(data)} แถว")
            except Exception as e:
                conn.rollback()
                print(f"เกิดข้อผิดพลาดในการ insert ลงตาราง {table_name}: {e}")
                raise
    
    context['ti'].xcom_push(key='processed_columns', value=columns)
    
    # ถ้าจำเป็นต้องส่งข้อมูลบางส่วน เช่น จำนวนแถว
    context['ti'].xcom_push(key='processed_row_count', value=len(data))

def upsert_data_to_dwd(**context):
    """
    Update or insert data to DWD table in PostgreSQL
    """
    # สร้าง hook เชื่อมต่อกับ PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='SESAME-DB')
    
    # ชื่อตาราง
    ods_table = 'ods_Dealer_Transaction_Data'
    dwd_table = 'dwd_Dealer_Transaction_Data'
    
    # กำหนด key columns
    key_columns = ['MARKETING_CODE', 'CUSTOMER_ID','UNITHOLDER_NO','FUND_CODE', 'ORDER_DATE', 'ORDER_TYPE']
    
    # ดึงข้อมูลจำนวนแถวล่าสุดที่โหลดเข้า ODS
    rows_loaded = context['ti'].xcom_pull(key='processed_row_count')
    
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                # ดึงรายชื่อคอลัมน์จาก ODS
                cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{ods_table.lower()}'
                ORDER BY ordinal_position
                """)
                all_columns = [row[0] for row in cur.fetchall()]
                
                # สร้าง temporary table เพื่อเก็บข้อมูลใหม่ล่าสุดที่โหลดเข้า ODS
                cur.execute(f"""
                CREATE TEMP TABLE temp_new_data AS
                SELECT * FROM {ods_table}
                ORDER BY ctid DESC
                LIMIT {rows_loaded}
                """)
                
                # อัปเดตข้อมูลเดิมใน DWD ด้วยข้อมูลใหม่จาก temp_new_data
                update_conditions = " AND ".join([f"dwd.{key} = temp.{key}" for key in key_columns])
                update_sets = ", ".join([f"{col} = temp.{col}" for col in all_columns if col not in key_columns])
                
                update_sql = f"""
                UPDATE {dwd_table} dwd
                SET {update_sets}
                FROM temp_new_data temp
                WHERE {update_conditions}
                """
                cur.execute(update_sql)
                updated_rows = cur.rowcount
                
                # เพิ่มข้อมูลใหม่ที่ไม่มีใน DWD
                insert_conditions = " AND ".join([f"temp.{key} = dwd.{key}" for key in key_columns])
                column_list = ", ".join(all_columns)
                
                insert_sql = f"""
                INSERT INTO {dwd_table} ({column_list})
                SELECT {column_list}
                FROM temp_new_data temp
                WHERE NOT EXISTS (
                    SELECT 1 FROM {dwd_table} dwd
                    WHERE {insert_conditions}
                )
                """
                cur.execute(insert_sql)
                inserted_rows = cur.rowcount
                
                # ลบ temporary table
                cur.execute("DROP TABLE temp_new_data")
                
                conn.commit()
                print(f"อัปเดต {updated_rows} แถว และเพิ่ม {inserted_rows} แถวใหม่ในตาราง {dwd_table}")
                
            except Exception as e:
                conn.rollback()
                print(f"เกิดข้อผิดพลาดในการอัปเดตหรือเพิ่มข้อมูลในตาราง {dwd_table}: {e}")
                raise

# DAG configuration
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
    'ETL_Dealer_Transaction_Data',
    default_args=default_args,
    description='Transfer Dealer Transaction Data from MSSQL to PostgreSQL ODS and DWD',
    schedule_interval='0 12 * * *',  
    catchup=False
) as dag:
    
    # ดึงข้อมูลจาก MSSQL
    extract_task = PythonOperator(
        task_id='extract_data_from_mssql',
        python_callable=extract_data_from_mssql,
        provide_context=True
    )
    
    # โหลดข้อมูลไปยัง ODS
    load_to_ods_task = PythonOperator(
        task_id='load_data_to_ods',
        python_callable=load_data_to_ods,
        provide_context=True
    )
    
    # อัปเดตหรือเพิ่มข้อมูลใน DWD
    upsert_to_dwd_task = PythonOperator(
        task_id='upsert_data_to_dwd',
        python_callable=upsert_data_to_dwd,
        provide_context=True
    )
    
    # กำหนดลำดับการทำงาน
    extract_task >> load_to_ods_task >> upsert_to_dwd_task