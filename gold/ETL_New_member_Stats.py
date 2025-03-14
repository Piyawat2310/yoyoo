from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import pandas as pd

def extract_data_from_mssql_joindate(**context):
    """
    Extract data from Microsoft SQL Server for members with recent join dates
    """
    mssql_hook = MsSqlHook(mssql_conn_id='company_connection')
    
    query = """
    USE BWM_PLTDB;
    SELECT MEMBERID, 
           CONVERT(DATE, JOINEDDATE) AS JOINEDDATE, 
           JOINEDTIME, 
           CONVERT(DATE, LASTLOGINDATE) AS LASTLOGINDATE, 
           LASTLOGINTIME
    FROM MP_MEMBER
    WHERE ACTIVATED = 'A'
    AND CONVERT(DATE, JOINEDDATE) >= DATEADD(DAY, -15, GETDATE());
    """
    
    # ประมวลผลคิวรีและดึงผลลัพธ์เป็น DataFrame
    df = mssql_hook.get_pandas_df(query)
    
    # ส่งข้อมูล DataFrame ไปยัง XCom สำหรับงานถัดไป
    context['ti'].xcom_push(key='mssql_data_joindate', value=df)
    
    return df

def extract_data_from_mssql_logindate(**context):
    """
    Extract data from Microsoft SQL Server for members with recent login dates
    """
    mssql_hook = MsSqlHook(mssql_conn_id='company_connection')
    
    query = """
    USE BWM_PLTDB;
    SELECT MEMBERID, 
           CONVERT(DATE, JOINEDDATE) AS JOINEDDATE, 
           JOINEDTIME, 
           CONVERT(DATE, LASTLOGINDATE) AS LASTLOGINDATE, 
           LASTLOGINTIME
    FROM MP_MEMBER
    WHERE ACTIVATED = 'A'
    AND CONVERT(DATE, LASTLOGINDATE) >= DATEADD(DAY, -15, GETDATE());
    """
    
    # ประมวลผลคิวรีและดึงผลลัพธ์เป็น DataFrame
    df = mssql_hook.get_pandas_df(query)
    
    # ส่งข้อมูล DataFrame ไปยัง XCom สำหรับงานถัดไป
    context['ti'].xcom_push(key='mssql_data_logindate', value=df)
    
    return df

def preprocess_data(df):
    """
    Preprocess the data before loading to the database
    """
    # แปลงข้อมูลวันที่ก่อนโหลด
    df['JOINEDDATE'] = pd.to_datetime(df['JOINEDDATE'], errors='coerce')
    df['LASTLOGINDATE'] = pd.to_datetime(df['LASTLOGINDATE'], errors='coerce')
    
    # แทนที่ค่าว่างและค่าที่เป็น NaN ด้วย None
    df = df.where(pd.notnull(df), None)
    
    # แปลง DataFrame เป็น list ของ dictionary พร้อมแปลง timestamps เป็น strings
    data = []
    for _, row in df.iterrows():
        processed_row = {}
        for col, value in row.items():
            # จัดการกับ timestamps โดยแปลงเป็น string
            if isinstance(value, pd.Timestamp):
                processed_row[col] = value.strftime('%Y-%m-%d')
            elif pd.isna(value):
                processed_row[col] = None
            else:
                processed_row[col] = value
        data.append(processed_row)
    
    return data, list(df.columns)

def load_data_to_ods_joindate(**context):
    """
    Load data filtered by join date from Microsoft SQL Server to ODS table in PostgreSQL
    """
    # ดึง DataFrame จาก XCom
    df = context['ti'].xcom_pull(key='mssql_data_joindate')
    
    # ประมวลผลข้อมูล
    data, columns = preprocess_data(df)
    
    # สร้าง hook เชื่อมต่อกับ PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='SESAME-DB')
    
    # ชื่อตารางที่จะโหลดข้อมูล
    table_name = 'ods_New_member_Stats'
    
    # สร้าง SQL เพื่อ insert 
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s']*len(columns))})"
    
    # เปิดการเชื่อมต่อและ insert
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.executemany(sql, [[row.get(col, None) for col in columns] for row in data])
                conn.commit()
                print(f"บันทึกข้อมูลลงในตาราง {table_name} จำนวน {len(data)} แถว (JoinDate)")
            except Exception as e:
                conn.rollback()
                print(f"เกิดข้อผิดพลาดในการ insert ลงตาราง {table_name}: {e}")
                raise
    
    context['ti'].xcom_push(key='processed_columns_joindate', value=columns)
    context['ti'].xcom_push(key='processed_row_count_joindate', value=len(data))

def load_data_to_ods_logindate(**context):
    """
    Load data filtered by login date from Microsoft SQL Server to ODS table in PostgreSQL
    """
    # ดึง DataFrame จาก XCom
    df = context['ti'].xcom_pull(key='mssql_data_logindate')
    
    # ประมวลผลข้อมูล
    data, columns = preprocess_data(df)
    
    # สร้าง hook เชื่อมต่อกับ PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='SESAME-DB')
    
    # ชื่อตารางที่จะโหลดข้อมูล
    table_name = 'ods_New_member_Stats'
    
    # สร้าง SQL เพื่อ insert 
    sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(['%s']*len(columns))})"
    
    # เปิดการเชื่อมต่อและ insert
    with postgres_hook.get_conn() as conn:
        with conn.cursor() as cur:
            try:
                cur.executemany(sql, [[row.get(col, None) for col in columns] for row in data])
                conn.commit()
                print(f"บันทึกข้อมูลลงในตาราง {table_name} จำนวน {len(data)} แถว (LoginDate)")
            except Exception as e:
                conn.rollback()
                print(f"เกิดข้อผิดพลาดในการ insert ลงตาราง {table_name}: {e}")
                raise
    
    context['ti'].xcom_push(key='processed_columns_logindate', value=columns)
    context['ti'].xcom_push(key='processed_row_count_logindate', value=len(data))

def upsert_data_to_dwd(**context):
    """
    Update or insert data to DWD table in PostgreSQL
    Parameters:
    - source_key: The XCom key for the source data (joindate or logindate)
    """
    # ดึงพารามิเตอร์จาก task instance
    source_key = context['source_key']
    
    # สร้าง hook เชื่อมต่อกับ PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='SESAME-DB')
    
    # ชื่อตาราง
    ods_table = 'ods_New_member_Stats'
    dwd_table = 'dwd_New_member_Stats'
    
    # กำหนด key columns
    key_columns = ['MEMBERID']
    
    # คอลัมน์ที่ต้องการยกเว้นจาก ODS (ไม่ต้องการโอนไปยัง DWD)
    exclude_columns = ['id', 'load_timestamp', 'etl_load']
    
    # ดึงข้อมูลจำนวนแถวล่าสุดที่โหลดเข้า ODS
    rows_loaded = context['ti'].xcom_pull(key=f'processed_row_count_{source_key}')
    
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
                all_columns_ods = [row[0] for row in cur.fetchall()]
                
                # ดึงรายชื่อคอลัมน์จาก DWD
                cur.execute(f"""
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_name = '{dwd_table.lower()}'
                ORDER BY ordinal_position
                """)
                all_columns_dwd = [row[0] for row in cur.fetchall()]
                
                # กรองเฉพาะคอลัมน์ที่มีอยู่ทั้งใน ODS และ DWD (ตัดคอลัมน์ที่ต้องการยกเว้นออก)
                common_columns = [col for col in all_columns_ods 
                                 if col in all_columns_dwd and col.lower() not in [c.lower() for c in exclude_columns]]
                
                # ตรวจสอบว่าได้คอลัมน์ที่จะใช้เพียงพอหรือไม่
                if not common_columns:
                    raise ValueError("ไม่พบคอลัมน์ที่ต้องการสำหรับการอัพเดต DWD")
                
                # สร้าง temporary table เพื่อเก็บข้อมูลใหม่ล่าสุดที่โหลดเข้า ODS
                # เลือกเฉพาะคอลัมน์ที่มีอยู่ใน DWD และไม่อยู่ในรายการยกเว้น
                columns_for_temp = ", ".join(common_columns)
                cur.execute(f"""
                CREATE TEMP TABLE temp_new_data AS
                SELECT {columns_for_temp} 
                FROM {ods_table}
                ORDER BY ctid DESC
                LIMIT {rows_loaded}
                """)
                
                # ใช้วิธี ON CONFLICT DO UPDATE เพื่อรองรับการทำงานแบบ UPSERT
                # ซึ่งจะแก้ไขปัญหา unique constraint violation
                
                # อัปเดตข้อมูลเดิมใน DWD ด้วยข้อมูลใหม่จาก temp_new_data
                update_columns = [col for col in common_columns if col.lower() not in [k.lower() for k in key_columns]]
                if update_columns:  # ตรวจสอบว่ามีคอลัมน์ที่จะอัพเดตหรือไม่
                    update_sets = ", ".join([f"{col} = temp.{col}" for col in update_columns])
                    update_conditions = " AND ".join([f"dwd.{key} = temp.{key}" for key in key_columns])
                    
                    update_sql = f"""
                    UPDATE {dwd_table} dwd
                    SET {update_sets}
                    FROM temp_new_data temp
                    WHERE {update_conditions}
                    """
                    cur.execute(update_sql)
                    updated_rows = cur.rowcount
                else:
                    updated_rows = 0
                    print("ไม่มีคอลัมน์ที่จะอัพเดต")
                
                # เพิ่มข้อมูลใหม่ที่ไม่มีใน DWD โดยใช้ INSERT ON CONFLICT
                column_list = ", ".join(common_columns)
                values_list = ", ".join([f"EXCLUDED.{col}" for col in update_columns]) if update_columns else ""
                
                # ใช้ ON CONFLICT เพื่อจัดการกับ duplicate key
                insert_sql = f"""
                INSERT INTO {dwd_table} ({column_list})
                SELECT {column_list}
                FROM temp_new_data temp
                WHERE NOT EXISTS (
                    SELECT 1 FROM {dwd_table} dwd
                    WHERE {" AND ".join([f"dwd.{key} = temp.{key}" for key in key_columns])}
                )
                ON CONFLICT ({", ".join(key_columns)}) DO NOTHING
                """
                cur.execute(insert_sql)
                inserted_rows = cur.rowcount
                
                # ลบ temporary table
                cur.execute("DROP TABLE temp_new_data")
                
                conn.commit()
                print(f"อัปเดต {updated_rows} แถว และเพิ่ม {inserted_rows} แถวใหม่ในตาราง {dwd_table} ({source_key})")
                
            except Exception as e:
                conn.rollback()
                print(f"เกิดข้อผิดพลาดในการอัปเดตหรือเพิ่มข้อมูลในตาราง {dwd_table}: {e}")
                raise

def upsert_data_to_dwd_joindate(**context):
    """
    Update or insert data to DWD table in PostgreSQL for join date data
    """
    context['source_key'] = 'joindate'
    return upsert_data_to_dwd(**context)

def upsert_data_to_dwd_logindate(**context):
    """
    Update or insert data to DWD table in PostgreSQL for login date data
    """
    context['source_key'] = 'logindate'
    return upsert_data_to_dwd(**context)


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
    'ETL_New_member_Stats',
    default_args=default_args,
    description='Transfer Member Profile Data from MSSQL to PostgreSQL ODS and DWD',
    schedule_interval='0 12 * * *',  
    catchup=False
) as dag:
    
    # จุดเริ่มต้นของ DAG
    start = DummyOperator(
        task_id='start',
        dag=dag,
    )
    
    # แบ่งเส้นทางที่ 1: ข้อมูลตาม join date
    extract_joindate_task = PythonOperator(
        task_id='extract_data_by_joindate',
        python_callable=extract_data_from_mssql_joindate,
        provide_context=True
    )
    
    load_to_ods_joindate_task = PythonOperator(
        task_id='load_data_to_ods_joindate',
        python_callable=load_data_to_ods_joindate,
        provide_context=True
    )
    
    upsert_to_dwd_joindate_task = PythonOperator(
        task_id='upsert_data_to_dwd_joindate',
        python_callable=upsert_data_to_dwd_joindate,
        provide_context=True
    )
    
    # แบ่งเส้นทางที่ 2: ข้อมูลตาม login date
    extract_logindate_task = PythonOperator(
        task_id='extract_data_by_logindate',
        python_callable=extract_data_from_mssql_logindate,
        provide_context=True
    )
    
    load_to_ods_logindate_task = PythonOperator(
        task_id='load_data_to_ods_logindate',
        python_callable=load_data_to_ods_logindate,
        provide_context=True
    )
    
    upsert_to_dwd_logindate_task = PythonOperator(
        task_id='upsert_data_to_dwd_logindate',
        python_callable=upsert_data_to_dwd_logindate,
        provide_context=True
    )
    
    # จุดสิ้นสุดของ DAG
    end = DummyOperator(
        task_id='end',
        dag=dag,
    )
    
    # กำหนดลำดับการทำงาน
    # เริ่มจาก start → แยกเป็น 2 เส้นทาง → จบที่ end
    start >> [extract_joindate_task, extract_logindate_task]
    
    # เส้นทางที่ 1: joindate
    extract_joindate_task >> load_to_ods_joindate_task >> upsert_to_dwd_joindate_task >> end
    
    # เส้นทางที่ 2: logindate
    extract_logindate_task >> load_to_ods_logindate_task >> upsert_to_dwd_logindate_task >> end
