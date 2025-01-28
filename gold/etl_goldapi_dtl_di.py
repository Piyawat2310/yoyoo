# from airflow import DAG
# from airflow.operators.python_operator import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from datetime import datetime, timedelta
# import requests
# import pandas as pd
# import logging
# import json

# # ตั้งค่า logger
# logger = logging.getLogger(__name__)

# # ค่าเริ่มต้นสำหรับ DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2025, 1, 24),
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5)
# }

# def validate_api_data(data):
#     """
#     ตรวจสอบความถูกต้องของข้อมูลจาก API
#     """
#     required_fields = [
#         'timestamp', 'metal', 'currency', 'exchange', 'price',
#         'prev_close_price', 'ch', 'chp'
#     ]
    
#     if 'error' in data:
#         return False, f"API returned error: {data['error']}"

#     for field in required_fields:
#         if field not in data:
#             return False, f"Missing required field: {field}"
    
#     return True, None


# def fetch_gold_api_data(**context):
#     """
#     ดึงข้อมูลจาก Gold API ย้อนหลัง 7 วัน และเก็บข้อมูลเฉพาะวันที่มีข้อมูลที่ถูกต้อง
#     """
#     try:
#         api_key = 'goldapi-7o5hpsm6exf2ss-io'
#         headers = {
#             'x-access-token': api_key,
#             'Content-Type': 'application/json'
#         }

#         all_data = []
#         execution_date = context['execution_date']

#         # วันที่รัน DAG (inc_day)
#         inc_day = execution_date.strftime('%Y%m%d')

#         # ดึงข้อมูลย้อนหลัง 7 วัน
#         for i in range(7):
#             date = (execution_date - timedelta(days=i)).strftime('%Y%m%d')
#             api_url = f'https://www.goldapi.io/api/XAU/THB/{date}'
            
#             logger.info(f"กำลังดึงข้อมูลวันที่ {date}...")

#             try:
#                 response = requests.get(api_url, headers=headers)
#                 response.raise_for_status()
#                 data = response.json()

#                 # Validate API response
#                 is_valid, error_message = validate_api_data(data)
#                 if not is_valid:
#                     logger.warning(f"ข้อมูลไม่ถูกต้องสำหรับวันที่ {date}: {error_message}")
#                     continue  # ข้ามวันที่ไม่มีข้อมูล

#                 logger.info(f"ดึงข้อมูลวันที่ {date} สำเร็จ")

#                 # เพิ่มข้อมูลพื้นฐาน
#                 base_data = {
#                     'metal': data.get('metal', 'XAU'),
#                     'currency': data.get('currency', 'THB'),
#                     'exchange': data.get('exchange', 'IDC'),
#                     'symbol': 'FX_IDC:XAUTHB',
#                     'golddate': date,
#                     'inc_day': inc_day
#                 }

#                 # เพิ่มข้อมูลจาก API
#                 numeric_fields = [
#                     'timestamp', 'prev_close_price', 'open_price', 'low_price',
#                     'high_price', 'open_time', 'price', 'ch', 'chp',
#                     'price_gram_24k', 'price_gram_22k', 'price_gram_21k',
#                     'price_gram_20k', 'price_gram_18k', 'price_gram_16k',
#                     'price_gram_14k', 'price_gram_10k'
#                 ]

#                 for field in numeric_fields:
#                     base_data[field] = data.get(field, 0)

#                 all_data.append(base_data)

#             except requests.exceptions.RequestException as e:
#                 logger.error(f"ไม่สามารถดึงข้อมูลวันที่ {date} ได้: {str(e)}")
#                 continue  # ข้ามวันที่ไม่มีข้อมูล

#         if not all_data:
#             logger.warning("ไม่มีข้อมูลที่สมบูรณ์ในการบันทึก")
#             return

#         # แปลงข้อมูลเป็น DataFrame
#         df = pd.DataFrame(all_data)

#         # จัดเรียงคอลัมน์
#         columns = [
#             'timestamp', 'golddate', 'metal', 'currency', 'exchange', 'symbol',
#             'prev_close_price', 'open_price', 'low_price', 'high_price',
#             'open_time', 'price', 'ch', 'chp', 'price_gram_24k',
#             'price_gram_22k', 'price_gram_21k', 'price_gram_20k',
#             'price_gram_18k', 'price_gram_16k', 'price_gram_14k',
#             'price_gram_10k', 'inc_day'
#         ]
#         df = df[columns]

#         # เชื่อมต่อกับ PostgreSQL
#         logger.info("กำลังเชื่อมต่อกับฐานข้อมูล...")
#         pg_hook = PostgresHook(postgres_conn_id='SESAME-DB')
#         conn = pg_hook.get_conn()

#         try:
#             with conn.cursor() as cur:
#                 # ลบข้อมูลที่มีอยู่ในช่วงวันที่ที่จะอัพเดท
#                 cur.execute("""
#                     DELETE FROM ods_goldapi_dtl_di
#                     WHERE inc_day = %s;
#                 """, (inc_day,))

#                 # บันทึกข้อมูลใหม่
#                 logger.info("กำลังบันทึกข้อมูลลงในตาราง ods_goldapi_dtl_di...")

#                 from io import StringIO
#                 output = StringIO()
#                 df.to_csv(output, sep='\t', header=False, index=False)
#                 output.seek(0)

#                 cur.copy_expert(
#                     """
#                     COPY ods_goldapi_dtl_di (
#                         timestamp, golddate, metal, currency, exchange, symbol,
#                         prev_close_price, open_price, low_price, high_price,
#                         open_time, price, ch, chp, price_gram_24k,
#                         price_gram_22k, price_gram_21k, price_gram_20k,
#                         price_gram_18k, price_gram_16k, price_gram_14k,
#                         price_gram_10k, inc_day
#                     ) FROM STDIN WITH CSV DELIMITER E'\t'
#                     """,
#                     output
#                 )

#             conn.commit()
#             logger.info("บันทึกข้อมูลสำเร็จ")

#         except Exception as e:
#             conn.rollback()
#             raise e

#     except Exception as e:
#         logger.error(f"เกิดข้อผิดพลาด: {str(e)}")
#         raise
#     finally:
#         if 'conn' in locals():
#             conn.close()

# # สร้าง DAG
# dag = DAG(
#     'etl_goldapi_dtl_di',
#     default_args=default_args,
#     description='DAG สำหรับดึงข้อมูลราคาทองคำย้อนหลัง 7 วัน',
#     schedule_interval='0 11 * * *',  # ทำงานทุกวันเวลา 11:00 น.
#     catchup=False
# )

# # สร้าง Task
# fetch_task = PythonOperator(
#     task_id='fetch_gold_api_data',
#     python_callable=fetch_gold_api_data,
#     provide_context=True,
#     dag=dag
# )







from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
import pandas as pd
import logging
import json

# ตั้งค่า logger
logger = logging.getLogger(__name__)

# ค่าเริ่มต้นสำหรับ DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 24),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

def validate_api_data(data):
    required_fields = [
        'timestamp', 'metal', 'currency', 'exchange', 'price',
        'prev_close_price', 'ch', 'chp'
    ]
    if 'error' in data:
        return False, f"API returned error: {data['error']}"

    for field in required_fields:
        if field not in data:
            return False, f"Missing required field: {field}"
    
    return True, None

def fetch_gold_api_data(**context):
    try:
        api_key = 'goldapi-7o5hpsm6exf2ss-io'
        headers = {
            'x-access-token': api_key,
            'Content-Type': 'application/json'
        }

        all_data = []
        execution_date = context['execution_date']
        inc_day = execution_date.strftime('%Y%m%d')

        for i in range(7):
            date = (execution_date - timedelta(days=i)).strftime('%Y%m%d')
            api_url = f'https://www.goldapi.io/api/XAU/THB/{date}'

            logger.info(f"กำลังดึงข้อมูลวันที่ {date}...")
            try:
                response = requests.get(api_url, headers=headers)
                response.raise_for_status()
                data = response.json()

                is_valid, error_message = validate_api_data(data)
                if not is_valid:
                    logger.warning(f"ข้อมูลไม่ถูกต้องสำหรับวันที่ {date}: {error_message}")
                    continue

                logger.info(f"ดึงข้อมูลวันที่ {date} สำเร็จ")

                base_data = {
                    'metal': data.get('metal', 'XAU'),
                    'currency': data.get('currency', 'THB'),
                    'exchange': data.get('exchange', 'IDC'),
                    'symbol': 'FX_IDC:XAUTHB',
                    'golddate': date,
                    'inc_day': inc_day
                }

                numeric_fields = [
                    'timestamp', 'prev_close_price', 'open_price', 'low_price',
                    'high_price', 'open_time', 'price', 'ch', 'chp',
                    'price_gram_24k', 'price_gram_22k', 'price_gram_21k',
                    'price_gram_20k', 'price_gram_18k', 'price_gram_16k',
                    'price_gram_14k', 'price_gram_10k'
                ]

                for field in numeric_fields:
                    base_data[field] = data.get(field, 0)

                all_data.append(base_data)

            except requests.exceptions.RequestException as e:
                logger.error(f"ไม่สามารถดึงข้อมูลวันที่ {date} ได้: {str(e)}")
                continue

        if not all_data:
            logger.warning("ไม่มีข้อมูลที่สมบูรณ์ในการบันทึก")
            return

        df = pd.DataFrame(all_data)

        columns = [
            'timestamp', 'golddate', 'metal', 'currency', 'exchange', 'symbol',
            'prev_close_price', 'open_price', 'low_price', 'high_price',
            'open_time', 'price', 'ch', 'chp', 'price_gram_24k',
            'price_gram_22k', 'price_gram_21k', 'price_gram_20k',
            'price_gram_18k', 'price_gram_16k', 'price_gram_14k',
            'price_gram_10k', 'inc_day'
        ]
        df = df[columns]

        logger.info("กำลังเชื่อมต่อกับฐานข้อมูล...")
        pg_hook = PostgresHook(postgres_conn_id='SESAME-DB')
        conn = pg_hook.get_conn()

        try:
            with conn.cursor() as cur:
                logger.info("กำลังบันทึกข้อมูลลงในตาราง ods_goldapi_dtl_di...")

                from io import StringIO
                output = StringIO()
                df.to_csv(output, sep='\t', header=False, index=False)
                output.seek(0)

                cur.copy_expert(
                    """
                    COPY ods_goldapi_dtl_di (
                        timestamp, golddate, metal, currency, exchange, symbol,
                        prev_close_price, open_price, low_price, high_price,
                        open_time, price, ch, chp, price_gram_24k,
                        price_gram_22k, price_gram_21k, price_gram_20k,
                        price_gram_18k, price_gram_16k, price_gram_14k,
                        price_gram_10k, inc_day
                    ) FROM STDIN WITH CSV DELIMITER E'\t'
                    """,
                    output
                )

            conn.commit()
            logger.info("บันทึกข้อมูลสำเร็จ")

        except Exception as e:
            conn.rollback()
            logger.error(f"เกิดข้อผิดพลาดในการบันทึกข้อมูล: {str(e)}")
            raise e

    except Exception as e:
        logger.error(f"เกิดข้อผิดพลาด: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()

dag = DAG(
    'etl_goldapi_dtl_di',
    default_args=default_args,
    description='DAG สำหรับดึงข้อมูลราคาทองคำย้อนหลัง 7 วัน',
    schedule_interval='0 11 * * *',
    catchup=False
)

fetch_task = PythonOperator(
    task_id='fetch_gold_api_data',
    python_callable=fetch_gold_api_data,
    provide_context=True,
    dag=dag
)
