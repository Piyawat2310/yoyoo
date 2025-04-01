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

def fetch_gold_api_data(**context):
    try:
        api_key = 'goldapi-346whsm6hkeukw-io'
        headers = {
            'x-access-token': api_key,
            'Content-Type': 'application/json'
        }

        all_data = []
        execution_date = context['execution_date']
        inc_day = execution_date.strftime('%Y%m%d')
        
        logger.info(f"Starting data fetch for execution_date: {execution_date}, inc_day: {inc_day}")

        # สร้างลิสต์ของวันที่ต้องการดึงข้อมูล (7 วัน)
        target_dates = []
        for i in range(7):
            date = execution_date - timedelta(days=i)
            target_dates.append(date.strftime('%Y%m%d'))
        
        logger.info(f"Target dates to fetch: {target_dates}")

        for date in target_dates:
            api_url = f'https://www.goldapi.io/api/XAU/THB/{date}'
            
            logger.info(f"Fetching data for date: {date}")
            try:
                response = requests.get(api_url, headers=headers)
                response.raise_for_status()
                data = response.json()

                if 'error' in data:
                    logger.warning(f"API Error for date {date}: {data['error']}")
                    continue

                timestamp = datetime.fromtimestamp(data.get('timestamp', 0) / 1000)
                open_time = datetime.fromtimestamp(data.get('open_time', 0) / 1000) if data.get('open_time') else timestamp

                base_data = {
                    'timestamp': timestamp,
                    'golddate': date,
                    'metal': data.get('metal', 'XAU'),
                    'currency': data.get('currency', 'THB'),
                    'exchange': data.get('exchange', 'IDC'),
                    'symbol': 'FX_IDC:XAUTHB',
                    'prev_close_price': data.get('prev_close_price', 0),
                    'open_price': data.get('open_price', 0),
                    'low_price': data.get('low_price', 0),
                    'high_price': data.get('high_price', 0),
                    'open_time': open_time,
                    'price': data.get('price', 0),
                    'ch': data.get('ch', 0),
                    'chp': data.get('chp', 0),
                    'price_gram_24k': data.get('price_gram_24k', 0),
                    'price_gram_22k': data.get('price_gram_22k', 0),
                    'price_gram_21k': data.get('price_gram_21k', 0),
                    'price_gram_20k': data.get('price_gram_20k', 0),
                    'price_gram_18k': data.get('price_gram_18k', 0),
                    'price_gram_16k': data.get('price_gram_16k', 0),
                    'price_gram_14k': data.get('price_gram_14k', 0),
                    'price_gram_10k': data.get('price_gram_10k', 0),
                    'inc_day': inc_day
                }

                all_data.append(base_data)
                logger.info(f"Successfully processed data for date: {date}")

            except requests.exceptions.RequestException as e:
                logger.error(f"Failed to fetch data for date {date}: {str(e)}")
                continue

        if not all_data:
            logger.warning("No complete data available for processing")
            return

        df = pd.DataFrame(all_data)

        # จัดลำดับคอลัมน์ให้ตรงกับฐานข้อมูล
        columns = [
            'timestamp', 'golddate', 'metal', 'currency', 'exchange', 'symbol',
            'prev_close_price', 'open_price', 'low_price', 'high_price',
            'open_time', 'price', 'ch', 'chp', 'price_gram_24k',
            'price_gram_22k', 'price_gram_21k', 'price_gram_20k',
            'price_gram_18k', 'price_gram_16k', 'price_gram_14k',
            'price_gram_10k', 'inc_day'
        ]
        df = df[columns]

        logger.info("Connecting to database...")
        pg_hook = PostgresHook(postgres_conn_id='SESAME-DB')
        conn = pg_hook.get_conn()

        try:
            with conn.cursor() as cur:
                # ลบข้อมูลเก่าที่มี inc_day เดียวกันใน ODS
                cur.execute(
                    "DELETE FROM ods_goldapi_dtl_di WHERE inc_day = %s",
                    (inc_day,)
                )
                logger.info(f"Deleted records with inc_day: {inc_day}")

                # นำเข้าข้อมูลใหม่ใน ODS
                from io import StringIO
                output = StringIO()
                df.to_csv(output, sep='\t', header=False, index=False, na_rep='NULL')
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
                    ) FROM STDIN WITH CSV DELIMITER E'\t' NULL 'NULL'
                    """,
                    output
                )
                logger.info("Successfully loaded data to ODS")

                # อัปเดต DWD แบบ row by row โดยกำหนดให้ inc_day เท่ากับ golddate
                for _, row in df.iterrows():
                    dwd_data = row.to_dict()
                    dwd_data['inc_day'] = dwd_data['golddate']  # กำหนดให้ inc_day ใน DWD เท่ากับ golddate
                    
                    cur.execute(
                        """
                        INSERT INTO dwd_goldapi_dtl_di (
                            golddate, timestamp, metal, currency, exchange, symbol,
                            prev_close_price, open_price, low_price, high_price,
                            open_time, price, ch, chp, price_gram_24k,
                            price_gram_22k, price_gram_21k, price_gram_20k,
                            price_gram_18k, price_gram_16k, price_gram_14k,
                            price_gram_10k, inc_day
                        ) VALUES (
                            %(golddate)s, %(timestamp)s, %(metal)s, %(currency)s,
                            %(exchange)s, %(symbol)s, %(prev_close_price)s, 
                            %(open_price)s, %(low_price)s, %(high_price)s,
                            %(open_time)s, %(price)s, %(ch)s, %(chp)s,
                            %(price_gram_24k)s, %(price_gram_22k)s,
                            %(price_gram_21k)s, %(price_gram_20k)s,
                            %(price_gram_18k)s, %(price_gram_16k)s,
                            %(price_gram_14k)s, %(price_gram_10k)s,
                            %(inc_day)s
                        )
                        ON CONFLICT (golddate) 
                        DO UPDATE SET
                            timestamp = EXCLUDED.timestamp,
                            metal = EXCLUDED.metal,
                            currency = EXCLUDED.currency,
                            exchange = EXCLUDED.exchange,
                            symbol = EXCLUDED.symbol,
                            prev_close_price = EXCLUDED.prev_close_price,
                            open_price = EXCLUDED.open_price,
                            low_price = EXCLUDED.low_price,
                            high_price = EXCLUDED.high_price,
                            open_time = EXCLUDED.open_time,
                            price = EXCLUDED.price,
                            ch = EXCLUDED.ch,
                            chp = EXCLUDED.chp,
                            price_gram_24k = EXCLUDED.price_gram_24k,
                            price_gram_22k = EXCLUDED.price_gram_22k,
                            price_gram_21k = EXCLUDED.price_gram_21k,
                            price_gram_20k = EXCLUDED.price_gram_20k,
                            price_gram_18k = EXCLUDED.price_gram_18k,
                            price_gram_16k = EXCLUDED.price_gram_16k,
                            price_gram_14k = EXCLUDED.price_gram_14k,
                            price_gram_10k = EXCLUDED.price_gram_10k,
                            inc_day = EXCLUDED.inc_day;
                        """,
                        dwd_data
                    )
                    logger.info(f"Updated DWD data for golddate: {dwd_data['golddate']}")

            conn.commit()
            logger.info("Successfully updated all data")

        except Exception as e:
            conn.rollback()
            logger.error(f"Database error: {str(e)}")
            raise e

    except Exception as e:
        logger.error(f"General error: {str(e)}")
        raise
    finally:
        if 'conn' in locals():
            conn.close()


dag = DAG(
    'etl_goldapi_dtl_di',
    default_args=default_args,
    description='DAG สำหรับดึงข้อมูลราคาทองคำย้อนหลัง 7 วัน',
    schedule_interval='0 18 * * *',
    catchup=False
)

fetch_task = PythonOperator(
    task_id='fetch_gold_api_data',
    python_callable=fetch_gold_api_data,
    provide_context=True,
    dag=dag
)







