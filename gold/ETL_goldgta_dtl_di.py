from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import logging

# Set up logging
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def convert_thai_date(thai_date_str):
    """ แปลงวันที่จาก '30/01/2568 14:06' เป็น '2025-01-30 14:06:00' """
    try:
        date_part, time_part = thai_date_str.split(' ')
        day, month, buddhist_year = map(int, date_part.split('/'))
        hour, minute = map(int, time_part.split(':'))
        gregorian_year = buddhist_year - 543  # แปลง พ.ศ. เป็น ค.ศ.
        return datetime(gregorian_year, month, day, hour, minute).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.error(f"Error converting date: {thai_date_str}, Error: {str(e)}")
        return None

def fetch_gold_prices(**context):
    try:
        url = "https://www.goldtraders.or.th/UpdatePriceList.aspx"
        headers = {'User-Agent': 'Mozilla/5.0'}
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        soup = BeautifulSoup(response.content, 'html.parser')
        table = soup.find('table', {'id': 'DetailPlace_MainGridView'})

        if not table:
            raise ValueError("ไม่พบตารางข้อมูลราคาทอง")

        rows = table.find_all('tr')[1:]  # ข้าม header
        data = []
        fetch_time = datetime.now()

        for row in rows:
            cols = [col.text.strip() for col in row.find_all('td')]
            if len(cols) >= 9:
                converted_time = convert_thai_date(cols[0])  # แปลงวันที่
                if not converted_time:
                    continue
                try:
                    data.append({
                        'fetch_time': fetch_time,
                        'time': converted_time,
                        'order_no': int(cols[1]),
                        'buy_price_bar': float(cols[2].replace(',', '')),
                        'sell_price_bar': float(cols[3].replace(',', '')),
                        'buy_price_shape': float(cols[4].replace(',', '')),
                        'sell_price_shape': float(cols[5].replace(',', '')),
                        'gold_spot': float(cols[6].replace(',', '')),
                        'exchange_rate': float(cols[7].replace(',', '')),
                        'price_change': float(cols[8].replace(',', ''))
                    })
                except (ValueError, IndexError) as e:
                    logger.error(f"Error processing row: {cols}. Error: {str(e)}")
                    continue

        if not data:
            raise ValueError("ไม่สามารถดึงข้อมูลได้")

        logger.info(f"Successfully fetched {len(data)} rows of gold price data")
        return data

    except Exception as e:
        logger.error(f"Error in fetch_gold_prices: {str(e)}")
        raise

def save_to_postgres(**context):
    try:
        data = context['ti'].xcom_pull(task_ids='fetch_gold_prices')
        if not data:
            raise ValueError("ไม่มีข้อมูล")

        pg_hook = PostgresHook(postgres_conn_id='SESAME-DB')
        insert_query = """
            INSERT INTO ODS_goldgta_dtl_di (
                fetch_time, time, order_no, buy_price_bar, sell_price_bar, 
                buy_price_shape, sell_price_shape, gold_spot, exchange_rate, price_change
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (fetch_time, time) DO NOTHING;
        """

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                for row in data:
                    try:
                        cursor.execute(insert_query, (
                            row['fetch_time'], row['time'], row['order_no'],
                            row['buy_price_bar'], row['sell_price_bar'],
                            row['buy_price_shape'], row['sell_price_shape'],
                            row['gold_spot'], row['exchange_rate'], row['price_change']
                        ))
                        conn.commit()
                    except Exception as e:
                        logger.error(f"Error inserting row: {row}. Error: {str(e)}")
                        conn.rollback()
                        continue

        logger.info(f"Successfully saved {len(data)} rows to PostgreSQL")

    except Exception as e:
        logger.error(f"Error in save_to_postgres: {str(e)}")
        raise

def aggregate_gold_prices(**context):
    try:
        pg_hook = PostgresHook(postgres_conn_id='SESAME-DB')
        aggregate_query = """
            SET datestyle = 'ISO, DMY';
            INSERT INTO dwd_goldgta_dtl_di (
                fetch_time, time, order_sum, 
                buy_price_bar_open, buy_price_bar_close, 
                sell_price_bar_open, sell_price_bar_close, 
                buy_price_shape_open, buy_price_shape_close, 
                sell_price_shape_open, sell_price_shape_close, 
                gold_spot, exchange_rate, price_change_sum
            )
            WITH daily_data AS (
                SELECT 
                    time::DATE as date_time,
                    order_no,
                    buy_price_bar,
                    sell_price_bar,
                    buy_price_shape,
                    sell_price_shape,
                    gold_spot,
                    exchange_rate,
                    price_change
                FROM ODS_goldgta_dtl_di
                WHERE time::DATE = CURRENT_DATE
            )
            SELECT 
                CURRENT_TIMESTAMP AS fetch_time,
                date_time AS time,
                COUNT(*) AS order_sum,
                MIN(buy_price_bar) AS buy_price_bar_open,
                MAX(buy_price_bar) AS buy_price_bar_close,
                MIN(sell_price_bar) AS sell_price_bar_open,
                MAX(sell_price_bar) AS sell_price_bar_close,
                MIN(buy_price_shape) AS buy_price_shape_open,
                MAX(buy_price_shape) AS buy_price_shape_close,
                MIN(sell_price_shape) AS sell_price_shape_open,
                MAX(sell_price_shape) AS sell_price_shape_close,
                MAX(gold_spot) AS gold_spot,
                MAX(exchange_rate) AS exchange_rate,
                SUM(price_change) AS price_change_sum
            FROM daily_data
            GROUP BY date_time
            ON CONFLICT (time) DO UPDATE SET
                order_sum = EXCLUDED.order_sum,
                buy_price_bar_open = EXCLUDED.buy_price_bar_open,
                buy_price_bar_close = EXCLUDED.buy_price_bar_close,
                sell_price_bar_open = EXCLUDED.sell_price_bar_open,
                sell_price_bar_close = EXCLUDED.sell_price_bar_close,
                buy_price_shape_open = EXCLUDED.buy_price_shape_open,
                buy_price_shape_close = EXCLUDED.buy_price_shape_close,
                sell_price_shape_open = EXCLUDED.sell_price_shape_open,
                sell_price_shape_close = EXCLUDED.sell_price_shape_close,
                gold_spot = EXCLUDED.gold_spot,
                exchange_rate = EXCLUDED.exchange_rate,
                price_change_sum = EXCLUDED.price_change_sum;
        """

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(aggregate_query)
                conn.commit()

        logger.info("Successfully aggregated gold prices")

    except Exception as e:
        logger.error(f"Error in aggregate_gold_prices: {str(e)}")
        raise

with DAG(
    'ETL_goldgta_dtl_di',
    default_args=default_args,
    description='Scrape gold prices and save to PostgreSQL',
    schedule_interval='0 18 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    fetch_task = PythonOperator(task_id='fetch_gold_prices', python_callable=fetch_gold_prices)
    save_task = PythonOperator(task_id='save_to_postgres', python_callable=save_to_postgres)
    aggregate_task = PythonOperator(task_id='aggregate_gold_prices', python_callable=aggregate_gold_prices)

    fetch_task >> save_task >> aggregate_task