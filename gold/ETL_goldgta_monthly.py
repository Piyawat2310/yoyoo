from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta
import logging
import traceback
from urllib3.util.retry import Retry
from requests.adapters import HTTPAdapter
import ssl
import certifi
import time
import json

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# กำหนดช่วงวันที่ต้องการดึงข้อมูล
START_DATE = '2025-01-13'  # วันที่เริ่มต้น
END_DATE = '2025-02-13'    # วันที่สิ้นสุด

def get_date_range():
    """Generate list of dates between START_DATE and END_DATE"""
    start = datetime.strptime(START_DATE, '%Y-%m-%d')
    end = datetime.strptime(END_DATE, '%Y-%m-%d')
    date_list = []
    current = start
    while current <= end:
        date_list.append(current.strftime('%Y-%m-%d'))
        current += timedelta(days=1)
    return date_list
def convert_thai_date(thai_date_str):
    """Convert Thai Buddhist date to Gregorian date"""
    try:
        date_part, time_part = thai_date_str.split(' ')
        day, month, buddhist_year = map(int, date_part.split('/'))
        hour, minute = map(int, time_part.split(':'))
        
        gregorian_year = buddhist_year - 543
        converted_date = datetime(gregorian_year, month, day, hour, minute)
        
        # ตรวจสอบว่าวันที่อยู่ในช่วงที่ต้องการหรือไม่
        start_date = datetime.strptime(START_DATE, '%Y-%m-%d').date()
        end_date = datetime.strptime(END_DATE, '%Y-%m-%d').date()
        
        if start_date <= converted_date.date() <= end_date:
            return converted_date.strftime('%Y-%m-%d %H:%M:%S')
        return None
            
    except Exception as e:
        logger.error(f"Error converting date: {thai_date_str}, Error: {str(e)}")
        return None


def fetch_gold_prices(**context):
    all_data = []
    date_list = get_date_range()
    
    session = requests.Session()
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
    )
    
    adapter = HTTPAdapter(
        max_retries=retry_strategy,
        pool_connections=10,
        pool_maxsize=10
    )
    
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'th,en;q=0.9,en-GB;q=0.8,en-US;q=0.7',
        'Content-Type': 'application/x-www-form-urlencoded',
        'Origin': 'https://www.goldtraders.or.th',
        'Referer': 'https://www.goldtraders.or.th/UpdatePriceList.aspx'
    }

    for target_date in date_list:
        logger.info(f"Fetching data for date: {target_date}")
        
        # แปลงวันที่เป็นรูปแบบพุทธศักราช
        target_date_th = datetime.strptime(target_date, '%Y-%m-%d')
        thai_year = target_date_th.year + 543
        date_param = f"{target_date_th.day}/{target_date_th.month}/{thai_year}"
        
        form_data = {
            'ctl00$DetailPlace$txtDate': date_param,
            'ctl00$DetailPlace$btnSearch': 'ค้นหา'
        }
        
        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                url = "https://www.goldtraders.or.th/UpdatePriceList.aspx"
                response = session.post(
                    url,
                    headers=headers,
                    data=form_data,
                    timeout=(10, 30),
                    verify=certifi.where()
                )
                
                response.raise_for_status()
                soup = BeautifulSoup(response.content, 'html.parser')
                
                table = soup.find('table', {'id': 'DetailPlace_MainGridView'})
                if not table:
                    table = soup.find('table', {'id': 'ctl00_DetailPlace_MainGridView'})
                if not table:
                    table = soup.find('table', class_='table-price')
                
                if not table:
                    logger.warning(f"Table not found for date {target_date}")
                    continue

                rows = table.find_all('tr')[1:]
                fetch_time = datetime.now()

                for row in rows:
                    cols = [col.text.strip() for col in row.find_all('td')]
                    if len(cols) < 9:
                        continue

                    converted_time = convert_thai_date(cols[0])
                    if not converted_time:
                        continue

                    try:
                        all_data.append({
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
                        logger.error(f"Error parsing row: {cols}. Error: {str(e)}")
                        continue

                break  # ออกจากลูป attempt ถ้าสำเร็จ
                
            except Exception as e:
                logger.error(f"Error on attempt {attempt + 1} for date {target_date}: {str(e)}")
                if attempt == max_attempts - 1:
                    logger.error(f"Failed to fetch data for date {target_date} after {max_attempts} attempts")
                time.sleep(5)

        # รอสักครู่ก่อนดึงข้อมูลวันถัดไป
        time.sleep(2)

    if not all_data:
        raise ValueError(f"No data found for date range {START_DATE} to {END_DATE}")

    # เพิ่ม logging เพื่อดูข้อมูลที่ดึงมาได้
    logger.info(f"Fetched data details:")
    for row in all_data:
        logger.info(f"Time: {row['time']}, Order No: {row['order_no']}, Gold Price: {row['buy_price_bar']}")
    
    logger.info(f"Successfully fetched {len(all_data)} records")
    return all_data

def save_to_postgres(**context):
    try:
        data = context['ti'].xcom_pull(task_ids='fetch_gold_prices')
        if not data:
            raise ValueError("No data available to save")

        pg_hook = PostgresHook(postgres_conn_id='SESAME-DB')
        
        # ล้างข้อมูลของวันนี้ก่อน
        delete_query = """
        DELETE FROM ods_goldgtatest 
        WHERE time::date = %s::date
        """
        
        # แก้ไข query ให้ไม่ใช้ ON CONFLICT
        insert_query = """
        INSERT INTO ods_goldgtatest (
            fetch_time, time, order_no, buy_price_bar, sell_price_bar, 
            buy_price_shape, sell_price_shape, gold_spot, exchange_rate, price_change
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # ล้างข้อมูลเก่าก่อน
                target_date = context['execution_date'].strftime('%Y-%m-%d')
                cursor.execute(delete_query, (target_date,))
                
                successful_inserts = 0
                for row in data:
                    try:
                        # สร้าง fetch_time ใหม่สำหรับแต่ละแถว
                        current_fetch_time = datetime.now()
                        
                        cursor.execute(insert_query, (
                            current_fetch_time,  # ใช้เวลาปัจจุบันแทน
                            row['time'],
                            row['order_no'],
                            row['buy_price_bar'],
                            row['sell_price_bar'],
                            row['buy_price_shape'],
                            row['sell_price_shape'],
                            row['gold_spot'],
                            row['exchange_rate'],
                            row['price_change']
                        ))
                        successful_inserts += 1
                        logger.info(f"Inserted row: time={row['time']}, order_no={row['order_no']}")
                    except Exception as e:
                        logger.error(f"Error inserting row: {row}. Error: {str(e)}")
                        continue
                
                conn.commit()
                logger.info(f"Successfully inserted {successful_inserts} rows")

    except Exception as e:
        logger.error(f"Error in save_to_postgres: {str(e)}")
        raise
    
def aggregate_gold_prices(**context):
    try:
        # รับค่าวันที่จาก execution_date
        target_date = context['execution_date'].strftime('%Y-%m-%d')
        
        pg_hook = PostgresHook(postgres_conn_id='SESAME-DB')
        aggregate_query = """
            INSERT INTO dwd_goldgtatest (
                fetch_time,
                time,
                order_sum,
                buy_price_bar_open,
                buy_price_bar_close,
                sell_price_bar_open,
                sell_price_bar_close,
                buy_price_shape_open,
                buy_price_shape_close,
                sell_price_shape_open,
                sell_price_shape_close,
                gold_spot_open,
                gold_spot_close,
                exchange_rate_open,
                exchange_rate_close,
                price_change_sum
            )
            WITH daily_data AS (
                SELECT 
                    time::DATE as date_time,
                    MIN(buy_price_bar) as first_buy_price_bar,
                    MAX(buy_price_bar) as last_buy_price_bar,
                    MIN(sell_price_bar) as first_sell_price_bar,
                    MAX(sell_price_bar) as last_sell_price_bar,
                    MIN(buy_price_shape) as first_buy_price_shape,
                    MAX(buy_price_shape) as last_buy_price_shape,
                    MIN(sell_price_shape) as first_sell_price_shape,
                    MAX(sell_price_shape) as last_sell_price_shape,
                    MIN(gold_spot) as first_gold_spot,
                    MAX(gold_spot) as last_gold_spot,
                    MIN(exchange_rate) as first_exchange_rate,
                    MAX(exchange_rate) as last_exchange_rate,
                    COUNT(*) as order_count,
                    SUM(price_change) as total_price_change
                FROM ods_goldgtatest
                WHERE time::DATE = %s::DATE
                GROUP BY time::DATE
            )
            SELECT 
                CURRENT_TIMESTAMP as fetch_time,
                date_time as time,
                order_count as order_sum,
                first_buy_price_bar as buy_price_bar_open,
                last_buy_price_bar as buy_price_bar_close,
                first_sell_price_bar as sell_price_bar_open,
                last_sell_price_bar as sell_price_bar_close,
                first_buy_price_shape as buy_price_shape_open,
                last_buy_price_shape as buy_price_shape_close,
                first_sell_price_shape as sell_price_shape_open,
                last_sell_price_shape as sell_price_shape_close,
                first_gold_spot as gold_spot_open,
                last_gold_spot as gold_spot_close,
                first_exchange_rate as exchange_rate_open,
                last_exchange_rate as exchange_rate_close,
                total_price_change as price_change_sum
            FROM daily_data
            ON CONFLICT (time) DO UPDATE SET
                fetch_time = EXCLUDED.fetch_time,
                order_sum = EXCLUDED.order_sum,
                buy_price_bar_open = EXCLUDED.buy_price_bar_open,
                buy_price_bar_close = EXCLUDED.buy_price_bar_close,
                sell_price_bar_open = EXCLUDED.sell_price_bar_open,
                sell_price_bar_close = EXCLUDED.sell_price_bar_close,
                buy_price_shape_open = EXCLUDED.buy_price_shape_open,
                buy_price_shape_close = EXCLUDED.buy_price_shape_close,
                sell_price_shape_open = EXCLUDED.sell_price_shape_open,
                sell_price_shape_close = EXCLUDED.sell_price_shape_close,
                gold_spot_open = EXCLUDED.gold_spot_open,
                gold_spot_close = EXCLUDED.gold_spot_close,
                exchange_rate_open = EXCLUDED.exchange_rate_open,
                exchange_rate_close = EXCLUDED.exchange_rate_close,
                price_change_sum = EXCLUDED.price_change_sum;
        """

        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(aggregate_query, (target_date,))
                conn.commit()
                logger.info(f"Successfully aggregated gold prices for date {target_date}")

    except Exception as e:
        logger.error(f"Error in aggregate_gold_prices: {str(e)}")
        raise
    
with DAG(
    'ETL_goldgta_monthly',
    default_args=default_args,
    description='Scrape gold prices for date range and save to PostgreSQL',
    schedule_interval='0 18 * * *',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    max_active_runs=1
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_gold_prices',
        python_callable=fetch_gold_prices,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    save_task = PythonOperator(
        task_id='save_to_postgres',
        python_callable=save_to_postgres,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    aggregate_task = PythonOperator(
        task_id='aggregate_gold_prices',
        python_callable=aggregate_gold_prices,
        retries=3,
        retry_delay=timedelta(minutes=1)
    )

    fetch_task >> save_task >> aggregate_task
    