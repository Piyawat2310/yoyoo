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

# Set up more comprehensive logging
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

def save_debug_info(response, soup):
    """Save debug information when scraping fails"""
    debug_info = {
        'timestamp': datetime.now().isoformat(),
        'status_code': response.status_code,
        'headers': dict(response.headers),
        'content_length': len(response.content)
    }
    
    try:
        with open('/tmp/gold_debug.json', 'w', encoding='utf-8') as f:
            json.dump(debug_info, f, ensure_ascii=False, indent=2)
            
        with open('/tmp/gold_response.html', 'w', encoding='utf-8') as f:
            f.write(response.text)
            
        with open('/tmp/gold_parsed.txt', 'w', encoding='utf-8') as f:
            f.write(str(soup.prettify()))
    except Exception as e:
        logger.error(f"Failed to save debug info: {str(e)}")

def convert_thai_date(thai_date_str):
    """Convert Thai Buddhist date to Gregorian date"""
    try:
        date_part, time_part = thai_date_str.split(' ')
        day, month, buddhist_year = map(int, date_part.split('/'))
        hour, minute = map(int, time_part.split(':'))
        gregorian_year = buddhist_year - 543
        return datetime(gregorian_year, month, day, hour, minute).strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        logger.error(f"วันที่ไม่ถูกต้อง: {thai_date_str}, Error: {str(e)}")
        return None

def fetch_gold_prices(**context):
    max_attempts = 3
    session = requests.Session()
    
    # Set up retry strategy
    retry_strategy = Retry(
        total=5,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
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
        'Cache-Control': 'max-age=0',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Referer': 'https://www.goldtraders.or.th/'
    }

    for attempt in range(max_attempts):
        try:
            logger.info(f"Attempt {attempt + 1} of {max_attempts}")
            
            response = session.get(
                "https://www.goldtraders.or.th/UpdatePriceList.aspx",
                headers=headers,
                timeout=(10, 30),
                verify=certifi.where()
            )
            
            logger.info(f"Response status code: {response.status_code}")
            logger.info(f"Response headers: {dict(response.headers)}")
            
            # Log response size
            content_length = len(response.content)
            logger.info(f"Response content length: {content_length} bytes")
            
            response.raise_for_status()
            
            # Initial parse
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Try different table identifiers
            table = soup.find('table', {'id': 'DetailPlace_MainGridView'})
            if not table:
                table = soup.find('table', {'id': 'ctl00_DetailPlace_MainGridView'})
            if not table:
                table = soup.find('table', class_='table-price')
                
            if not table:
                logger.warning(f"Table not found on attempt {attempt + 1}")
                logger.warning(f"Status code: {response.status_code}")
                logger.warning(f"Content type: {response.headers.get('content-type', 'Not specified')}")
                
                # Save HTML content for debugging
                debug_file = f'/tmp/gold_debug_attempt_{attempt + 1}.html'
                with open(debug_file, 'w', encoding='utf-8') as f:
                    f.write(response.text)
                logger.warning(f"Debug HTML saved to: {debug_file}")
                
                save_debug_info(response, soup)
                time.sleep(5)  # Wait before retry
                continue

            rows = table.find_all('tr')[1:]  # Skip header row
            data = []
            fetch_time = datetime.now()

            for row in rows:
                cols = [col.text.strip() for col in row.find_all('td')]
                if len(cols) < 9:
                    logger.warning(f"Invalid row length: {len(cols)}, expected 9+")
                    logger.warning(f"Row content: {cols}")
                    continue

                converted_time = convert_thai_date(cols[0])
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
                    logger.error(f"Error parsing row: {cols}. Error: {str(e)}")
                    continue

            if not data:
                logger.warning(f"No data found on attempt {attempt + 1}")
                logger.warning(f"Status code: {response.status_code}")
                logger.warning(f"Found {len(rows)} rows in table")
                time.sleep(5)  # Wait before retry
                continue

            logger.info(f"Successfully fetched {len(data)} rows")
            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"Request failed on attempt {attempt + 1}: {str(e)}")
            if hasattr(e, 'response') and e.response is not None:
                logger.error(f"Status code: {e.response.status_code}")
                logger.error(f"Response headers: {dict(e.response.headers)}")
            if attempt == max_attempts - 1:
                raise
            time.sleep(5)  # Wait before retry

        except Exception as e:
            logger.error(f"Unexpected error on attempt {attempt + 1}: {str(e)}")
            logger.error(traceback.format_exc())
            if attempt == max_attempts - 1:
                raise
            time.sleep(5)  # Wait before retry

    error_msg = f"Failed to fetch data after {max_attempts} attempts"
    logger.error(error_msg)
    raise ValueError(error_msg)

def save_to_postgres(**context):
    try:
        data = context['ti'].xcom_pull(task_ids='fetch_gold_prices')
        if not data:
            raise ValueError("No data available to save")

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
                successful_inserts = 0
                for row in data:
                    try:
                        cursor.execute(insert_query, (
                            row['fetch_time'],
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
                    except Exception as e:
                        logger.error(f"Error inserting row: {row}. Error: {str(e)}")
                        conn.rollback()
                        continue
                    
                conn.commit()
                logger.info(f"Successfully inserted {successful_inserts} rows")

    except Exception as e:
        logger.error(f"Error in save_to_postgres: {str(e)}")
        logger.error(traceback.format_exc())
        raise

def aggregate_gold_prices(**context):
    try:
        pg_hook = PostgresHook(postgres_conn_id='SESAME-DB')
        aggregate_query = """
            INSERT INTO dwd_goldgta_dtl_di (
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
                FROM ods_goldgta_dtl_di
                WHERE time::DATE = CURRENT_DATE
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
                cursor.execute(aggregate_query)
                conn.commit()
                logger.info("Successfully aggregated gold prices")

    except Exception as e:
        logger.error(f"Error in aggregate_gold_prices: {str(e)}")
        logger.error(traceback.format_exc())
        raise

with DAG(
    'ETL_goldgta_dtl_di',
    default_args=default_args,
    description='Scrape gold prices and save to PostgreSQL with enhanced error handling',
    schedule_interval='0 18 * * *',  # รันทุกวันเวลา 18:00 น.
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







