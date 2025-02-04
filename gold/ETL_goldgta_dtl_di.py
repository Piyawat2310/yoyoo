# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from bs4 import BeautifulSoup
# import requests
# from datetime import datetime, timedelta
# import logging
# from urllib3.util.retry import Retry
# from requests.adapters import HTTPAdapter

# # Set up logging
# logger = logging.getLogger(__name__)

# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'email_on_failure': False,
#     'email_on_retry': False,
#     'retries': 1,
#     'retry_delay': timedelta(minutes=5),
# }

# def convert_thai_date(thai_date_str):
#     try:
#         date_part, time_part = thai_date_str.split(' ')
#         day, month, buddhist_year = map(int, date_part.split('/'))
#         hour, minute = map(int, time_part.split(':'))
#         gregorian_year = buddhist_year - 543
#         return datetime(gregorian_year, month, day, hour, minute).strftime('%Y-%m-%d %H:%M:%S')
#     except Exception as e:
#         logger.error(f"Error converting date: {thai_date_str}, Error: {str(e)}")
#         return None

# def fetch_gold_prices(**context):
#     try:
#         url = "https://www.goldtraders.or.th/UpdatePriceList.aspx"
#         headers = {
#             'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
#             'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
#             'Accept-Language': 'en-US,en;q=0.5',
#             'Connection': 'keep-alive',
#         }

#         # Set up session with retry strategy
#         session = requests.Session()
#         retry_strategy = Retry(
#             total=5,
#             backoff_factor=1,
#             status_forcelist=[500, 502, 503, 504]
#         )
#         adapter = HTTPAdapter(max_retries=retry_strategy)
#         session.mount("http://", adapter)
#         session.mount("https://", adapter)

#         try:
#             response = session.get(
#                 url,
#                 headers=headers,
#                 timeout=30,
#                 verify=False  # Only if necessary
#             )
#             response.raise_for_status()
#         except requests.exceptions.RequestException as e:
#             logger.error(f"Failed to fetch from primary URL: {str(e)}")
#             raise

#         soup = BeautifulSoup(response.content, 'html.parser')
#         table = soup.find('table', {'id': 'DetailPlace_MainGridView'})

#         if not table:
#             raise ValueError("ไม่พบตารางข้อมูลราคาทอง")

#         rows = table.find_all('tr')[1:]
#         data = []
#         fetch_time = datetime.now()

#         for row in rows:
#             cols = [col.text.strip() for col in row.find_all('td')]
#             if len(cols) >= 9:
#                 converted_time = convert_thai_date(cols[0])
#                 if not converted_time:
#                     continue
#                 try:
#                     data.append({
#                         'fetch_time': fetch_time,
#                         'time': converted_time,
#                         'order_no': int(cols[1]),
#                         'buy_price_bar': float(cols[2].replace(',', '')),
#                         'sell_price_bar': float(cols[3].replace(',', '')),
#                         'buy_price_shape': float(cols[4].replace(',', '')),
#                         'sell_price_shape': float(cols[5].replace(',', '')),
#                         'gold_spot': float(cols[6].replace(',', '')),
#                         'exchange_rate': float(cols[7].replace(',', '')),
#                         'price_change': float(cols[8].replace(',', ''))
#                     })
#                 except (ValueError, IndexError) as e:
#                     logger.error(f"Error processing row: {cols}. Error: {str(e)}")
#                     continue

#         if not data:
#             raise ValueError("ไม่สามารถดึงข้อมูลได้")

#         logger.info(f"Successfully fetched {len(data)} rows of gold price data")
#         return data

#     except Exception as e:
#         logger.error(f"Error in fetch_gold_prices: {str(e)}")
#         raise

# def save_to_postgres(**context):
#     try:
#         data = context['ti'].xcom_pull(task_ids='fetch_gold_prices')
#         if not data:
#             raise ValueError("ไม่มีข้อมูล")

#         pg_hook = PostgresHook(postgres_conn_id='SESAME-DB')
#         insert_query = """
#             INSERT INTO ODS_goldgta_dtl_di (
#                 fetch_time, time, order_no, buy_price_bar, sell_price_bar, 
#                 buy_price_shape, sell_price_shape, gold_spot, exchange_rate, price_change
#             )
#             VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
#             ON CONFLICT (fetch_time, time) DO NOTHING;
#         """

#         with pg_hook.get_conn() as conn:
#             with conn.cursor() as cursor:
#                 for row in data:
#                     try:
#                         cursor.execute(insert_query, (
#                             row['fetch_time'],
#                             row['time'],
#                             row['order_no'],
#                             row['buy_price_bar'],
#                             row['sell_price_bar'],
#                             row['buy_price_shape'],
#                             row['sell_price_shape'],
#                             row['gold_spot'],
#                             row['exchange_rate'],
#                             row['price_change']
#                         ))
#                         conn.commit()
#                     except Exception as e:
#                         logger.error(f"Error inserting row: {row}. Error: {str(e)}")
#                         conn.rollback()
#                         continue

#         logger.info(f"Successfully saved {len(data)} rows to PostgreSQL")

#     except Exception as e:
#         logger.error(f"Error in save_to_postgres: {str(e)}")
#         raise

# def aggregate_gold_prices(**context):
#     try:
#         pg_hook = PostgresHook(postgres_conn_id='SESAME-DB')
#         aggregate_query = """
#             INSERT INTO dwd_goldgta_dtl_di (
#                 fetch_time,
#                 time,
#                 order_sum,
#                 buy_price_bar_open,
#                 buy_price_bar_close,
#                 sell_price_bar_open,
#                 sell_price_bar_close,
#                 buy_price_shape_open,
#                 buy_price_shape_close,
#                 sell_price_shape_open,
#                 sell_price_shape_close,
#                 gold_spot_open,
#                 gold_spot_close,
#                 exchange_rate_open,
#                 exchange_rate_close,
#                 price_change_sum
#             )
#             WITH daily_data AS (
#                 SELECT 
#                     time::DATE as date_time,
#                     MIN(buy_price_bar) as first_buy_price_bar,
#                     MAX(buy_price_bar) as last_buy_price_bar,
#                     MIN(sell_price_bar) as first_sell_price_bar,
#                     MAX(sell_price_bar) as last_sell_price_bar,
#                     MIN(buy_price_shape) as first_buy_price_shape,
#                     MAX(buy_price_shape) as last_buy_price_shape,
#                     MIN(sell_price_shape) as first_sell_price_shape,
#                     MAX(sell_price_shape) as last_sell_price_shape,
#                     MIN(gold_spot) as first_gold_spot,
#                     MAX(gold_spot) as last_gold_spot,
#                     MIN(exchange_rate) as first_exchange_rate,
#                     MAX(exchange_rate) as last_exchange_rate,
#                     COUNT(*) as order_count,
#                     SUM(price_change) as total_price_change
#                 FROM ods_goldgta_dtl_di
#                 WHERE time::DATE = CURRENT_DATE
#                 GROUP BY time::DATE
#             )
#             SELECT 
#                 CURRENT_TIMESTAMP as fetch_time,
#                 date_time as time,
#                 order_count as order_sum,
#                 first_buy_price_bar as buy_price_bar_open,
#                 last_buy_price_bar as buy_price_bar_close,
#                 first_sell_price_bar as sell_price_bar_open,
#                 last_sell_price_bar as sell_price_bar_close,
#                 first_buy_price_shape as buy_price_shape_open,
#                 last_buy_price_shape as buy_price_shape_close,
#                 first_sell_price_shape as sell_price_shape_open,
#                 last_sell_price_shape as sell_price_shape_close,
#                 first_gold_spot as gold_spot_open,
#                 last_gold_spot as gold_spot_close,
#                 first_exchange_rate as exchange_rate_open,
#                 last_exchange_rate as exchange_rate_close,
#                 total_price_change as price_change_sum
#             FROM daily_data
#             ON CONFLICT (time) DO UPDATE SET
#                 fetch_time = EXCLUDED.fetch_time,
#                 order_sum = EXCLUDED.order_sum,
#                 buy_price_bar_open = EXCLUDED.buy_price_bar_open,
#                 buy_price_bar_close = EXCLUDED.buy_price_bar_close,
#                 sell_price_bar_open = EXCLUDED.sell_price_bar_open,
#                 sell_price_bar_close = EXCLUDED.sell_price_bar_close,
#                 buy_price_shape_open = EXCLUDED.buy_price_shape_open,
#                 buy_price_shape_close = EXCLUDED.buy_price_shape_close,
#                 sell_price_shape_open = EXCLUDED.sell_price_shape_open,
#                 sell_price_shape_close = EXCLUDED.sell_price_shape_close,
#                 gold_spot_open = EXCLUDED.gold_spot_open,
#                 gold_spot_close = EXCLUDED.gold_spot_close,
#                 exchange_rate_open = EXCLUDED.exchange_rate_open,
#                 exchange_rate_close = EXCLUDED.exchange_rate_close,
#                 price_change_sum = EXCLUDED.price_change_sum;
#         """

#         with pg_hook.get_conn() as conn:
#             with conn.cursor() as cursor:
#                 cursor.execute(aggregate_query)
#                 conn.commit()

#         logger.info("Successfully aggregated gold prices")

#     except Exception as e:
#         logger.error(f"Error in aggregate_gold_prices: {str(e)}")
#         raise

# with DAG(
#     'ETL_goldgta_dtl_di',
#     default_args=default_args,
#     description='Scrape gold prices and save to PostgreSQL',
#     schedule_interval='0 18 * * *',
#     start_date=datetime(2023, 1, 1),
#     catchup=False,
# ) as dag:

#     fetch_task = PythonOperator(task_id='fetch_gold_prices', python_callable=fetch_gold_prices)
#     save_task = PythonOperator(task_id='save_to_postgres', python_callable=save_to_postgres)
#     aggregate_task = PythonOperator(task_id='aggregate_gold_prices', python_callable=aggregate_gold_prices)

#     fetch_task >> save_task >> aggregate_task
    
    
    
    
    
    
    
    
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

def convert_thai_date(thai_date_str):
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
    for attempt in range(max_attempts):
        try:
            url = "https://www.goldtraders.or.th/UpdatePriceList.aspx"
            
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
                'Referer': 'https://www.goldtraders.or.th/',
            }

            session = requests.Session()
            retry_strategy = Retry(
                total=3,
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

            ssl_context = ssl.create_default_context(cafile=certifi.where())

            response = session.get(
                url,
                headers=headers,
                timeout=(10, 30),
                verify=True
            )
            
            response.raise_for_status()

            soup = BeautifulSoup(response.content, 'html.parser')
            table = soup.find('table', {'id': 'DetailPlace_MainGridView'})

            if not table:
                logger.warning("ไม่พบตารางข้อมูลราคาทอง")
                continue

            rows = table.find_all('tr')[1:]
            data = []
            fetch_time = datetime.now()

            for row in rows:
                cols = [col.text.strip() for col in row.find_all('td')]
                if len(cols) < 9:
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
                    logger.error(f"แถวข้อมูลไม่ถูกต้อง: {cols}. Error: {str(e)}")

            if not data:
                logger.warning(f"ไม่พบข้อมูล ครั้งที่ {attempt + 1}")
                continue

            logger.info(f"ดึงข้อมูลสำเร็จ {len(data)} แถว")
            return data

        except requests.exceptions.RequestException as e:
            logger.error(f"เกิดข้อผิดพลาดในการเชื่อมต่อ ครั้งที่ {attempt + 1}: {str(e)}")
            if attempt == max_attempts - 1:
                logger.error("การดึงข้อมูลล้มเหลว หลังจากพยายาม 3 ครั้ง")
                raise

        except Exception as e:
            logger.error(f"ข้อผิดพลาดที่ไม่คาดคิด: {str(e)}")
            logger.error(traceback.format_exc())
            if attempt == max_attempts - 1:
                raise

    raise ValueError("ไม่สามารถดึงข้อมูลได้หลังจากพยายาม 3 ครั้ง")

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