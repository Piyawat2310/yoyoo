from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import requests
from bs4 import BeautifulSoup
import logging
from psycopg2.extras import execute_values

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'ETL_goldgta_monthly_historical',
    default_args=default_args,
    description='Extract gold price history for the last 30 days and load into PostgreSQL',
    schedule_interval=None,
    catchup=False
)

BASE_URL = "https://goldtraders.or.th/DailyPrices.aspx"


def extract_historical_gold_data(**kwargs):
    """ดึงข้อมูลราคาทองย้อนหลัง 30 วัน"""
    logging.info("เริ่มดึงข้อมูล...")
    data = []
    session = requests.Session()
    fetch_time = datetime.now()

    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'th,en-US;q=0.7,en;q=0.3',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    for day in range(30):
        try:
            date = datetime.now() - timedelta(days=day)
            formatted_date = date.strftime('%d/%m/%Y')
            logging.info(f"กำลังดึงข้อมูลวันที่ {formatted_date}")

            payload = {
                'txtDate': formatted_date,
                'btnSearch': 'ค้นหา'
            }
            
            response = session.post(BASE_URL, data=payload, headers=headers)
            response.encoding = 'utf-8'
            
            logging.info(f"Status Code: {response.status_code}")
            logging.info(f"Content Length: {len(response.content)}")
            
            if response.status_code != 200:
                logging.error(f"Failed to fetch data for {formatted_date}: Status code {response.status_code}")
                continue
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Add debug logging for HTML content
            logging.debug(f"HTML Content: {soup.prettify()}")
            
            # Try different table identifiers
            table = None
            possible_tables = [
                soup.find('table', id='DetailPlace_MainGridView'),
                soup.find('table', {'class': 'table'}),
                soup.find('table', id='GridView1')
            ]
            
            for possible_table in possible_tables:
                if possible_table:
                    table = possible_table
                    break
            
            if not table:
                logging.warning(f"ไม่พบตารางข้อมูลสำหรับวันที่ {formatted_date}")
                # Add debug logging for all tables found
                all_tables = soup.find_all('table')
                logging.debug(f"Found {len(all_tables)} tables on the page")
                for idx, t in enumerate(all_tables):
                    logging.debug(f"Table {idx} attributes: {t.attrs}")
                continue

            rows = table.find_all('tr')[1:]  # Skip header row
            
            # Debug logging for row data
            for row_idx, row in enumerate(rows):
                cols = row.find_all('td')
                col_data = [col.text.strip() for col in cols]
                logging.debug(f"Row {row_idx + 1} data: {col_data}")
                
                if len(cols) < 8:
                    logging.warning(f"Row {row_idx + 1} has insufficient columns: {len(cols)}")
                    continue
                    
                try:
                    # Convert time string to proper format
                    time_str = cols[0].text.strip()
                    
                    # Handle potential different time formats
                    try:
                        record_time = datetime.strptime(f"{formatted_date} {time_str}", '%d/%m/%Y %H:%M')
                    except ValueError:
                        logging.warning(f"Invalid time format: {time_str}")
                        continue
                    
                    # Clean and convert numeric values
                    def clean_number(text):
                        return float(text.strip().replace(',', '')) if text.strip() else 0.0
                    
                    record = (
                        fetch_time,
                        record_time,
                        int(cols[1].text.strip().replace(',', '')) if cols[1].text.strip() else 0,
                        *[clean_number(cols[i].text) for i in range(2, 8)],
                        clean_number(cols[8].text) if len(cols) > 8 else 0.0
                    )
                    
                    # Validate record values
                    if all(isinstance(x, (int, float, datetime)) for x in record):
                        data.append(record)
                        logging.info(f"บันทึกข้อมูลสำเร็จสำหรับวันที่ {formatted_date} เวลา {time_str}")
                    else:
                        logging.warning(f"Invalid data types in record: {record}")
                        
                except Exception as e:
                    logging.error(f"เกิดข้อผิดพลาดในการแปลงข้อมูล: {e}")
                    logging.error(f"ข้อมูลที่ผิดพลาด: {[col.text.strip() for col in cols]}")

        except Exception as e:
            logging.error(f"เกิดข้อผิดพลาดสำหรับวันที่ {formatted_date}: {e}")
            continue

    if not data:
        logging.error("ไม่พบข้อมูลที่ถูกต้องเลย")
        # Instead of raising an error, return empty list to allow pipeline to continue
        return []
    
    logging.info(f"จำนวนข้อมูลทั้งหมดที่ดึงได้: {len(data)}")
    kwargs['ti'].xcom_push(key='gold_data', value=data)
    return data

def load_to_ods(**kwargs):
    """Load extracted data into ODS table."""
    data = kwargs['ti'].xcom_pull(task_ids='extract_historical_data', key='gold_data')
    if not data:
        logging.warning("No data found in XCom for loading! Skipping load task.")
        return

    pg_hook = PostgresHook(postgres_conn_id='SESAME-DB')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM ODS_goldgta_dtl_di WHERE time >= current_date - interval '30 days'")
        insert_query = """
            INSERT INTO ODS_goldgta_dtl_di 
            (fetch_time, time, order_no, buy_price_bar, sell_price_bar, 
             buy_price_shape, sell_price_shape, gold_spot, exchange_rate, price_change)
            VALUES %s
        """
        execute_values(cursor, insert_query, data)
        conn.commit()
        logging.info(f"{len(data)} records loaded into ODS successfully.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error inserting data into ODS: {e}")
    finally:
        cursor.close()
        conn.close()

def transform_to_dwd():
    """Transform data from ODS to DWD."""
    pg_hook = PostgresHook(postgres_conn_id='SESAME-DB')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("DELETE FROM dwd_goldgta_dtl_di WHERE time >= current_date - interval '30 days'")
        
        transform_query = """
            WITH daily_data AS (
                SELECT 
                    fetch_time,
                    DATE(time) AS date_time,
                    order_no,
                    buy_price_bar,
                    sell_price_bar,
                    buy_price_shape,
                    sell_price_shape,
                    gold_spot,
                    exchange_rate,
                    price_change,
                    ROW_NUMBER() OVER (PARTITION BY DATE(time) ORDER BY time ASC) as first_row,
                    ROW_NUMBER() OVER (PARTITION BY DATE(time) ORDER BY time DESC) as last_row
                FROM ODS_goldgta_dtl_di
                WHERE time >= current_date - interval '30 days'
            )
            INSERT INTO dwd_goldgta_dtl_di (
                fetch_time, time, order_sum, 
                buy_price_bar_open, buy_price_bar_close, 
                sell_price_bar_open, sell_price_bar_close, 
                buy_price_shape_open, buy_price_shape_close, 
                sell_price_shape_open, sell_price_shape_close, 
                gold_spot_open, gold_spot_close, 
                exchange_rate_open, exchange_rate_close, 
                price_change_sum
            )
            SELECT 
                MAX(fetch_time) AS fetch_time,
                date_time AS time,
                COUNT(order_no) AS order_sum,
                MAX(CASE WHEN first_row = 1 THEN buy_price_bar END) AS buy_price_bar_open,
                MAX(CASE WHEN last_row = 1 THEN buy_price_bar END) AS buy_price_bar_close,
                MAX(CASE WHEN first_row = 1 THEN sell_price_bar END) AS sell_price_bar_open,
                MAX(CASE WHEN last_row = 1 THEN sell_price_bar END) AS sell_price_bar_close,
                MAX(CASE WHEN first_row = 1 THEN buy_price_shape END) AS buy_price_shape_open,
                MAX(CASE WHEN last_row = 1 THEN buy_price_shape END) AS buy_price_shape_close,
                MAX(CASE WHEN first_row = 1 THEN sell_price_shape END) AS sell_price_shape_open,
                MAX(CASE WHEN last_row = 1 THEN sell_price_shape END) AS sell_price_shape_close,
                MAX(CASE WHEN first_row = 1 THEN gold_spot END) AS gold_spot_open,
                MAX(CASE WHEN last_row = 1 THEN gold_spot END) AS gold_spot_close,
                MAX(CASE WHEN first_row = 1 THEN exchange_rate END) AS exchange_rate_open,
                MAX(CASE WHEN last_row = 1 THEN exchange_rate END) AS exchange_rate_close,
                SUM(price_change) AS price_change_sum
            FROM daily_data
            GROUP BY date_time;
        """
        cursor.execute(transform_query)
        conn.commit()
        logging.info("Data transformed into DWD successfully.")
    except Exception as e:
        conn.rollback()
        logging.error(f"Error transforming data: {e}")
    finally:
        cursor.close()
        conn.close()

# Define Tasks
extract_historical_task = PythonOperator(
    task_id='extract_historical_data',
    python_callable=extract_historical_gold_data,  # ตรงกับชื่อฟังก์ชัน
    dag=dag
)

load_ods_task = PythonOperator(
    task_id='load_to_ods',
    python_callable=load_to_ods,
    dag=dag
)

transform_dwd_task = PythonOperator(
    task_id='transform_to_dwd',
    python_callable=transform_to_dwd,
    dag=dag
)

# Task Dependencies
extract_historical_task >> load_ods_task >> transform_dwd_task