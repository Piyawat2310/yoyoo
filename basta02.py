from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
import random
import time
import logging

# Configure logging
logger = logging.getLogger(__name__)

# SQL สำหรับสร้างตารางในฐานข้อมูล
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS tesla_stock_price (
    id SERIAL PRIMARY KEY,
    price_usd DECIMAL(18, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS tesla_stock_summary (
    date DATE PRIMARY KEY,
    average_price DECIMAL(18, 2),
    max_price DECIMAL(18, 2),
    min_price DECIMAL(18, 2)
);

CREATE TABLE IF NOT EXISTS microsoft_stock_price (
    id SERIAL PRIMARY KEY,
    price_usd DECIMAL(18, 2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS microsoft_stock_summary (
    date DATE PRIMARY KEY,
    average_price DECIMAL(18, 2),
    max_price DECIMAL(18, 2),
    min_price DECIMAL(18, 2)
);
"""

def scrape_tesla_stock_price(ti):
    """ดึงราคาหุ้น Tesla จาก Yahoo Finance"""
    url = 'https://finance.yahoo.com/quote/TSLA/'
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    ]
    headers = {
        "User-Agent": random.choice(USER_AGENTS)
    }
    session = requests.Session()
    try:
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        price_element = soup.select_one('span[data-testid="qsp-price"]')
        if not price_element:
            raise ValueError("Tesla stock price element not found on the page")
        stock_price = float(price_element.text.replace(',', ''))
        ti.xcom_push(key='tesla_stock_price', value=stock_price)
        logger.info(f"Scraped Tesla stock price: ${stock_price:,.2f}")
        time.sleep(2)  # หน่วงเวลาเพื่อเลี่ยงการบล็อก
    except Exception as e:
        logger.error(f"Error scraping Tesla stock price: {e}")
        raise

def scrape_microsoft_stock_price(ti):
    """ดึงราคาหุ้น Microsoft จาก Yahoo Finance"""
    url = 'https://finance.yahoo.com/quote/MSFT/'
    USER_AGENTS = [
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
        "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    ]
    headers = {
        "User-Agent": random.choice(USER_AGENTS)
    }
    session = requests.Session()
    try:
        response = session.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        price_element = soup.select_one('span[data-testid="qsp-price"]')
        if not price_element:
            raise ValueError("Microsoft stock price element not found on the page")
        stock_price = float(price_element.text.replace(',', ''))
        ti.xcom_push(key='microsoft_stock_price', value=stock_price)
        logger.info(f"Scraped Microsoft stock price: ${stock_price:,.2f}")
        time.sleep(2)  # หน่วงเวลาเพื่อเลี่ยงการบล็อก
    except Exception as e:
        logger.error(f"Error scraping Microsoft stock price: {e}")
        raise

def transform_and_insert(ti):
    """แปลงข้อมูลและบันทึกลงฐานข้อมูล"""
    try:
        tesla_stock_price = ti.xcom_pull(key='tesla_stock_price', task_ids='scrape_tesla_stock')
        microsoft_stock_price = ti.xcom_pull(key='microsoft_stock_price', task_ids='scrape_microsoft_stock')

        if tesla_stock_price is None or microsoft_stock_price is None:
            raise ValueError("No stock price found in XCom")

        postgres = PostgresHook(postgres_conn_id='Tesla_stock_price')
        created_at = datetime.now()

        postgres.run("""
            INSERT INTO tesla_stock_price (price_usd, created_at)
            VALUES (%s, %s)
        """, parameters=(tesla_stock_price, created_at))

        postgres.run("""
            INSERT INTO microsoft_stock_price (price_usd, created_at)
            VALUES (%s, %s)
        """, parameters=(microsoft_stock_price, created_at))

        logger.info(f"Inserted Tesla stock price: ${tesla_stock_price:,.2f}")
        logger.info(f"Inserted Microsoft stock price: ${microsoft_stock_price:,.2f}")
    except Exception as e:
        logger.error(f"Error in transform and insert: {e}")
        raise

def aggregate_daily_summary():
    """สรุปข้อมูลรายวัน: ค่าเฉลี่ย, ค่าสูงสุด, ค่าต่ำสุด"""
    try:
        postgres = PostgresHook(postgres_conn_id='Tesla_stock_price')
        query_tesla = """
            SELECT price_usd, created_at::date AS date
            FROM tesla_stock_price;
        """
        df_tesla = postgres.get_pandas_df(query_tesla)

        if not df_tesla.empty:
            summary_tesla = df_tesla.groupby('date').agg({
                'price_usd': ['mean', 'max', 'min']
            }).reset_index()
            summary_tesla.columns = ['date', 'average_price', 'max_price', 'min_price']

            for _, row in summary_tesla.iterrows():
                postgres.run("""
                    INSERT INTO tesla_stock_summary (date, average_price, max_price, min_price)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (date) DO UPDATE 
                    SET average_price = EXCLUDED.average_price,
                        max_price = EXCLUDED.max_price,
                        min_price = EXCLUDED.min_price;
                """, parameters=(row['date'], row['average_price'], row['max_price'], row['min_price']))

        query_microsoft = """
            SELECT price_usd, created_at::date AS date
            FROM microsoft_stock_price;
        """
        df_microsoft = postgres.get_pandas_df(query_microsoft)

        if not df_microsoft.empty:
            summary_microsoft = df_microsoft.groupby('date').agg({
                'price_usd': ['mean', 'max', 'min']
            }).reset_index()
            summary_microsoft.columns = ['date', 'average_price', 'max_price', 'min_price']

            for _, row in summary_microsoft.iterrows():
                postgres.run("""
                    INSERT INTO microsoft_stock_summary (date, average_price, max_price, min_price)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT (date) DO UPDATE 
                    SET average_price = EXCLUDED.average_price,
                        max_price = EXCLUDED.max_price,
                        min_price = EXCLUDED.min_price;
                """, parameters=(row['date'], row['average_price'], row['max_price'], row['min_price']))

        logger.info("Daily summary aggregation completed and saved.")
    except Exception as e:
        logger.error(f"Error in daily aggregation: {e}")
        raise
    
def analyze_daily_percentage_change():
    """
    วิเคราะห์เปอร์เซ็นต์การเปลี่ยนแปลงของหุ้น Tesla และ Microsoft
    สำหรับแต่ละวัน และส่งออกข้อมูลเป็นไฟล์ CSV
    """
    export_dir = Path('/opt/airflow/dags/stock_reports')
    export_dir.mkdir(exist_ok=True)
    today = datetime.now().strftime('%Y-%m-%d')

    try:
        postgres = PostgresHook(postgres_conn_id='Tesla_stock_price')

        # Query Tesla stock prices
        query_tesla = """
            SELECT created_at::date AS date, price_usd
            FROM tesla_stock_price
            ORDER BY created_at;
        """
        df_tesla = postgres.get_pandas_df(query_tesla)

        if not df_tesla.empty:
            # คำนวณราคาหุ้นเฉลี่ยในแต่ละวัน
            df_tesla['date'] = pd.to_datetime(df_tesla['date'])
            df_tesla_avg = df_tesla.groupby('date')['price_usd'].mean().reset_index()

            # คำนวณเปอร์เซ็นต์การเปลี่ยนแปลง
            df_tesla_avg['percentage_change'] = df_tesla_avg['price_usd'].pct_change() * 100

            # เก็บค่า price_usd ในแต่ละวัน
            df_tesla_avg['price_usd'] = df_tesla_avg['price_usd'].round(2)

            # เติมค่า percentage_change ที่เป็น NaN สำหรับวันแรก
            df_tesla_avg['percentage_change'] = df_tesla_avg['percentage_change'].fillna(0)

            # Save Tesla data to CSV
            csv_path_tesla = export_dir / f'tesla_daily_percentage_change_{today}.csv'
            df_tesla_avg.to_csv(csv_path_tesla, index=False)
            logger.info(f"Tesla percentage change export completed: CSV at {csv_path_tesla}")

        # Query Microsoft stock prices
        query_microsoft = """
            SELECT created_at::date AS date, price_usd
            FROM microsoft_stock_price
            ORDER BY created_at;
        """
        df_microsoft = postgres.get_pandas_df(query_microsoft)

        if not df_microsoft.empty:
            # คำนวณราคาหุ้นเฉลี่ยในแต่ละวัน
            df_microsoft['date'] = pd.to_datetime(df_microsoft['date'])
            df_microsoft_avg = df_microsoft.groupby('date')['price_usd'].mean().reset_index()

            # คำนวณเปอร์เซ็นต์การเปลี่ยนแปลง
            df_microsoft_avg['percentage_change'] = df_microsoft_avg['price_usd'].pct_change() * 100

            # เก็บค่า price_usd ในแต่ละวัน
            df_microsoft_avg['price_usd'] = df_microsoft_avg['price_usd'].round(2)

            # เติมค่า percentage_change ที่เป็น NaN สำหรับวันแรก
            df_microsoft_avg['percentage_change'] = df_microsoft_avg['percentage_change'].fillna(0)

            # Save Microsoft data to CSV
            csv_path_microsoft = export_dir / f'microsoft_daily_percentage_change_{today}.csv'
            df_microsoft_avg.to_csv(csv_path_microsoft, index=False)
            logger.info(f"Microsoft percentage change export completed: CSV at {csv_path_microsoft}")

    except Exception as e:
        logger.error(f"Error in analyzing daily percentage change: {e}")
        raise



def export_data():
    """ส่งออกราคาหุ้น Tesla และ Microsoft เป็นไฟล์ CSV"""
    export_dir = Path('/opt/airflow/dags/stock_reports')
    export_dir.mkdir(exist_ok=True)
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        postgres = PostgresHook(postgres_conn_id='Tesla_stock_price')
        query_tesla = """
            SELECT price_usd, created_at
            FROM tesla_stock_price
            WHERE DATE(created_at) = CURRENT_DATE
            ORDER BY created_at DESC;
        """
        df_tesla = postgres.get_pandas_df(query_tesla)

        query_microsoft = """
            SELECT price_usd, created_at
            FROM microsoft_stock_price
            WHERE DATE(created_at) = CURRENT_DATE
            ORDER BY created_at DESC;
        """
        df_microsoft = postgres.get_pandas_df(query_microsoft)

        if not df_tesla.empty:
            csv_path_tesla = export_dir / f'tesla_stock_prices_{today}.csv'
            df_tesla.to_csv(csv_path_tesla, index=False)
            logger.info(f"Tesla export completed: CSV at {csv_path_tesla}")

        if not df_microsoft.empty:
            csv_path_microsoft = export_dir / f'microsoft_stock_prices_{today}.csv'
            df_microsoft.to_csv(csv_path_microsoft, index=False)
            logger.info(f"Microsoft export completed: CSV at {csv_path_microsoft}")
    except Exception as e:
        logger.error(f"Error in data export: {e}")
        raise

# การกำหนด DAG
default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}
dag = DAG(
    'stock_price_pipeline',
    default_args=default_args,
    description='ETL pipeline for stock price data',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['stocks', 'etl']
)

# Tasks
create_table = PostgresOperator(
    task_id='create_table',
    postgres_conn_id='Tesla_stock_price',
    sql=CREATE_TABLE_SQL,
    dag=dag
)
scrape_tesla_stock = PythonOperator(
    task_id='scrape_tesla_stock',
    python_callable=scrape_tesla_stock_price,
    dag=dag
)
scrape_microsoft_stock = PythonOperator(
    task_id='scrape_microsoft_stock',
    python_callable=scrape_microsoft_stock_price,
    dag=dag
)
transform_insert = PythonOperator(
    task_id='transform_insert',
    python_callable=transform_and_insert,
    dag=dag
)
aggregate_summary = PythonOperator(
    task_id='aggregate_daily_summary',
    python_callable=aggregate_daily_summary,
    dag=dag
)
analyze_percentage_change = PythonOperator(
    task_id='analyze_percentage_change',
    python_callable=analyze_daily_percentage_change,
    dag=dag
)
export_reports = PythonOperator(
    task_id='export_reports',
    python_callable=export_data,
    dag=dag
)

# Workflow
create_table >> [scrape_tesla_stock, scrape_microsoft_stock] >> transform_insert >> aggregate_summary >> [export_reports, analyze_percentage_change]