from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import requests
from bs4 import BeautifulSoup
import pandas as pd
from pathlib import Path
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
    """ดึงราคาหุ้น Tesla จาก Google Search"""
    url = 'https://www.google.com/search?q=tesla+stock+price+today'
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        price_element = soup.select_one('span.IsqQVc.NprOob.wT3VGc')
        if not price_element:
            raise ValueError("Tesla stock price element not found on the page")

        stock_price = float(price_element.text.replace(',', ''))
        ti.xcom_push(key='tesla_stock_price', value=stock_price)
        logger.info(f"Scraped Tesla stock price: ${stock_price:,.2f}")
    except Exception as e:
        logger.error(f"Error scraping Tesla stock price: {e}")
        raise

def scrape_microsoft_stock_price(ti):
    """ดึงราคาหุ้น Microsoft จาก Google Search"""
    url = 'https://www.google.com/search?q=microsoft+stock+price+today+'
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    try:
        response = requests.get(url, headers=headers, timeout=10)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        price_element = soup.select_one('span.IsqQVc.NprOob.wT3VGc')
        if not price_element:
            raise ValueError("Microsoft stock price element not found on the page")

        stock_price = float(price_element.text.replace(',', ''))
        ti.xcom_push(key='microsoft_stock_price', value=stock_price)
        logger.info(f"Scraped Microsoft stock price: ${stock_price:,.2f}")
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

        # ใช้เวลาในขณะนี้สำหรับการบันทึกลงฐานข้อมูล
        created_at = datetime.now()

        # บันทึกข้อมูลของ Tesla
        postgres.run("""
            INSERT INTO tesla_stock_price (price_usd, created_at)
            VALUES (%s, %s)
        """, parameters=(tesla_stock_price, created_at))

        # บันทึกข้อมูลของ Microsoft
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

        if df_tesla.empty:
            logger.warning("No data found for Tesla aggregation.")
            return

        # สรุปข้อมูลรายวันสำหรับ Tesla
        summary_tesla = df_tesla.groupby('date').agg({
            'price_usd': ['mean', 'max', 'min']
        }).reset_index()
        summary_tesla.columns = ['date', 'average_price', 'max_price', 'min_price']

        # บันทึกลงฐานข้อมูล
        for _, row in summary_tesla.iterrows():
            postgres.run("""
                INSERT INTO tesla_stock_summary (date, average_price, max_price, min_price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (date) DO UPDATE 
                SET average_price = EXCLUDED.average_price,
                    max_price = EXCLUDED.max_price,
                    min_price = EXCLUDED.min_price;
            """, parameters=(row['date'], row['average_price'], row['max_price'], row['min_price']))

        logger.info("Tesla daily summary aggregation completed and saved.")

        query_microsoft = """
            SELECT price_usd, created_at::date AS date
            FROM microsoft_stock_price;
        """
        df_microsoft = postgres.get_pandas_df(query_microsoft)

        if df_microsoft.empty:
            logger.warning("No data found for Microsoft aggregation.")
            return

        # สรุปข้อมูลรายวันสำหรับ Microsoft
        summary_microsoft = df_microsoft.groupby('date').agg({
            'price_usd': ['mean', 'max', 'min']
        }).reset_index()
        summary_microsoft.columns = ['date', 'average_price', 'max_price', 'min_price']

        # บันทึกลงฐานข้อมูล
        for _, row in summary_microsoft.iterrows():
            postgres.run("""
                INSERT INTO microsoft_stock_summary (date, average_price, max_price, min_price)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (date) DO UPDATE 
                SET average_price = EXCLUDED.average_price,
                    max_price = EXCLUDED.max_price,
                    min_price = EXCLUDED.min_price;
            """, parameters=(row['date'], row['average_price'], row['max_price'], row['min_price']))

        logger.info("Microsoft daily summary aggregation completed and saved.")
    except Exception as e:
        logger.error(f"Error in daily aggregation: {e}")
        raise

def export_data():
    """ส่งออกราคาหุ้น Tesla และ Microsoft เป็นไฟล์ CSV"""
    export_dir = Path('/opt/airflow/dags/stock_reports')
    export_dir.mkdir(exist_ok=True)
    logger.info(f"Export directory: {export_dir}")
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

        if df_tesla.empty and df_microsoft.empty:
            logger.warning("No data found for today's Tesla and Microsoft stock prices. Export skipped.")
            return

        # Export to CSV
        csv_path_tesla = export_dir / f'tesla_stock_prices_{today}.csv'
        csv_path_microsoft = export_dir / f'microsoft_stock_prices_{today}.csv'

        if not df_tesla.empty:
            df_tesla.to_csv(csv_path_tesla, index=False)
            logger.info(f"Tesla export completed: CSV at {csv_path_tesla}")

        if not df_microsoft.empty:
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
export_reports = PythonOperator(
    task_id='export_reports',
    python_callable=export_data,
    dag=dag
)

# Workflow
create_table >> [scrape_tesla_stock, scrape_microsoft_stock] >> transform_insert >> aggregate_summary >> export_reports
