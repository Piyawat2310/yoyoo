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

        # ตรวจสอบและปรับปรุง selector ที่ถูกต้อง
        price_element = soup.select_one('span.IsqQVc.NprOob.wT3VGc')
        if not price_element:
            raise ValueError("Tesla stock price element not found on the page")

        # ดึงราคาและแปลงเป็น float
        stock_price = float(price_element.text.replace(',', ''))
        ti.xcom_push(key='tesla_stock_price', value=stock_price)
        logger.info(f"Scraped Tesla stock price: ${stock_price:,.2f}")
    except Exception as e:
        logger.error(f"Error scraping Tesla stock price: {e}")
        raise


def transform_and_insert(ti):
    """แปลงข้อมูลและบันทึกลงฐานข้อมูล"""
    try:
        tesla_stock_price = ti.xcom_pull(key='tesla_stock_price', task_ids='scrape_tesla_stock')
        if tesla_stock_price is None:
            raise ValueError("No Tesla stock price found in XCom")
        
        postgres = PostgresHook(postgres_conn_id='Tesla_stock_price')
        # ใช้เวลาในขณะนี้สำหรับการบันทึกลงฐานข้อมูล
        created_at = datetime.now()

        postgres.run("""
            INSERT INTO tesla_stock_price (price_usd, created_at)
            VALUES (%s, %s)
        """, parameters=(tesla_stock_price, created_at))
        logger.info(f"Inserted Tesla stock price: ${tesla_stock_price:,.2f}")
    except Exception as e:
        logger.error(f"Error in transform and insert: {e}")
        raise

def aggregate_daily_summary():
    """สรุปข้อมูลรายวัน: ค่าเฉลี่ย, ค่าสูงสุด, ค่าต่ำสุด"""
    try:
        postgres = PostgresHook(postgres_conn_id='Tesla_stock_price')
        query = """
            SELECT price_usd, created_at::date AS date
            FROM tesla_stock_price;
        """
        df = postgres.get_pandas_df(query)

        if df.empty:
            logger.warning("No data found for aggregation.")
            return

        # สรุปข้อมูลรายวัน
        summary = df.groupby('date').agg({
            'price_usd': ['mean', 'max', 'min']
        }).reset_index()
        summary.columns = ['date', 'average_price', 'max_price', 'min_price']

        # บันทึกลงฐานข้อมูล
        for _, row in summary.iterrows():
            postgres.run("""
                INSERT INTO tesla_stock_summary (date, average_price, max_price, min_price)
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

def export_data():
    """ส่งออกราคาหุ้น Tesla เป็นไฟล์ CSV"""
    export_dir = Path('/opt/airflow/dags/tesla_reports')
    export_dir.mkdir(exist_ok=True)
    logger.info(f"Export directory: {export_dir}")
    today = datetime.now().strftime('%Y-%m-%d')
    try:
        postgres = PostgresHook(postgres_conn_id='Tesla_stock_price')
        query = """
            SELECT price_usd, created_at
            FROM tesla_stock_price
            WHERE DATE(created_at) = CURRENT_DATE
            ORDER BY created_at DESC;
        """
        df = postgres.get_pandas_df(query)

        if df.empty:
            logger.warning("No data found for today's Tesla stock price. Export skipped.")
            return

        # Export to CSV
        csv_path = export_dir / f'tesla_stock_prices_{today}.csv'
        df.to_csv(csv_path, index=False)
        logger.info(f"Export completed: CSV at {csv_path}")
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
    'tesla_stock_pipeline',
    default_args=default_args,
    description='ETL pipeline for Tesla stock price data',
    schedule_interval=timedelta(hours=1),
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['tesla', 'etl']
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
create_table >> scrape_tesla_stock >> transform_insert >> aggregate_summary >> export_reports
