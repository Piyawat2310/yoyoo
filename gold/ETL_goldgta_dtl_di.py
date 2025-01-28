from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from bs4 import BeautifulSoup
import requests
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def fetch_gold_prices(**context):
    # URL และดึงข้อมูลจากเว็บ
    url = "https://www.goldtraders.or.th/UpdatePriceList.aspx"
    response = requests.get(url)
    response.raise_for_status()
    soup = BeautifulSoup(response.content, 'html.parser')

    # หา table ตาม ID
    table = soup.find('table', {'id': 'DetailPlace_MainGridView'})
    rows = table.find_all('tr')[1:]  # ข้าม header (row แรก)

    # เตรียมข้อมูลสำหรับบันทึก
    data = []
    fetch_time = datetime.now()  # เวลาที่ดึงข้อมูล
    for row in rows:
        cols = [col.text.strip() for col in row.find_all('td')]
        if len(cols) >= 9:  # ตรวจสอบว่ามีข้อมูลครบ
            data.append({
                'fetch_time': fetch_time,
                'time': cols[0],  # วันที่/เวลา
                'order_no': cols[1],  # ครั้งที่
                'buy_price_bar': float(cols[2].replace(',', '')),  # ราคาซื้อ (ทองแท่ง)
                'sell_price_bar': float(cols[3].replace(',', '')),  # ราคาขาย (ทองแท่ง)
                'buy_price_shape': float(cols[4].replace(',', '')),  # ราคาซื้อ (ทองรูปพรรณ)
                'sell_price_shape': float(cols[5].replace(',', '')),  # ราคาขาย (ทองรูปพรรณ)
                'gold_spot': float(cols[6].replace(',', '')),  # Gold Spot
                'exchange_rate': float(cols[7].replace(',', '')),  # อัตราแลกเปลี่ยน
                'price_change': float(cols[8].replace(',', ''))  # เปลี่ยนแปลง
            })
    return data

def save_to_postgres(**context):
    # ดึงข้อมูลจาก Task ก่อนหน้า
    data = context['ti'].xcom_pull(task_ids='fetch_gold_prices')

    # ใช้ PostgresHook สำหรับบันทึกลงใน ODS_goldgta_dtl_di
    pg_hook = PostgresHook(postgres_conn_id='SESAME-DB')
    insert_query = """
        INSERT INTO ODS_goldgta_dtl_di (fetch_time, time, order_no, buy_price_bar, sell_price_bar, 
                                        buy_price_shape, sell_price_shape, gold_spot, exchange_rate, price_change)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (fetch_time, time) DO NOTHING;
    """
    # บันทึกข้อมูลทั้งหมดลงในฐานข้อมูล
    with pg_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            for row in data:
                cursor.execute(insert_query, (
                    row['fetch_time'], row['time'], row['order_no'],
                    row['buy_price_bar'], row['sell_price_bar'],
                    row['buy_price_shape'], row['sell_price_shape'],
                    row['gold_spot'], row['exchange_rate'], row['price_change']
                ))

with DAG(
    'ETL_goldgta_dtl_di', 
    default_args=default_args,
    description='Scrape gold prices and save to PostgreSQL',
    schedule_interval='*/30 * * * *',  # รันทุก 30 นาที
    start_date=datetime(2023, 1, 1),
    catchup=False,
) as dag:

    fetch_task = PythonOperator(
        task_id='fetch_gold_prices',
        python_callable=fetch_gold_prices,
        provide_context=True,
    )

    save_task = PythonOperator(
        task_id='save_to_postgres',
        python_callable=save_to_postgres,
        provide_context=True,
    )

    fetch_task >> save_task
