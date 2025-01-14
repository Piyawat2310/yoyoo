import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator

DAG_ID = "test01"

        
def write_to_txt_file(**context):
    output_file_path = "/opt/airflow/dags/low_stock_items.txt"
    records = context["ti"].xcom_pull(task_ids="get_low_stock_items")
    
    with open(output_file_path, "w") as file:
        if records:
            file.write("Low Stock Items:\n")
            # ใช้ตัวนับเพื่อสร้าง Item ID ใหม่
            for idx, record in enumerate(records, start=1):
                _, item_name, category, stock_quantity, price_per_unit = record
                file.write(f"Item ID: {idx}, Name: {item_name}, Category: {category}, Stock: {stock_quantity}, Price: {price_per_unit}\n")
        else:
            file.write("No low stock items found.\n")
    print(f"Output written to {output_file_path}")



with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
) as dag:

    create_inventory_table = SQLExecuteQueryOperator(
        task_id="create_inventory_table",
        conn_id="INVENTORY",
        sql="""
            CREATE TABLE IF NOT EXISTS inventory (
                item_id SERIAL PRIMARY KEY,
                item_name VARCHAR(255) NOT NULL,
                category VARCHAR(100) NOT NULL,
                stock_quantity INT NOT NULL,
                price_per_unit DECIMAL(10, 2) NOT NULL
            );
        """,
    )


    populate_inventory_table = SQLExecuteQueryOperator(
        task_id="populate_inventory_table",
        conn_id="INVENTORY",
        sql="""
            
            TRUNCATE TABLE inventory RESTART IDENTITY;
         
            INSERT INTO inventory (item_name, category, stock_quantity, price_per_unit)
            VALUES 
                ('Laptop', 'Electronics', 15, 1000.00),
                ('Mouse', 'Electronics', 50, 20.00),
                ('Keyboard', 'Electronics', 5, 70.00),
                ('Chair', 'Furniture', 8, 45.00),
                ('Desk', 'Furniture', 5, 150.00);
                
        """,
    )


    get_all_items = SQLExecuteQueryOperator(
        task_id="get_all_items",
        conn_id="INVENTORY",
        sql="SELECT * FROM inventory;",
    )


    get_low_stock_items = SQLExecuteQueryOperator(
        task_id="get_low_stock_items",
        conn_id="INVENTORY",
        sql="SELECT * FROM inventory WHERE stock_quantity < 10;"
    )
    
    write_to_file = PythonOperator(
    task_id="write_to_file",
    python_callable=write_to_txt_file,
    provide_context=True,
    )

    create_inventory_table >> populate_inventory_table >> get_all_items >> get_low_stock_items >> write_to_file

