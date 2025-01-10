from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="basta",  
    start_date=datetime(2025, 1, 1),  
    schedule="0 0 * * *",  
    catchup=True  
) as dag:

    # Task 1
    task1 = BashOperator(
        task_id="task1",
        bash_command="echo 'Task 1 test'"
    )

    # Task 2
    task2 = BashOperator(
        task_id="task2",
        bash_command="echo 'Task 2 test'"
    )

    # Task 3
    @task()
    def task3():
        print("Task 3 test")

    # Task 4
    task4 = BashOperator(
        task_id="task4",
        bash_command="echo 'Task 4 test'"
    )

   
    task1 >> task2 >> task3() >> task4






# import datetime
# from airflow import DAG
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator


# DAG_ID = "basta1"

# with DAG(
#     dag_id=DAG_ID,
#     start_date=datetime.datetime(2025, 1, 1),
#     schedule="@once",
#     catchup=False,
# ) as dag:
#     # สร้างตาราง inventory
#     create_inventory_table = SQLExecuteQueryOperator(
#         task_id="create_inventory_table",
#         conn_id="INVENTORY",
#         sql="""
#             CREATE TABLE IF NOT EXISTS inventory (
#                 item_id SERIAL PRIMARY KEY,
#                 item_name VARCHAR(255) NOT NULL,
#                 category VARCHAR(100) NOT NULL,
#                 stock_quantity INT NOT NULL,
#                 price_per_unit DECIMAL(10, 2) NOT NULL
#             );
#         """,
#     )

#     # เพิ่มข้อมูลตัวอย่าง
#     populate_inventory_table = SQLExecuteQueryOperator(
#         task_id="populate_inventory_table",
#         conn_id="INVENTORY",
#         sql="""
#             INSERT INTO inventory (item_name, category, stock_quantity, price_per_unit)
#             VALUES 
#                 ('Laptop', 'Electronics', 15, 1000.00),
#                 ('Mouse', 'Electronics', 50, 20.00),
#                 ('Chair', 'Furniture', 8, 45.00),
#                 ('Desk', 'Furniture', 5, 150.00);
#         """,
#     )

#     # ดึงข้อมูลสินค้าทั้งหมด
#     get_all_items = SQLExecuteQueryOperator(
#         task_id="get_all_items",
#         conn_id="INVENTORY",
#         sql="SELECT * FROM inventory;",
#     )

#     # ดึงสินค้าที่มี stock ต่ำกว่า 10
#     get_low_stock_items = SQLExecuteQueryOperator(
#         task_id="get_low_stock_items",
#         conn_id="INVENTORY",
#         sql="SELECT * FROM inventory WHERE stock_quantity < 10;",
#     ) 
    

#     # ลำดับการทำงานของ DAG
#     create_inventory_table >> populate_inventory_table >> get_all_items >> get_low_stock_items
