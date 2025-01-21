import datetime
from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.python import PythonOperator

DAG_ID = "order_management_postgresql"

def write_to_txt_file(**context):
    output_file_path = "/opt/airflow/dags/orders_to_ship.txt"
    records = context["ti"].xcom_pull(task_ids="get_orders_to_ship")
    
    with open(output_file_path, "w") as file:
        if records:
            file.write("Orders to Ship Tomorrow:\n")
            for record in records:
                order_id, customer_name, product_name, quantity, order_date, ship_date = record
                file.write(
                    f"Order ID: {order_id}, Customer: {customer_name}, Product: {product_name}, Quantity: {quantity}, "
                    f"Order Date: {order_date}, Ship Date: {ship_date}\n"
                )
        else:
            file.write("No orders to ship tomorrow.\n")
    print(f"Output written to {output_file_path}")

with DAG(
    dag_id=DAG_ID,
    start_date=datetime.datetime(2025, 1, 1),
    schedule="@once",
    catchup=False,
) as dag:

    create_orders_table = SQLExecuteQueryOperator(
        task_id="create_orders_table",
        conn_id="Postgresql_Test",
        sql="""
            CREATE TABLE IF NOT EXISTS orders (
                order_id SERIAL PRIMARY KEY,
                customer_name VARCHAR(255) NOT NULL,
                product_name VARCHAR(255) NOT NULL,
                quantity INT NOT NULL,
                order_date DATE NOT NULL,
                ship_date DATE NOT NULL
            );
        """,
    )

    populate_orders_table = SQLExecuteQueryOperator(
        task_id="populate_orders_table",
        conn_id="Postgresql_Test",
        sql="""
            TRUNCATE TABLE orders RESTART IDENTITY;

            INSERT INTO orders (customer_name, product_name, quantity, order_date, ship_date)
            VALUES 
                ('Anny', 'Laptop', 1, '2025-01-19', '2025-01-20'),
                ('Bob', 'Mouse', 2, '2025-01-18', '2025-01-20'),
                ('Charlie', 'Keyboard', 1, '2025-01-17', '2025-01-19'),
                ('Diana', 'Desk', 1, '2025-01-19', '2025-01-20'),
                ('Eve', 'Chair', 3, '2025-01-19', '2025-01-21');
        """,
    )

    get_all_orders = SQLExecuteQueryOperator(
        task_id="get_all_orders",
        conn_id="Postgresql_Test",
        sql="SELECT * FROM orders;",
    )

    get_orders_to_ship = SQLExecuteQueryOperator(
        task_id="get_orders_to_ship",
        conn_id="Postgresql_Test",
        sql="""
            SELECT * 
            FROM orders 
            WHERE ship_date = CURRENT_DATE + INTERVAL '1 day';
        """,
    )
    
    write_to_file = PythonOperator(
        task_id="write_to_file",
        python_callable=write_to_txt_file,
        provide_context=True,
    )

    create_orders_table >> populate_orders_table >> get_all_orders >> get_orders_to_ship >> write_to_file
