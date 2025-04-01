import logging
from airflow import DAG
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta

def extract_data_from_mssql(**kwargs):
    try:
        mssql_hook = MsSqlHook(mssql_conn_id="company_connection")
        sql_query = """
        SELECT 
    ID , 
    PURCHASE_NO, 
    MEMBERID, 
    PACKAGECODE, 
    PRICE, 
    [RIGHT] AS purchase_right,  
    OM_CHARGEID, 
    CHARGE_DATETIME, 
    RES_CHARGE_DATETIME, 
    RES_CHARGE_STATUS, 
    RES_CHARGE_FAILURECODE, 
    AUTHEN_DATETIME, 
    RES_AUTHEN_DATETIME, 
    RES_DETAIL_DATETIME, 
    RES_DETAIL_STATUS, 
    RES_DETAIL_FAILURECODE, 
    RES_DETAIL_PAID, 
    RES_DETAIL_REALCHARGE_DATETIME,  
    SEND_MAIL_BILLPAYMENT, 
    RECEIPT_ADDRESS, 
    RECEIPT_TAXID, 
    RECEIPT_TITLE, 
    RECEIPT_FIRSTNAMETH, 
    RECEIPT_LASTNAMETH, 
    RECEIPT_CORPORATENAMETH, 
    RECEIPT_EMAIL, 
    IS_WANTRECEIPT, 
    OM_CARDID, 
    OM_SCHEDULEID, 
    PROMOTION_CODE, 
    COUPONCODE, 
    PURCHASE_SOURCE, 
    APPLEINAPP_ID, 
    APPLE_FAILURECODE,
    RECEIPTID, 
    PAYMENT_METHOD, 
    FEE_RATE, 
    FEE_FLAT, 
    VAT_RATE, 
    NET_PRICE
FROM BWM_PLTDB.dbo.RT_PURCHASE_HISTORY
WHERE CHARGE_DATETIME >= DATEADD(DAY, -7, GETDATE());
        """
        records = mssql_hook.get_records(sql_query)
        
        if not records:
            logging.info("No records found in source table")
        
        kwargs['ti'].xcom_push(key='extracted_data', value=records)
        return len(records)
    except Exception as e:
        logging.error(f"Error in extraction: {str(e)}")
        raise

def load_data_into_ods(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="SESAME-DB")
    records = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extract_data')
    
    if not records:
        logging.info("No records to load into ODS")
        return 0
    
    try:
        conn = pg_hook.get_conn()
        cursor = conn.cursor()
        
        # Truncate long strings to fit column constraints
        truncated_records = []
        for record in records:
            truncated_record = list(record)
            # Define max lengths for string columns (adjust as needed)
            truncation_map = {
                9: 100,   # res_charge_status
                10: 200,  # res_charge_failurecode
                15: 100,  # res_detail_status
                16: 200,  # res_detail_failurecode
                17: 200,  # res_detail_paid
                18: 50,   # send_mail_billpayment
                20: 50,   # receipt_taxid
                21: 100,  # receipt_title
                22: 200,  # receipt_firstnameth
                23: 200,  # receipt_lastnameth
                24: 300,  # receipt_corporatenameth
                25: 300,  # receipt_email
                26: 50,   # is_wantreceipt
                27: 200,  # om_cardid
                28: 200,  # om_scheduleid
                29: 100,  # promotion_code
                30: 100,  # couponcode
                31: 100,  # purchase_source
                32: 200,  # appleinapp_id
                33: 200,  # apple_failurecode
                34: 200,  # receiptid
                35: 100   # payment_method
            }
            
            for col, max_length in truncation_map.items():
                if isinstance(truncated_record[col], str):
                    truncated_record[col] = truncated_record[col][:max_length]
            
            truncated_records.append(truncated_record)
        
        insert_query = """
        INSERT INTO ods_purchase_transactions_dtl_di (
            id, 
            purchase_no, 
            memberid, 
            packagecode, 
            price, 
            purchase_right, 
            om_chargeid, 
            charge_datetime, 
            res_charge_datetime, 
            res_charge_status, 
            res_charge_failurecode, 
            authen_datetime, 
            res_authen_datetime, 
            res_detail_datetime, 
            res_detail_status, 
            res_detail_failurecode, 
            res_detail_paid, 
            res_detail_realcharge_datetime, 
            send_mail_billpayment, 
            receipt_address, 
            receipt_taxid, 
            receipt_title, 
            receipt_firstnameth, 
            receipt_lastnameth, 
            receipt_corporatenameth, 
            receipt_email, 
            is_wantreceipt, 
            om_cardid, 
            om_scheduleid, 
            promotion_code, 
            couponcode, 
            purchase_source, 
            appleinapp_id, 
            apple_failurecode, 
            receiptid, 
            payment_method, 
            fee_rate, 
            fee_flat, 
            vat_rate, 
            net_price,
            inc_date
        ) VALUES (
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, 
            CURRENT_TIMESTAMP
        )
        """
        cursor.executemany(insert_query, truncated_records)
        conn.commit()
        logging.info(f"Loaded {cursor.rowcount} records into ODS")
        return cursor.rowcount
    except Exception as e:
        conn.rollback()
        logging.error(f"Error loading into ODS: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()

def upsert_data_into_dwd(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id="SESAME-DB")
    
    try:
        # ใช้ CTE เพื่อเลือกข้อมูลล่าสุดและไม่ซ้ำกัน
        upsert_query = """
        WITH latest_ods_data AS (
            SELECT DISTINCT ON (purchase_no, memberid, packagecode, om_chargeid) *
            FROM ods_purchase_transactions_dtl_di
            WHERE inc_date = (
                SELECT MAX(inc_date)
                FROM ods_purchase_transactions_dtl_di
            )
            ORDER BY purchase_no, memberid, packagecode, om_chargeid, inc_date DESC
        )
        INSERT INTO dwd_purchase_transactions_dtl_di AS dwd (
            id, 
            purchase_no, 
            memberid, 
            packagecode, 
            price, 
            purchase_right, 
            om_chargeid, 
            charge_datetime, 
            res_charge_datetime, 
            res_charge_status, 
            res_charge_failurecode, 
            authen_datetime, 
            res_authen_datetime, 
            res_detail_datetime, 
            res_detail_status, 
            res_detail_failurecode, 
            res_detail_paid, 
            res_detail_realcharge_datetime, 
            send_mail_billpayment, 
            receipt_address, 
            receipt_taxid, 
            receipt_title, 
            receipt_firstnameth, 
            receipt_lastnameth, 
            receipt_corporatenameth, 
            receipt_email, 
            is_wantreceipt, 
            om_cardid, 
            om_scheduleid, 
            promotion_code, 
            couponcode, 
            purchase_source, 
            appleinapp_id, 
            apple_failurecode, 
            receiptid, 
            payment_method, 
            fee_rate, 
            fee_flat, 
            vat_rate, 
            net_price,
            inc_date
        )
        SELECT 
            id, 
            purchase_no, 
            memberid, 
            packagecode, 
            price, 
            purchase_right, 
            om_chargeid, 
            charge_datetime, 
            res_charge_datetime,  -- Use res_charge_datetime as inc_date
            res_charge_status, 
            res_charge_failurecode, 
            authen_datetime, 
            res_authen_datetime, 
            res_detail_datetime, 
            res_detail_status, 
            res_detail_failurecode, 
            res_detail_paid, 
            res_detail_realcharge_datetime, 
            send_mail_billpayment, 
            receipt_address, 
            receipt_taxid, 
            receipt_title, 
            receipt_firstnameth, 
            receipt_lastnameth, 
            receipt_corporatenameth, 
            receipt_email, 
            is_wantreceipt, 
            om_cardid, 
            om_scheduleid, 
            promotion_code, 
            couponcode, 
            purchase_source, 
            appleinapp_id, 
            apple_failurecode, 
            receiptid, 
            payment_method, 
            fee_rate, 
            fee_flat, 
            vat_rate, 
            net_price,
            res_charge_datetime  -- This will be used as inc_date
        FROM latest_ods_data
        ON CONFLICT (purchase_no, memberid, packagecode, om_chargeid) 
        DO UPDATE SET 
            id = EXCLUDED.id,
            price = EXCLUDED.price,
            purchase_right = EXCLUDED.purchase_right,
            charge_datetime = EXCLUDED.charge_datetime,
            res_charge_datetime = EXCLUDED.res_charge_datetime,
            res_charge_status = EXCLUDED.res_charge_status,
            res_charge_failurecode = EXCLUDED.res_charge_failurecode,
            authen_datetime = EXCLUDED.authen_datetime,
            res_authen_datetime = EXCLUDED.res_authen_datetime,
            res_detail_datetime = EXCLUDED.res_detail_datetime,
            res_detail_status = EXCLUDED.res_detail_status,
            res_detail_failurecode = EXCLUDED.res_detail_failurecode,
            res_detail_paid = EXCLUDED.res_detail_paid,
            res_detail_realcharge_datetime = EXCLUDED.res_detail_realcharge_datetime,
            send_mail_billpayment = EXCLUDED.send_mail_billpayment,
            receipt_address = EXCLUDED.receipt_address,
            receipt_taxid = EXCLUDED.receipt_taxid,
            receipt_title = EXCLUDED.receipt_title,
            receipt_firstnameth = EXCLUDED.receipt_firstnameth,
            receipt_lastnameth = EXCLUDED.receipt_lastnameth,
            receipt_corporatenameth = EXCLUDED.receipt_corporatenameth,
            receipt_email = EXCLUDED.receipt_email,
            is_wantreceipt = EXCLUDED.is_wantreceipt,
            om_cardid = EXCLUDED.om_cardid,
            om_scheduleid = EXCLUDED.om_scheduleid,
            promotion_code = EXCLUDED.promotion_code,
            couponcode = EXCLUDED.couponcode,
            purchase_source = EXCLUDED.purchase_source,
            appleinapp_id = EXCLUDED.appleinapp_id,
            apple_failurecode = EXCLUDED.apple_failurecode,
            receiptid = EXCLUDED.receiptid,
            payment_method = EXCLUDED.payment_method,
            fee_rate = EXCLUDED.fee_rate,
            fee_flat = EXCLUDED.fee_flat,
            vat_rate = EXCLUDED.vat_rate,
            net_price = EXCLUDED.net_price,
            inc_date = EXCLUDED.res_charge_datetime;  -- Update inc_date with res_charge_datetime
        """
        pg_hook.run(upsert_query)
        logging.info("Successfully upserted latest run ODS data into DWD")
    except Exception as e:
        logging.error(f"Error upserting into DWD: {str(e)}")
        raise


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id="ETL_purchase_transactions_dtl_di",
    default_args=default_args,
    schedule_interval="0 12 * * *",  # Different time from previous DAG
    catchup=False
) as dag:
    extract_data = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data_from_mssql
    )
    
    load_ods = PythonOperator(
        task_id="load_ods",
        python_callable=load_data_into_ods
    )
    
    upsert_dwd = PythonOperator(
        task_id="upsert_dwd",
        python_callable=upsert_data_into_dwd
    )
    
    extract_data >> load_ods >> upsert_dwd