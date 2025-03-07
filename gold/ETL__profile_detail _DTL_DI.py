from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime
import pandas as pd

# กำหนดค่าเริ่มต้นสำหรับ DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}

# สร้าง DAG
dag = DAG(
    "ETL_profile_detail_DTL_DI",
    default_args=default_args,
    description="ย้ายข้อมูลจาก SQL Server ไปยัง PostgreSQL พร้อมจัดการข้อมูลใน ODS และ DWD",
    schedule_interval='0 18 * * *',
    catchup=False,
)

# SQL Query จาก SQL Server (ใช้ query เดิมของคุณ)
sqlserver_query = """
--ProfileDetail
use BWM_FIT_DB

select 
isnull(convert(nvarchar(30),cus.customer_id),rinv.email) CUSTOMER_ID,
isnull(convert(nvarchar(1),srl.risk_level),'') KYC_LEVEL,
case max(rinv.GENDER)
when 'Female' then 'หญิง'
when 'Male' then 'ชาย' 
else '' end as SEX
, case max(rinv.MARITAL_STATUS)
when 'Single' then 'โสด'
when 'Married' then 'แต่งงาน'
when 'Widow' then 'หม้าย'
when 'Divorced' then 'หย่าร้าง'
else '' end as MARITAL_STATUS
--, CONVERT(date, max(rinv.BIRTHDATE)) as BIRTHDATE
, datediff(year, max(rinv.BIRTHDATE),getdate()) as AGE
, case when (datediff(year, max(rinv.BIRTHDATE),getdate()) <= 17) then '1) 0-17' 
when (datediff(year, max(rinv.BIRTHDATE),getdate()) >= 18 and datediff(year, max(rinv.BIRTHDATE),getdate()) <= 24) then '2) 18-24' 
when (datediff(year, max(rinv.BIRTHDATE),getdate()) >= 25 and datediff(year, max(rinv.BIRTHDATE),getdate()) <= 34) then '3) 25-34' 
when (datediff(year, max(rinv.BIRTHDATE),getdate()) >= 35 and datediff(year, max(rinv.BIRTHDATE),getdate()) <= 44) then '4) 35-44' 
when (datediff(year, max(rinv.BIRTHDATE),getdate()) >= 45 and datediff(year, max(rinv.BIRTHDATE),getdate()) <= 54) then '5) 45-54' 
when (datediff(year, max(rinv.BIRTHDATE),getdate()) >= 55 and datediff(year, max(rinv.BIRTHDATE),getdate()) <= 64) then '6) 55-64' 
when (datediff(year, max(rinv.BIRTHDATE),getdate()) >= 65) then '7) 65+' end as RANGE_AGE

, max(edu.EDUCATION_NAME_THAI) as EDUCATION
, max(rinv.MONTHLY_INCOME) as MONTHLY_INCOME
, case when max(rinv.MONTHLY_INCOME) <= 5000 then '1) 0-5,000'
when (max(rinv.MONTHLY_INCOME) >= 5001 and max(rinv.MONTHLY_INCOME) <= 10000) then '2) 5,001-10,000'
when (max(rinv.MONTHLY_INCOME) >= 10001 and max(rinv.MONTHLY_INCOME) <= 20000) then '3) 10,001-20,000'
when (max(rinv.MONTHLY_INCOME) >= 20001 and max(rinv.MONTHLY_INCOME) <= 50000) then '4) 20,001-50,000'
when (max(rinv.MONTHLY_INCOME) >= 50001 and max(rinv.MONTHLY_INCOME) <= 90000) then '5) 50,001-90,000'
when max(rinv.MONTHLY_INCOME) >= 90001 then '6) 90,001+'
end as RANGE_MONTHLY_INCOME
, case when max(occ.OCCUPATION_NAME_THAI) = 'อื่นๆ' then max(rinv.OCCUPATION_SPECIFY_OTHER) else max(occ.OCCUPATION_NAME_THAI) end as OCCUPATION_NAME
, case when max(bus.BUSINESS_TYPE_NAME_THAI) = 'อื่นๆ' then max(rinv.BUSINESS_TYPE_SPECIFY_OTHER) else max(bus.BUSINESS_TYPE_NAME_THAI) end as BUSINESS_TYPE
, max(hpro.PROVINCE_NAME_THAI) as HOME_ADDRESS , max(hdis.DISTRICT_NAME_THAI) as HOME_DISTRICT
, isnull(max(wpro.PROVINCE_NAME_THAI), '') as WORK_ADDRESS , isnull(max(wdis.DISTRICT_NAME_THAI), '') as WORK_DISTRICT
, case when isnull(max(wdis.DISTRICT_NAME_THAI), '') in ('บางซื่อ', 'ดุสิต', 'พญาไท', 'ราชเทวี', 'ปทุมวัน', 'พระนคร', 'ป้อมปราบศัตรูพ่าย', 'สัมพันธวงศ์', 'บางรัก')
then 'บางซื่อ ดุสิต พญาไท ราชเทวี ปทุมวัน พระนคร ป้อมปราบศัตรูพ่าย สัมพันธวงศ์ บางรัก' 
  when isnull(max(wdis.DISTRICT_NAME_THAI), '') in ('ดอนเมือง', 'หลักสี่', 'สายไหม', 'บางเขน', 'จตุจักร', 'ลาดพร้าว', 'บึงกุ่ม', 'บางกะปิ', 'วังทองหลาง')
then 'ดอนเมือง หลักสี่ สายไหม บางเขน จตุจักร ลาดพร้าว บึงกุ่ม บางกะปิ วังทองหลาง' 
  when isnull(max(wdis.DISTRICT_NAME_THAI), '') in ('สะพานสูง', 'มีนบุรี', 'คลองสามวา', 'หนองจอก', 'ลาดกระบัง', 'ประเวศ', 'สวนหลวง', 'คันนายาว')
then 'สะพานสูง มีนบุรี คลองสามวา หนองจอก ลาดกระบัง ประเวศ สวนหลวง คันนายาว' 
  when isnull(max(wdis.DISTRICT_NAME_THAI), '') in ('ดินแดง', 'ห้วยขวาง', 'วัฒนา', 'คลองเตย', 'บางนา', 'พระโขนง', 'สาทร', 'บางคอแหลม', 'ยานนาวา')
then 'ดินแดง ห้วยขวาง วัฒนา คลองเตย บางนา พระโขนง สาทร บางคอแหลม ยานนาวา' 
  when isnull(max(wdis.DISTRICT_NAME_THAI), '') in ('บางขุนเทียน', 'บางบอน', 'จอมทอง', 'ราษฎร์บูรณะ', 'ทุ่งครุ', 'ธนบุรี', 'คลองสาน', 'บางมด')
then 'บางขุนเทียน บางบอน จอมทอง ราษฎร์บูรณะ ทุ่งครุ ธนบุรี คลองสาน บางมด' 
  when isnull(max(wdis.DISTRICT_NAME_THAI), '') in ('บางพลัด', 'ตลิ่งชัน', 'บางกอกน้อย', 'บางกอกใหญ่', 'ภาษีเจริญ', 'หนองแขม', 'ทวีวัฒนา')
then 'บางพลัด ตลิ่งชัน บางกอกน้อย บางกอกใหญ่ ภาษีเจริญ หนองแขม ทวีวัฒนา' 
  when isnull(max(wdis.DISTRICT_NAME_THAI), '') in ('เมือง', 'บางกรวย', 'บางใหญ่') and  isnull(max(wpro.PROVINCE_NAME_THAI), '') = 'นนทบุรี'
then 'อ.เมือง บางกรวย บางใหญ่' 
  when isnull(max(wdis.DISTRICT_NAME_THAI), '') in ('ปากเกร็ด', 'บางบัวทอง', 'ไทรน้อย')
then 'อ.ปากเกร็ด บางบัวทอง ไทรน้อย' 
  when isnull(max(wdis.DISTRICT_NAME_THAI), '') in ('เมือง', 'สามโคก', 'ลาดหลุมแก้ว') and  isnull(max(wpro.PROVINCE_NAME_THAI), '') = 'ปทุมธานี'
then 'อ.เมือง สามโคก ลาดหลุมแก้ว' 
  when isnull(max(wdis.DISTRICT_NAME_THAI), '') in ('คลองหลวง', 'ธัญบุรี', 'หนองเสือ', 'ลำลูกกา')
then 'อ.คลองหลวง ธัญบุรี หนองเสือ ลำลูกกา' 
  else '' end WORK_DISTRICT_GROUP
, convert(date, min(rinv.CREATE_DATETIME)) as CREATE_DATE
, MIN(CASE RUH.REQUEST_STATUS 
    WHEN 'COMPLETED' THEN '1 เปิดบัญชีสำเร็จ' 
    WHEN 'SYNCHRONIZED' THEN '2 รออนุมัติจาก บลจ.'
    WHEN 'APPROVED_DOC' THEN '2 รออนุมัติจาก บลจ.'
    WHEN 'INCOMPLETED_DOCUMENT' THEN '3 รอเอกสารเพิ่มเติม' 
    WHEN 'NEW' THEN '4 รอเอกสาร' 
    WHEN 'REJECTED' THEN '5 ไม่อนุมัติ' 
 
 WHEN 'NEW_BYMARKETING' THEN '6 สร้างโดย Marketing' 
 WHEN 'APPROVED_DOCUMENT_BYMARKETING' THEN '7 Marketing ตรวจสอบเอกสารแล้ว' 
 WHEN 'REJECTED_BYMARKETING' THEN '8 ยกเลิกโดย Marketing'     
    WHEN 'CANCELLED' THEN '9 ยกเลิกโดย Customer'      
 END) as REQ_STATUS,
 max(ab.AGENT_BRANCH_NAME_THAI) 
, '1' COUNT_
from BWM_FIT_DB.dbo.REQUEST_INVESTOR_OPEN_ACCOUNTS rinv
left join BWM_SA_DB.dbo.SA_CUSTOMER cus on rinv.id_no = cus.regis_card_no
left join BWM_FIT_DB.dbo.EDUCATIONS edu on edu.EDUCATION_ID = rinv.EDUCATION_ID
left join BWM_FIT_DB.dbo.OCCUPATIONS occ on occ.OCCUPATION_ID = rinv.OCCUPATION_ID
left join BWM_FIT_DB.dbo.BUSINESS_TYPES bus on bus.BUSINESS_TYPE_ID = rinv.BUSINESS_TYPE_ID
left join BWM_FIT_DB.dbo.REQUEST_ADDRESSES haddr on haddr.REQUEST_ADDRESS_ID = rinv.REQUEST_HOME_ADDRESS_ID
left join BWM_FIT_DB.dbo.SUBDISTRICTS hsub on hsub.SUBDISTRICT_ID = haddr.SUBDISTRICT_ID
left join BWM_FIT_DB.dbo.DISTRICTS hdis on hdis.DISTRICT_ID = hsub.DISTRICT_ID
left join BWM_FIT_DB.dbo.PROVINCES hpro on hpro.PROVINCE_ID = hdis.PROVINCE_ID
left join BWM_FIT_DB.dbo.REQUEST_ADDRESSES waddr on waddr.REQUEST_ADDRESS_ID = rinv.REQUEST_WORKPLACE_ADDRESS_ID
left join BWM_FIT_DB.dbo.SUBDISTRICTS wsub on wsub.SUBDISTRICT_ID = waddr.SUBDISTRICT_ID
left join BWM_FIT_DB.dbo.DISTRICTS wdis on wdis.DISTRICT_ID = wsub.DISTRICT_ID
left join BWM_FIT_DB.dbo.PROVINCES wpro on wpro.PROVINCE_ID = wdis.PROVINCE_ID
left JOIN BWM_FIT_DB.dbo.REQUEST_UH_OPEN_ACCOUNTS RUH ON RUH.REQUEST_INVESTOR_ID = RINV.REQUEST_INVESTOR_ID
left join BWM_sa_db.dbo.SA_RISK_LEVEL srl on srl.customer_id = cus.customer_id
left join BWM_FIT_DB.dbo.UNITHOLDERS uh on uh.REQUEST_UNITHOLDER_ID = rinv.REQUEST_INVESTOR_ID
left join BWM_FIT_DB.dbo.MARKETINGS mkt on uh.MARKETING_ID = mkt.MARKETING_ID
left join BWM_FIT_DB.dbo.AGENT_BRANCHES ab on mkt.AGENT_BRANCH_ID = ab.AGENT_BRANCH_ID
WHERE rinv.CREATE_DATETIME >= DATEADD(day, -180, GETDATE())
group by rinv.email, cus.customer_id, srl.risk_level
order by create_date
"""

# ฟังก์ชันสำหรับดึงข้อมูลจาก SQL Server
def fetch_sqlserver_data():
    """
    ดึงข้อมูลจาก SQL Server โดยใช้ query ที่กำหนด
    returns: ข้อมูลที่ดึงมาจาก SQL Server
    """
    sqlserver_hook = MsSqlHook(mssql_conn_id="company_connection")
    conn = sqlserver_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sqlserver_query)
    rows = cursor.fetchall()
    return rows

# ฟังก์ชันสำหรับประมวลผลและบันทึกข้อมูล
def process_and_insert_data(rows):
    """
    ประมวลผลและบันทึกข้อมูลลงในตาราง ODS และ DWD
    """
    postgres_hook = PostgresHook(postgres_conn_id="SESAME-DB")
    conn = postgres_hook.get_conn()
    cur = conn.cursor()
    current_date = datetime.now().date()
    
    # แปลงข้อมูลสำหรับ ODS
    processed_rows = []
    for row in rows:
        row_list = list(row)
        row_list.append(current_date)  # เพิ่ม inc_day
        processed_rows.append(tuple(row_list))
    
    try:
        # บันทึกข้อมูลลงในตาราง ODS
        ods_insert_query = """
        INSERT INTO ods_profile_detail (
            CUSTOMER_ID, KYC_LEVEL, SEX, MARITAL_STATUS, AGE, RANGE_AGE, EDUCATION, 
            MONTHLY_INCOME, RANGE_MONTHLY_INCOME, OCCUPATION_NAME, BUSINESS_TYPE, 
            HOME_ADDRESS, HOME_DISTRICT, WORK_ADDRESS, WORK_DISTRICT, WORK_DISTRICT_GROUP,
            CREATE_DATE, REQ_STATUS, AGENT_BRANCH_NAME_THAI, COUNT_, inc_day
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cur.executemany(ods_insert_query, processed_rows)
        
        # ดึงข้อมูล create_date ที่มีอยู่ในตาราง DWD
        cur.execute("SELECT create_date, customer_id FROM dwd_profile_detail")
        existing_records = {(str(row[0]), row[1]) for row in cur.fetchall()}
        
        # แยกข้อมูลสำหรับ insert และ update
        rows_to_insert = []
        rows_to_update = []
        
        for row in processed_rows:
            key = (str(row[16]), row[0])  # create_date และ customer_id
            if key in existing_records:
                rows_to_update.append(row)
            else:
                rows_to_insert.append(row)
        
        # Insert ข้อมูลใหม่
        if rows_to_insert:
            dwd_insert_query = """
            INSERT INTO dwd_profile_detail (
                CUSTOMER_ID, KYC_LEVEL, SEX, MARITAL_STATUS, AGE, RANGE_AGE, EDUCATION, 
                MONTHLY_INCOME, RANGE_MONTHLY_INCOME, OCCUPATION_NAME, BUSINESS_TYPE, 
                HOME_ADDRESS, HOME_DISTRICT, WORK_ADDRESS, WORK_DISTRICT, WORK_DISTRICT_GROUP,
                CREATE_DATE, REQ_STATUS, AGENT_BRANCH_NAME_THAI, COUNT_, inc_day
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            insert_data = [row[:-1] + (row[16],) for row in rows_to_insert]  # Set inc_day = create_date
            cur.executemany(dwd_insert_query, insert_data)

        # Update ข้อมูลที่มีอยู่
        if rows_to_update:
            dwd_update_query = """
            UPDATE dwd_profile_detail
            SET 
                KYC_LEVEL = %s, SEX = %s, MARITAL_STATUS = %s, AGE = %s, 
                RANGE_AGE = %s, EDUCATION = %s, MONTHLY_INCOME = %s, 
                RANGE_MONTHLY_INCOME = %s, OCCUPATION_NAME = %s, BUSINESS_TYPE = %s,
                HOME_ADDRESS = %s, HOME_DISTRICT = %s, WORK_ADDRESS = %s, 
                WORK_DISTRICT = %s, WORK_DISTRICT_GROUP = %s, REQ_STATUS = %s,
                AGENT_BRANCH_NAME_THAI = %s, COUNT_ = %s, inc_day = %s
            WHERE CUSTOMER_ID = %s AND CREATE_DATE = %s
            """
            
            formatted_rows = []
            for row in rows_to_update:
                # จัดเตรียมข้อมูลสำหรับ SET clause
                update_data = [
                    row[1],   # KYC_LEVEL
                    row[2],   # SEX
                    row[3],   # MARITAL_STATUS
                    row[4],   # AGE
                    row[5],   # RANGE_AGE
                    row[6],   # EDUCATION
                    row[7],   # MONTHLY_INCOME
                    row[8],   # RANGE_MONTHLY_INCOME
                    row[9],   # OCCUPATION_NAME
                    row[10],  # BUSINESS_TYPE
                    row[11],  # HOME_ADDRESS
                    row[12],  # HOME_DISTRICT
                    row[13],  # WORK_ADDRESS
                    row[14],  # WORK_DISTRICT
                    row[15],  # WORK_DISTRICT_GROUP
                    row[17],  # REQ_STATUS
                    row[18],  # AGENT_BRANCH_NAME_THAI
                    row[19],  # COUNT_
                    row[16],  # inc_day (CREATE_DATE)
                    row[0],   # CUSTOMER_ID (WHERE)
                    row[16]   # CREATE_DATE (WHERE)
                ]
                formatted_rows.append(tuple(update_data))
            
            cur.executemany(dwd_update_query, formatted_rows)
            
        # Commit การเปลี่ยนแปลงทั้งหมด
        conn.commit()
    
    except Exception as e:
        conn.rollback()
        raise e
    
    finally:
        cur.close()
        conn.close()

# กำหนด Tasks
fetch_data_task = PythonOperator(
    task_id="fetch_sqlserver_data",
    python_callable=fetch_sqlserver_data,
    dag=dag,
)

process_insert_task = PythonOperator(
    task_id="process_and_insert_data",
    python_callable=process_and_insert_data,
    op_args=[fetch_data_task.output],
    dag=dag,
)

# กำหนดลำดับการทำงานของ Tasks
fetch_data_task >> process_insert_task


