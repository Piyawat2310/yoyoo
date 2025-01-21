from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from datetime import datetime

# กำหนดค่าเริ่มต้นของ DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
}
dag = DAG(
    "transfer_sql_to_postgresql",
    default_args=default_args,
    description="Migrate data from SQL Server to PostgreSQL",
    schedule_interval=None,
    catchup=False,
)

# SQL Server Query
sqlserver_query = """
--ProfileDetail
use BWM_FIT_DB

declare @inMonth varchar(10);
declare @startDate date;
declare @endDate date;
--set @inMonth = format(GETDATE(),'yyyy-MM')+'%';
set @inMonth = '2024-07%'
set @startDate = REPLACE(REPLACE(@inMonth,'-',''),'%','')+'01';
set @endDate = DATEADD(s,-1,DATEADD(mm, DATEDIFF(m,0,@startDate)+1,0))

--select @enddate

select 
FORMAT(@endDate,'dd/MM/yyyy','en-us') AS_OF_DATE,
--max(rinv.REQUEST_INVESTOR_ID) as REQUEST_INVESTOR_ID
--, isnull(max(cus.CIF_CODE), '') as cif_code
isnull(convert(nvarchar(30),cus.customer_id),'') CUSTOMER_ID,
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
 END) as REQ_STATUS
, '1' COUNT_
from dbo.REQUEST_INVESTOR_OPEN_ACCOUNTS rinv
left join BWM_SA_DB.dbo.SA_CUSTOMER cus on rinv.id_no = cus.regis_card_no
left join dbo.EDUCATIONS edu on edu.EDUCATION_ID = rinv.EDUCATION_ID
left join dbo.OCCUPATIONS occ on occ.OCCUPATION_ID = rinv.OCCUPATION_ID
left join dbo.BUSINESS_TYPES bus on bus.BUSINESS_TYPE_ID = rinv.BUSINESS_TYPE_ID
left join dbo.REQUEST_ADDRESSES haddr on haddr.REQUEST_ADDRESS_ID = rinv.REQUEST_HOME_ADDRESS_ID
left join dbo.SUBDISTRICTS hsub on hsub.SUBDISTRICT_ID = haddr.SUBDISTRICT_ID
left join dbo.DISTRICTS hdis on hdis.DISTRICT_ID = hsub.DISTRICT_ID
left join dbo.PROVINCES hpro on hpro.PROVINCE_ID = hdis.PROVINCE_ID
left join dbo.REQUEST_ADDRESSES waddr on waddr.REQUEST_ADDRESS_ID = rinv.REQUEST_WORKPLACE_ADDRESS_ID
left join dbo.SUBDISTRICTS wsub on wsub.SUBDISTRICT_ID = waddr.SUBDISTRICT_ID
left join dbo.DISTRICTS wdis on wdis.DISTRICT_ID = wsub.DISTRICT_ID
left join dbo.PROVINCES wpro on wpro.PROVINCE_ID = wdis.PROVINCE_ID
left JOIN REQUEST_UH_OPEN_ACCOUNTS RUH ON RUH.REQUEST_INVESTOR_ID = RINV.REQUEST_INVESTOR_ID
left join BWM_sa_db.dbo.SA_RISK_LEVEL srl on srl.customer_id = cus.customer_id
where --RUH.REQUEST_STATUS NOT IN ('CANCELLED') 
convert(date, rinv.CREATE_DATETIME) <= @endDate
--and cus.customer_id in (1304,1647,2048,2188,2355,2870,3263,3702,4118,4280,4292,4334,4575,7131,7241,7310,7311,7312,7505,7848,7925,8180,8334,8912,9444,9475,9538)
group by rinv.email, cus.customer_id, srl.risk_level
order by create_date
"""

# Function: Fetch data from MSSQL
def fetch_sqlserver_data():
    sqlserver_hook = MsSqlHook(mssql_conn_id="company_connection")
    conn = sqlserver_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute(sqlserver_query)
    rows = cursor.fetchall()
    return rows

# Function: Create PostgreSQL Table
def create_postgres_table():
    postgres_hook = PostgresHook(postgres_conn_id="INVENTORY")
    create_table_query = """
    CREATE TABLE IF NOT EXISTS profile_detail (
        as_of_date TEXT,
        customer_id TEXT,
        kyc_level TEXT,
        sex TEXT,
        marital_status TEXT,
        age INT,
        range_age TEXT,
        education TEXT,
        monthly_income INT,
        range_monthly_income TEXT,
        occupation_name TEXT,
        business_type TEXT,
        home_address TEXT,
        home_district TEXT,
        work_address TEXT,
        work_district TEXT,
        work_district_group TEXT,
        create_date DATE,
        req_status TEXT,
        count_ INT
    );
    """
    postgres_hook.run(create_table_query)

# Function: Insert Data into PostgreSQL
def insert_into_postgres(rows):
    postgres_hook = PostgresHook(postgres_conn_id="INVENTORY")
    insert_query = """
    INSERT INTO profile_detail (
        as_of_date, customer_id, kyc_level, sex, marital_status, age, range_age,
        education, monthly_income, range_monthly_income, occupation_name,
        business_type, home_address, home_district, work_address, work_district,
        work_district_group, create_date, req_status, count_
    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    postgres_hook.insert_rows(table="profile_detail", rows=rows)

# Function: Validate Data in PostgreSQL
def validate_postgres_data():
    postgres_hook = PostgresHook(postgres_conn_id="INVENTORY")
    select_query = "SELECT * FROM profile_detail LIMIT 10;"
    records = postgres_hook.get_records(select_query)
    print("Sample Records:")
    for record in records:
        print(record)

# Define Tasks
fetch_data_task = PythonOperator(
    task_id="fetch_sqlserver_data",
    python_callable=fetch_sqlserver_data,
    dag=dag,
)

create_table_task = PythonOperator(
    task_id="create_postgres_table",
    python_callable=create_postgres_table,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id="insert_into_postgres",
    python_callable=insert_into_postgres,
    op_args=[fetch_data_task.output],  # ส่ง output จาก fetch_data_task
    dag=dag,
)

validate_data_task = PythonOperator(
    task_id="validate_postgres_data",
    python_callable=validate_postgres_data,
    dag=dag,
)

# Define Task Dependencies
fetch_data_task >> create_table_task >> insert_data_task >> validate_data_task
