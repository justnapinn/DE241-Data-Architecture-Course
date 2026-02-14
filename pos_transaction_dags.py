from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models.param import Param
from datetime import datetime
import pandas as pd
import hashlib

# ชื่อ Connection ID ใน Airflow (ต้องตรงกับที่คุณตั้ง)
CONN_ID = 'mysql_mydb'

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0
}

def prepare_source_data():
    """Step 0: แบ่ง 1,000 rows ออกเป็น 5 วัน วันละ 200 rows"""
    mysql_hook = MySqlHook(mysql_conn_id=CONN_ID)
    engine = mysql_hook.get_sqlalchemy_engine()
    
    # 1. ดึงข้อมูลทั้งหมดมา 1,000 rows
    df = pd.read_sql("SELECT * FROM raw_data_pos_sales_data ORDER BY Transaction_ID", engine)
    
    if len(df) == 0:
        print("No data in raw_data_pos_sales_data")
        return

    # 2. สร้าง List ของวันที่ 5 วัน
    target_dates = ['2024-01-01', '2024-01-02', '2024-01-03', '2024-01-04', '2024-01-05']
    
    # 3. วนลูปแก้ค่า Date ใน DataFrame
    # ใช้การแบ่ง index เช่น 0-199, 200-399, ...
    for i, date_str in enumerate(target_dates):
        start_idx = i * 200
        end_idx = (i + 1) * 200
        df.iloc[start_idx:end_idx, df.columns.get_loc('Date')] = date_str

    # 4. เขียนกลับลงตารางเดิม (แบบ Overwrite)
    df.to_sql('raw_data_pos_sales_data', engine, if_exists='replace', index=False)
    print("Prepared 1,000 rows into 5 days successfully.")

def generate_store_id(location):
    """สร้าง Hash ID จาก Location เพื่อให้ได้ ID เดิมเสมอ"""
    if pd.isna(location):
        return None
    # ใช้ MD5 hash แล้วตัดมาสัก 10 ตัวอักษรก็พอ
    return hashlib.md5(str(location).encode('utf-8')).hexdigest()[:10]

def extract_transform_load_staging(**kwargs):
    # รับค่า Date จากการ Trigger (Params)
    target_date = kwargs['params']['transaction_date']
    print(f"Processing Data for Date: {target_date}")
    
    # 1. เชื่อมต่อ Database
    mysql_hook = MySqlHook(mysql_conn_id=CONN_ID)
    engine = mysql_hook.get_sqlalchemy_engine()
    
    # 2. Extract: ดึง Raw Data ตามวันที่ระบุ
    sql_query = f"""
        SELECT * FROM raw_data_pos_sales_data 
        WHERE Date = '{target_date}'
    """
    df = pd.read_sql(sql_query, engine)
    
    if df.empty:
        print(f"No data found for {target_date}")
        return

    # 3. Transform (Python)
    # 3.1 สร้าง store_id
    df['store_id'] = df['Store_Location'].apply(generate_store_id)
    
    # 3.2 คำนวณ Total Price (เผื่อไว้)
    # แปลง type ให้ชัวร์ก่อนคำนวณ
    df['Quantity'] = pd.to_numeric(df['Quantity'])
    df['Unit_Price'] = pd.to_numeric(df['Unit_Price'])
    df['Total_Price'] = df['Quantity'] * df['Unit_Price']
    
    # 3.3 เพิ่ม ingest_date
    df['ingest_date'] = datetime.now()
    
    # 4. Load to Staging (Idempotency)
    # ลบข้อมูลเก่าของวันที่นี้ทิ้งก่อน (Clear data)
    with mysql_hook.get_conn() as conn:
        with conn.cursor() as cursor:
            delete_sql = f"DELETE FROM stg_pos_transaction WHERE Date = '{target_date}'"
            cursor.execute(delete_sql)
            conn.commit()
            print(f"Deleted old staging data for {target_date}")

    # Insert ข้อมูลใหม่
    df.to_sql('stg_pos_transaction', engine, if_exists='append', index=False)
    print(f"Inserted {len(df)} rows to staging.")

with DAG(
    dag_id='pos_sales_etl_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    params={
        "transaction_date": Param("2024-01-01", type="string", description="Date to process (YYYY-MM-DD)")
    },
    tags=['etl', 'pos', 'mysql']
) as dag:
    
    # --- Step 0: Data Prep (ทำครั้งเดียว หรือทำทุกครั้งที่รันก็ได้เพื่อให้ data ชัวร์) ---
    task_prepare_data = PythonOperator(
        task_id='prepare_raw_data_step0',
        python_callable=prepare_source_data
    )

    # --- Step 1: Raw to Staging (Python + Pandas) ---
    task_extract_staging = PythonOperator(
        task_id='extract_to_staging',
        python_callable=extract_transform_load_staging
    )

    # --- Step 2: Load Dimensions (SQL) ---
    # ใช้ INSERT IGNORE เพื่อลงเฉพาะข้อมูลใหม่ (ถ้ามี ID ซ้ำให้ข้าม)
    
    task_load_dim_customer = SQLExecuteQueryOperator(
        task_id='load_dim_customer',
        conn_id=CONN_ID,
        sql="""
            INSERT IGNORE INTO dim_customer (Customer_ID, Customer_Name)
            SELECT DISTINCT Customer_ID, NULL 
            FROM stg_pos_transaction 
            WHERE Date = '{{ params.transaction_date }}';
        """
    )

    task_load_dim_store = SQLExecuteQueryOperator(
        task_id='load_dim_store',
        conn_id=CONN_ID,
        sql="""
            INSERT IGNORE INTO dim_store (store_id, store_location)
            SELECT DISTINCT store_id, Store_Location 
            FROM stg_pos_transaction 
            WHERE Date = '{{ params.transaction_date }}';
        """
    )

    task_load_dim_product = SQLExecuteQueryOperator(
        task_id='load_dim_product',
        conn_id=CONN_ID,
        sql="""
            INSERT IGNORE INTO dim_product (Product_ID, Product_Name, Unit_Price)
            SELECT DISTINCT Product_ID, Product_Name, Unit_Price 
            FROM stg_pos_transaction 
            WHERE Date = '{{ params.transaction_date }}';
        """
    )

    # --- Step 3: Load Fact Table (SQL) ---
    # ลบ Fact ของวันนั้นก่อน แล้ว Insert ใหม่
    task_load_fact = SQLExecuteQueryOperator(
        task_id='load_fact_transaction',
        conn_id=CONN_ID,
        sql="""
            DELETE FROM fact_transaction WHERE Date = '{{ params.transaction_date }}';

            INSERT INTO fact_transaction 
            (Transaction_ID, Date, Product_ID, Customer_ID, store_id, Payment_Method, Quantity, Total_Price, processed_date)
            SELECT 
                Transaction_ID, 
                Date, 
                Product_ID, 
                Customer_ID, 
                store_id, 
                Payment_Method, 
                Quantity, 
                Total_Price,
                NOW()
            FROM stg_pos_transaction
            WHERE Date = '{{ params.transaction_date }}';
        """
    )

    # --- Step 4: Load Data Mart (SQL) ---
    # คำนวณยอดขายรายวันแยกตาม Store
    task_load_mart = SQLExecuteQueryOperator(
        task_id='load_mart_store_sale',
        conn_id=CONN_ID,
        sql="""
            DELETE FROM mart_store_sale_daily WHERE Date = '{{ params.transaction_date }}';

            INSERT INTO mart_store_sale_daily (store_id, Date, Total_Sales, processed_date)
            SELECT 
                store_id,
                Date,
                SUM(Total_Price) as Total_Sales,
                NOW()
            FROM fact_transaction
            WHERE Date = '{{ params.transaction_date }}'
            GROUP BY store_id, Date;
        """
    )

    # --- Dependencies ---
    # 1. Staging เสร็จก่อน
    # 2. Dimensions รันพร้อมกันได้
    # 3. Fact รอ Dimensions ครบ
    # 4. Mart รอ Fact เสร็จ
    
    task_prepare_data >> task_extract_staging >> [task_load_dim_customer, task_load_dim_store, task_load_dim_product]
    [task_load_dim_customer, task_load_dim_store, task_load_dim_product] >> task_load_fact
    task_load_fact >> task_load_mart