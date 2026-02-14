from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.mysql.hooks.mysql import MySqlHook
from airflow.models.param import Param
from datetime import datetime
import pandas as pd
import os
import hashlib

# --- CONFIGURATION ---
CONN_ID = 'mysql_mydb'
BASE_PATH = '/opt/airflow/datalake'  # ใช้ / เพื่อป้องกันปัญหากับ Python

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 0
}

def create_folder_structure(**kwargs):
    """Step 1: สร้าง Folder พื้นฐานเตรียมไว้"""
    folders = [
        'staging',
        'bronze',
        'silver/fact_transaction',
        'silver/dim_product',
        'silver/dim_customer',
        'silver/dim_store',
        'gold/mart_store_sale_daily',
        'sharing/mart_store_sale_daily'
    ]
    
    for folder in folders:
        path = os.path.join(BASE_PATH, folder)
        os.makedirs(path, exist_ok=True)
        print(f"Created/Checked folder: {path}")

def extract_to_staging(**kwargs):
    """Step 2: MySQL -> Staging (CSV)"""
    target_date = kwargs['params']['transaction_date'] # yyyy-mm-dd
    
    # Connect MySQL
    hook = MySqlHook(mysql_conn_id=CONN_ID)
    sql = f"SELECT * FROM raw_data_pos_sales_data WHERE Date = '{target_date}'"
    df = hook.get_pandas_df(sql)
    
    if df.empty:
        print(f"No data for {target_date}")
        return

    # Create Path: staging/<Date>/
    save_dir = os.path.join(BASE_PATH, 'staging', target_date)
    os.makedirs(save_dir, exist_ok=True)
    
    # Save CSV
    file_path = os.path.join(save_dir, 'raw_data.csv')
    df.to_csv(file_path, index=False)
    print(f"Saved Staging CSV at: {file_path}")

def staging_to_bronze(**kwargs):
    """Step 3: Staging (CSV) -> Bronze (Parquet)"""
    target_date = kwargs['params']['transaction_date']
    
    # Read Staging
    staging_path = os.path.join(BASE_PATH, 'staging', target_date, 'raw_data.csv')
    if not os.path.exists(staging_path):
        print("Staging file not found.")
        return
        
    df = pd.read_csv(staging_path)
    
    # Create Path: bronze/date=<Date>/
    # ใช้ Hive Partition Style (date=xxxx) จะดีกว่า แต่ทำตามโจทย์คือแค่เปลี่ยน format
    bronze_dir = os.path.join(BASE_PATH, 'bronze', f'date={target_date}')
    os.makedirs(bronze_dir, exist_ok=True)
    
    save_path = os.path.join(bronze_dir, 'raw_data.parquet')
    df.to_parquet(save_path, index=False)
    print(f"Saved Bronze Parquet at: {save_path}")

def generate_store_id(location):
    if pd.isna(location): return None
    return hashlib.md5(str(location).encode('utf-8')).hexdigest()[:10]

def bronze_to_silver(**kwargs):
    """Step 4: Bronze -> Silver (Normalize & Split)"""
    target_date = kwargs['params']['transaction_date']
    
    # Read Bronze
    bronze_path = os.path.join(BASE_PATH, 'bronze', f'date={target_date}', 'raw_data.parquet')
    if not os.path.exists(bronze_path):
        return
    
    df = pd.read_parquet(bronze_path)
    
    # --- Transform ---
    df['store_id'] = df['Store_Location'].apply(generate_store_id)
    df['Quantity'] = pd.to_numeric(df['Quantity'])
    df['Unit_Price'] = pd.to_numeric(df['Unit_Price'])
    df['Total_Price'] = df['Quantity'] * df['Unit_Price']
    df['processed_date'] = datetime.now()

    # --- 1. Dim Customer ---
    dim_cust = df[['Customer_ID']].drop_duplicates()
    dim_cust['Customer_Name'] = None # ตามโจทย์
    dim_cust['processed_date'] = datetime.now()
    
    path_cust = os.path.join(BASE_PATH, 'silver/dim_customer', f'date={target_date}')
    os.makedirs(path_cust, exist_ok=True)
    dim_cust.to_parquet(os.path.join(path_cust, 'data.parquet'), index=False)

    # --- 2. Dim Product ---
    dim_prod = df[['Product_ID', 'Product_Name', 'Unit_Price']].drop_duplicates()
    dim_prod['processed_date'] = datetime.now()
    
    path_prod = os.path.join(BASE_PATH, 'silver/dim_product', f'date={target_date}')
    os.makedirs(path_prod, exist_ok=True)
    dim_prod.to_parquet(os.path.join(path_prod, 'data.parquet'), index=False)

    # --- 3. Dim Store ---
    dim_store = df[['store_id', 'Store_Location']].drop_duplicates()
    dim_store['processed_date'] = datetime.now()
    
    path_store = os.path.join(BASE_PATH, 'silver/dim_store', f'date={target_date}')
    os.makedirs(path_store, exist_ok=True)
    dim_store.to_parquet(os.path.join(path_store, 'data.parquet'), index=False)

    # --- 4. Fact Transaction ---
    # เลือกเฉพาะ column ที่เป็น Fact
    fact_cols = ['Transaction_ID', 'Date', 'Product_ID', 'Customer_ID', 
                 'store_id', 'Payment_Method', 'Quantity', 'Total_Price', 'processed_date']
    fact_df = df[fact_cols]
    
    path_fact = os.path.join(BASE_PATH, 'silver/fact_transaction', f'date={target_date}')
    os.makedirs(path_fact, exist_ok=True)
    fact_df.to_parquet(os.path.join(path_fact, 'data.parquet'), index=False)
    
    print(f"Silver Layer Normalized for {target_date}")

def silver_to_gold(**kwargs):
    """Step 5: Silver (Fact) -> Gold (Aggregated)"""
    target_date = kwargs['params']['transaction_date']
    
    # Read Fact from Silver
    fact_path = os.path.join(BASE_PATH, 'silver/fact_transaction', f'date={target_date}', 'data.parquet')
    if not os.path.exists(fact_path):
        return
    
    df_fact = pd.read_parquet(fact_path)
    
    # Aggregate
    df_mart = df_fact.groupby(['store_id', 'Date'])['Total_Price'].sum().reset_index()
    df_mart.rename(columns={'Total_Price': 'Total_Sales'}, inplace=True)
    df_mart['processed_date'] = datetime.now()
    
    # Save to Gold (Partition by sale_date)
    gold_path = os.path.join(BASE_PATH, 'gold/mart_store_sale_daily', f'sale_date={target_date}')
    os.makedirs(gold_path, exist_ok=True)
    
    df_mart.to_parquet(os.path.join(gold_path, 'data.parquet'), index=False)
    print(f"Gold Layer Aggregated for {target_date}")

def gold_to_sharing(**kwargs):
    """Step 6: Gold -> Sharing (CSV)"""
    target_date = kwargs['params']['transaction_date']
    
    # Read Gold
    gold_path = os.path.join(BASE_PATH, 'gold/mart_store_sale_daily', f'sale_date={target_date}', 'data.parquet')
    if not os.path.exists(gold_path):
        return
        
    df_mart = pd.read_parquet(gold_path)
    
    # Save to Sharing
    share_path = os.path.join(BASE_PATH, 'sharing/mart_store_sale_daily', f'sale_date={target_date}')
    os.makedirs(share_path, exist_ok=True)
    
    # Export CSV
    df_mart.to_csv(os.path.join(share_path, 'data.csv'), index=False)
    print(f"Sharing Layer Exported for {target_date}")

with DAG(
    dag_id='local_datalake_elt_pipeline',
    default_args=default_args,
    schedule=None,
    catchup=False,
    params={
        "transaction_date": Param("2024-01-01", type="string", description="Date to process (YYYY-MM-DD)")
    },
    tags=['etl', 'datalake', 'local']
) as dag:

    t1_create_folders = PythonOperator(
        task_id='create_folder_structure',
        python_callable=create_folder_structure
    )

    t2_extract = PythonOperator(
        task_id='extract_to_staging',
        python_callable=extract_to_staging
    )

    t3_bronze = PythonOperator(
        task_id='staging_to_bronze',
        python_callable=staging_to_bronze
    )

    t4_silver = PythonOperator(
        task_id='bronze_to_silver',
        python_callable=bronze_to_silver
    )

    t5_gold = PythonOperator(
        task_id='silver_to_gold',
        python_callable=silver_to_gold
    )

    t6_sharing = PythonOperator(
        task_id='gold_to_sharing',
        python_callable=gold_to_sharing
    )

    # Dependency Flow
    t1_create_folders >> t2_extract >> t3_bronze >> t4_silver >> t5_gold >> t6_sharing