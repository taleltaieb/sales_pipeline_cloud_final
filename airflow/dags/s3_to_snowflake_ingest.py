from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests
import snowflake.connector

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# S3 base URL and file mappings
TABLES = {
    "sales_train": {
        "filename": "sales_train.csv",
        "schema": """
            date TEXT,
            date_block_num INT,
            shop_id INT,
            item_id INT,
            item_price FLOAT,
            item_cnt_day FLOAT
        """
    },
    "items": {
        "filename": "items.csv",
        "schema": """
            item_name TEXT,
            item_id INT,
            item_category_id INT
        """
    },
    "item_categories": {
        "filename": "item_categories.csv",
        "schema": """
            item_category_name TEXT,
            item_category_id INT
        """
    },
    "shops": {
        "filename": "shops.csv",
        "schema": """
            shop_name TEXT,
            shop_id INT
        """
    }
}

def load_csv_to_snowflake(s3_filename, table_name, table_schema):
    # Download CSV from S3
    s3_url = f"https://sales-pipeline-talel.s3.eu-north-1.amazonaws.com/raw/{s3_filename}"
    local_file = f"/tmp/{s3_filename}"

    response = requests.get(s3_url)
    response.raise_for_status()
    with open(local_file, 'wb') as f:
        f.write(response.content)

    # Connect to Snowflake
    conn = snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse='COMPUTE_WH',
        database='SALES_PIPELINE',
        schema='RAW',
        role='ACCOUNTADMIN'
    )

    cs = conn.cursor()
    try:
        cs.execute("CREATE OR REPLACE STAGE RAW.my_stage;")
        cs.execute(f"PUT file://{local_file} @RAW.my_stage AUTO_COMPRESS=TRUE OVERWRITE=TRUE;")

        # Create table dynamically
        cs.execute(f"CREATE OR REPLACE TABLE RAW.{table_name} ({table_schema});")

        # Load data
        cs.execute(f"""
        COPY INTO RAW.{table_name}
        FROM @RAW.my_stage/{s3_filename}.gz
        FILE_FORMAT = (
            TYPE = 'CSV'
            FIELD_OPTIONALLY_ENCLOSED_BY = '"'
            SKIP_HEADER = 1
            ENCODING = 'UTF8'
            ESCAPE_UNENCLOSED_FIELD = NONE
        )
        ON_ERROR = 'CONTINUE';
        """)
    finally:
        cs.close()
        conn.close()

with DAG(
    dag_id='s3_to_snowflake_python',
    default_args=default_args,
    description='Download CSVs from public S3 and load to Snowflake RAW schema',
    schedule_interval='@daily',
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['sales_pipeline'],
) as dag:

    tasks = []
    for table_name, config in TABLES.items():
        task = PythonOperator(
            task_id=f"load_{table_name}_to_snowflake",
            python_callable=load_csv_to_snowflake,
            op_args=[config["filename"], table_name, config["schema"]],
        )
        tasks.append(task)

    # Optional: define dependencies if needed
    tasks[0] >> tasks[1:]  # sales_train first, others after
