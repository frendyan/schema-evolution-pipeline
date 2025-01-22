from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import sqlalchemy as sa

# Database connection string
DB_URI = "postgresql+psycopg2://airflow:airflow@postgres:5432/dwh"

def extract(**kwargs):
    with open('/opt/airflow/dags/data/sample_data.json', 'r') as file:
        return json.load(file)

def load(**kwargs):
    engine = sa.create_engine(DB_URI)
    data = kwargs['ti'].xcom_pull(task_ids='extract')
    
    with engine.connect() as conn:
        # Fetch existing columns
        existing_columns = conn.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_schema = 'staging' AND table_name = 'user_data';"
        ).fetchall()
        existing_columns = {col[0] for col in existing_columns}
        
        # Check for new columns
        for row in data:
            for key in row.keys():
                if key not in existing_columns:
                    # Add new column if it doesn't exist
                    print(f"Adding new column: {key}")
                    conn.execute(f"ALTER TABLE staging.user_data ADD COLUMN {key} TEXT;")
                    existing_columns.add(key)

        # Insert data into the table
        for row in data:
            keys = ', '.join(row.keys())
            values = ', '.join([f"'{v}'" for v in row.values()])
            query = f"INSERT INTO staging.user_data ({keys}) VALUES ({values});"
            conn.execute(query)

with DAG(
    'etl_pipeline',
    default_args={'retries': 1},
    schedule_interval=None,
    start_date=datetime(2025, 1, 22),
    catchup=False,
) as dag:
    extract_task = PythonOperator(task_id='extract', python_callable=extract)
    load_task = PythonOperator(task_id='load', python_callable=load)

    extract_task >> load_task
