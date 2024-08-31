from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from modules.helpers import get_snowflake_engine
from modules.extract import run_extraction
from modules.clean import run_cleaning
from modules.load import load_csv_to_snowflake, exec_procedure
# from modules.load import load_csv_to_snowflake, exec_procedure

# getting engine
engine = get_snowflake_engine()

# default arguments
default_args = {
    'owner': 'tolu_samuel',
    #'depends_on_past': False,
    'start_date': datetime(2024, 8, 29),
    'email': 'tolutosin16@gmail.org',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': None,
    #'retries_delay': None
    # 'retries': 1,
    # 'retries_delay': timedelta(minutes=1)
}

# instantiate the DAG
with DAG(
    'nyc_dag',
    default_args = default_args,
    description = 'this is nyc payroll data dag',
    schedule_interval = '0 0 1 1 *',  #shedule to run at midnight every Jan 1st
    catchup = False,
) as dag:
# alternative way   
# dag = DAG(
#     'nyc_dag',
#     default_args=default_args,
#     description='nyc batch etl pipeline'
# )


# task for extracting data
    extraction = PythonOperator(
        task_id='extract_data',
        python_callable=run_extraction,
        #dag=dag,
    )

# task for data cleaning
    cleaning = PythonOperator(
        task_id='clean_data',
        python_callable=run_cleaning
        #dag=dag,
    )

# task for loading to snowflake STG schema
    stg_loading = PythonOperator(
        task_id='stg_load',
        python_callable=load_csv_to_snowflake,
        op_kwargs = {'table_name' : 'nyc_payroll', 'engine' : engine, 'schema': 'STG'}
        #dag=dag,
    )

# task for loading to snowflake PRD schema
    prd_loading = PythonOperator(
        task_id='prd_load',
        python_callable=exec_procedure,
        op_kwargs = {'engine' : engine}
        #dag=dag,
    )

# set dependencies
extraction >> cleaning >> stg_loading >> prd_loading 


