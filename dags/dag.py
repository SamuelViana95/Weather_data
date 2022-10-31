from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta

with DAG('weather_data_extract',start_date=datetime(2022,10,29), 
    schedule_interval='@hourly', catchup=False) as dag:

    extract_data = BashOperator(
        task_id = 'extract_data',
        bash_command = 'python3 /root/project/get_data.py /root/project/'
    )

    get_average = BashOperator(
        task_id = 'get_average',
        bash_command = 'python3 /root/project/get_avg.py /root/project/'
    )

    merge_df = BashOperator(
        task_id = 'merge_dataframes',
        bash_command = 'python3 /root/project/merge_df.py /root/project/'
    )

    extract_data >> get_average >> merge_df
