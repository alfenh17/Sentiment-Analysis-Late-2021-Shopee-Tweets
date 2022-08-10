from datetime import timedelta
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
from airflow import DAG

default_args = {
    'owner': 'Kelompok_6',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_retry': False,
    'catchup' : False,
    'retries': 5,
    'retry_delay' : timedelta(minutes=2),
}
with DAG(
    'Shopee_Twitter_Data_Stream',
    default_args= default_args,
    description='Scheduling For Shopee Twitter Data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 12, 26),
) as dag:

    stream        = BashOperator(
                    task_id='stream_data',
                    bash_command='python Tap.py'
                    )

    etl        = BashOperator(
                    task_id='extract_transform_load_twitter_data',
                    bash_command='python etl.py'
                    )
    dump_to_csv = BashOperator(
                    task_id='dump_db_to_csv',
                    bash_command='python dump_to_csv.py'
                    )

    stream.set_downstream(etl)
    etl.set_downstream(dump_to_csv)


