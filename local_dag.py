from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago

# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Create the DAG
dag = DAG(
    'local_beam_pipeline_dag',
    default_args=default_args,
    description='A DAG to run the local Beam pipeline',
    schedule_interval='@daily',
)

# Task to run Beam job
run_beam_job = BashOperator(
    task_id='run_beam_pipeline',
    bash_command='python ~/Desktop/apacheBeam/beam.py',  # Replace with the path to your beam.py
    dag=dag,
)

run_beam_job
