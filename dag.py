from airflow import DAG
from airflow.providers.google.cloud.operators.dataflow import DataflowCreatePythonJobOperator
from airflow.utils.dates import days_ago

# Define default args
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

# Create the DAG
dag = DAG(
    'word_count_pipeline_dag',
    default_args=default_args,
    description='A DAG to run the word count pipeline daily',
    schedule_interval='@daily',
)

# Task to run Dataflow job
run_dataflow_job = DataflowCreatePythonJobOperator(
    task_id='run_word_count_pipeline',
    py_file='gs://dataflow-apache-22396/dataflow_beam.py',  # Replace with your GCS path
    job_name='word-count-job',  # Unique job name
    project_id='<project-ids>',  # Replace with your project ID
    location='asia-south1',  # Replace with your Dataflow region
    dag=dag,
)

run_dataflow_job