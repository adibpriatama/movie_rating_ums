from datetime import datetime, timedelta
import airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

from airflow.hooks.base import BaseHook
from airflow.hooks.mysql_hook import MySqlHook
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.utils.helpers import chain
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from airflow.providers.amazon.aws.sensors.glue import GlueJobSensor



PROJECT_PATH = "/home/adibp/projects/movie_rating_ums"
S3_BUCKET = "ums-datasource"


with DAG(
        dag_id="UMS_Data_Ingestion",
        schedule=None,
        start_date=datetime(2023, 8, 4),
        tags=["data pipeline from local MySQL to AWS"],
        catchup=False,
        ) as dag:

    start_pipeline = EmptyOperator(
            task_id='start_pipeline',
            dag=dag
            )

    export_csv = BashOperator(
            task_id="export_movie_rating_csv",
            bash_command=f" bash {PROJECT_PATH}/scripts/export_to_csv.sh ",
            dag=dag
            )
    
    upload_to_s3_01 = LocalFilesystemToS3Operator(
            task_id="movie_csv_to_s3",
            filename=f"{PROJECT_PATH}/data/movie.csv",
            dest_key="data_airflow/movie.csv",
            dest_bucket=S3_BUCKET,
            replace=True,
            dag=dag
            )
    
    upload_to_s3_02 = LocalFilesystemToS3Operator(
            task_id="rating_csv_to_s3",
            filename=f"{PROJECT_PATH}/data/rating.csv",
            dest_key="data_airflow/rating.csv",
            dest_bucket=S3_BUCKET,
            replace=True,
            dag=dag
            )
    
    submit_glue_job = GlueJobOperator(
            task_id="submit_glue_job",
            job_name="UMS_Movie_Rating",
            aws_conn_id="aws_default",
            concurrent_run_limit=1,
            script_location="s3://ums-datasource/scripts/UMS_movie_rating.py",
            s3_bucket="ums-datasource",
            iam_role_name="AWSGlueServiceRoleDefault",
            create_job_kwargs={"GlueVersion": "4.0", "NumberOfWorkers": 3, "WorkerType": "G.1X", "Timeout" : 15},
            retry_limit=0,

            )
    submit_glue_job.wait_for_completion = False
    
    wait_glue = GlueJobSensor(
            task_id="wait_for_job",
            job_name="UMS_Movie_Rating",
            run_id=submit_glue_job.output,
            verbose=True,
            )
    wait_glue.poke_interval = 5

    finish_pipeline = EmptyOperator(
            task_id='finish_pipeline',
            dag=dag
            )

    chain(start_pipeline, export_csv)
    chain(export_csv, upload_to_s3_01)
    chain(export_csv, upload_to_s3_02)
    chain(upload_to_s3_01, submit_glue_job)
    chain(upload_to_s3_02, submit_glue_job)
    chain(submit_glue_job, wait_glue, finish_pipeline)
    
