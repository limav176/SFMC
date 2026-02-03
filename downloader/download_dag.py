import os
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Param
from airflow.operators.python import PythonOperator
from pandas.io.formats.style import jinja2

from airflow_dag_level_permissions import AIRFLOW_GENERIC_DAG_LEVEL_PERMISSIONS
from salesforce_ingestion.downloader.download_ondemand import FTPFileDownloader

LOCAL_TZ = pendulum.timezone(os.getenv("BRAZIL_TIMEZONE"))

DEFAULT_ARGS = {
    "owner": "Engineering",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1, tzinfo=LOCAL_TZ),
    "email": ["data-engineering@meupag.com.br"],
    "email_on_failure": False,
    "email_on_retry": False,
}


def download_files(sftp_path, sftp_file, bucket_date=0):
    FTPFileDownloader().download(sftp_path, sftp_file, bucket_date)


with DAG(
    dag_id="salesforce_downloader",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(minutes=180),
    schedule_interval=None,
    catchup=False,
    template_undefined=jinja2.StrictUndefined,
    tags=['salesforce', 'csv'],
    params={
        "sftp_path": Param(type="string"),
        "sftp_file": Param(type="string"),
        "bucket_date": Param(type="string"),
    },
    access_control=AIRFLOW_GENERIC_DAG_LEVEL_PERMISSIONS,
) as dag:

    download_files = PythonOperator(
        task_id="download_files",
        python_callable=download_files,
        provide_context=True,
        op_kwargs={"sftp_path": "{{ params.sftp_path }}", "sftp_file": "{{ params.sftp_file }}", "bucket_date":"{{ params.bucket_date }}"}
    )

