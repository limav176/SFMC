import os
import pendulum
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from pandas.io.formats.style import jinja2

from airflow_dag_level_permissions import AIRFLOW_GENERIC_DAG_LEVEL_PERMISSIONS
from common.local.framework_spark_r2p.v1.persistent_emr_cluster import PersistentEMRCluster
from common.local.framework_spark_r2p.v1.framework_spark_r2p import FrameworkSparkR2P

from common.sensors.custom_time_delta_sensor import CustomTimeDeltaSensor

from airflow.providers.amazon.aws.operators.emr import EmrAddStepsOperator
from airflow.providers.amazon.aws.sensors.emr import EmrStepSensor
from airflow.providers.amazon.aws.hooks.emr import EmrHook

import glob

from salesforce_ingestion.src.ftp_file_downloader_operator import FTPFileDownloaderOperator
from salesforce_ingestion.src.ftp_file_exists_sensor import FTPFileExistsSensor

LOCAL_TZ = pendulum.timezone(os.getenv("BRAZIL_TIMEZONE"))

DEFAULT_ARGS = {
    "owner": "Engineering",
    "depends_on_past": False,
    "start_date": datetime(2021, 1, 1, tzinfo=LOCAL_TZ),
    "email": ["data-engineering@meupag.com.br"],
    "email_on_failure": False,
    "email_on_retry": False,
}


def get_config_files():
    config_files = glob.glob(f"{os.path.dirname(os.path.abspath(__file__))}/*_download.yaml")

    return {
        config_file.split("/")[-1].removesuffix("_download.yaml"): config_file
        for config_file
        in config_files
    }


with DAG(
    dag_id="salesforce_pipeline",
    default_args=DEFAULT_ARGS,
    schedule_interval="30 0 * * *",
    catchup=False,
    template_undefined=jinja2.StrictUndefined,
    max_active_runs=1,
    tags=['salesforce', 'csv'],
    access_control=AIRFLOW_GENERIC_DAG_LEVEL_PERMISSIONS,
):
    download_files = []

    for config, config_file in get_config_files().items():
        file_sensor = FTPFileExistsSensor(
            task_id=f"file_sensor_{config}",
            config_file=config_file,
            poke_interval=300,  # 5 minutes in seconds
            timeout=21600,  # 6 hours in seconds
            mode='reschedule'
        )

        file_downloader = FTPFileDownloaderOperator(
            task_id=f"download_files_{config}",
            config_file=config_file,
            bucket='ftp-will-salesforce-prod',
            trigger_rule=TriggerRule.ALL_DONE  # Trigger even if the sensor fails
        )

        file_sensor >> file_downloader
        download_files.append(
            file_downloader
        )

    emr_hook = EmrHook(aws_conn_id='aws_default')
    circle = 'martech'
    circle_cluster_name = f'raw_to_processed__{circle}_circle'
    on_states = ["BOOTSTRAPPING", "RUNNING", "STARTING", "WAITING"]

    emr_tags = {"Circle": "Marketing", "CircleId": "62a78eeddf76475b626af9ca"}
    emr_cluster_r2p = PersistentEMRCluster(env=os.getenv("ENVIRONMENT") or "dev", cluster_name=circle_cluster_name, emr_tags=emr_tags)

    start_r2p_emr_cluster = emr_cluster_r2p.get_start_emr_cluster_task_group()

    run_for_n_minutes = CustomTimeDeltaSensor(
        task_id="run_for_5_minutes",
        delta=timedelta(minutes=5),
        mode="reschedule",
        start_date="{{ dag_run.start_date }}",
        poke_interval=300
    )

    terminate_cluster_sensor = emr_cluster_r2p.get_idle_or_terminate_task(max_idle_time=600,
                                                                          timeout=18000)
    cluster_terminator = emr_cluster_r2p.get_emr_terminate_task()

    wait_cluster_tg = FrameworkSparkR2P.get_cluster_tasks(cluster_name=circle_cluster_name)

    spark_steps_2 = [{
        "Name": "filesort_into_raw_zip",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": [
                "/usr/bin/spark-submit",
                "--deploy-mode", "client",
                "--master", "yarn",
                "--name", "filesort_into_raw_zip",
                "s3://data-app-zone-will-prod/airflow-dags/salesforce_ingestion/src/main.py",
                "data-raw-zone-will-prod",
                "ftp-will-salesforce-prod"
            ]
        }
    }]

    filesort_into_raw = EmrAddStepsOperator(
        task_id="filesort_into_raw",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='wait_cluster.wait_cluster_r2p', key='return_value')}}",
        aws_conn_id="aws_default",
        steps=spark_steps_2,
        retries=4,
        pool="emr_pool",
        priority_weight=5
    )

    watch_step_filesort_into_raw = EmrStepSensor(
        task_id="watch_step_filesort_into_raw",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='wait_cluster.wait_cluster_r2p', key='return_value')}}",
        step_id="{{ " + f"task_instance.xcom_pull(task_ids='filesort_into_raw', key='return_value')[0]" + " }}",
        aws_conn_id="aws_default",
        retries=6,
        pool="emr_pool"
    )

    # Tasks do R2P geradas pelo config
    r2p_tasks = FrameworkSparkR2P.get_add_step_tasks(path_dag_dir=os.path.dirname(__file__))

    download_files >> start_r2p_emr_cluster >> run_for_n_minutes >> terminate_cluster_sensor >> cluster_terminator
    download_files >> wait_cluster_tg >> filesort_into_raw >> watch_step_filesort_into_raw >> r2p_tasks
