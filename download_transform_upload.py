from airflow.decorators import dag
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from docker.types import Mount
import uuid

link1 = 'https://drive.google.com/file/d/1YK_SOKFXhLaWdgdQglLxEoAsOMCA7M4x/view'
link2 = 'https://drive.google.com/file/d/1iW0GBTox3BMdn_kRiH88LIIj_OCp-3zI/view'

dockerops_kwargs = {
    "mount_tmp_dir": False,
    "mounts": [
        Mount(
            source="/home/gp/airflow_final/data",  # Change to your path
            target="/opt/airflow/data/",
            type="bind",
        )
    ],
    "retries": 1,
    "api_version": "auto",
    "docker_url": "tcp://docker-socket-proxy:2375",
    "network_mode": "bridge",
}


@dag("database_creation", start_date=days_ago(0), schedule="@yearly", catchup=False)
def taskflow():
    # Task 1
    check_file = SimpleHttpOperator(
        task_id='check_file',
        method='HEAD',
        endpoint='1YK_SOKFXhLaWdgdQglLxEoAsOMCA7M4x/view?usp=drive_link',
        http_conn_id='ga_sessions_id',
        log_response=True
    )

    check_file2 = SimpleHttpOperator(
        task_id='check_file2',
        method='HEAD',
        endpoint='1iW0GBTox3BMdn_kRiH88LIIj_OCp-3zI/view?usp=drive_link',
        http_conn_id='ga_sessions_id',
        log_response=True
    )

    # Task 2
    unique_container_name = f"data_etl_{str(uuid.uuid4())[:8]}"

    data_extract_transform_load = DockerOperator(
        task_id="extract_transform_load",
        container_name=unique_container_name,
        image="data-etl:latest",
        command=f"python3 data_etl.py {link1} {link2}",
        **dockerops_kwargs,
    )

    check_file >> check_file2 >> data_extract_transform_load


taskflow()
