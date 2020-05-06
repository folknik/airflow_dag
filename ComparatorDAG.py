import json
from airflow import DAG
from datetime import datetime, timedelta
from airflow.contrib.operators.ssh_operator import SSHOperator

default_args = {
        "owner": "airflow",
        "start_date": datetime(2020, 4, 20),
        "email": ["airflow@airflow.com"],
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 1,
        "retry_delay": timedelta(minutes=2)
}

with open("/usr/local/airflow/credentials/comparator.json", "r+") as f:
    credentials = json.loads(f.read())

EXEC_DATE = '{{ ds }}'

dag = DAG('TableComparatorDag',
          default_args=default_args,
          schedule_interval='@daily',
          max_active_runs=1,
          concurrency=3)

task = SSHOperator(task_id='TableComparatorTask',
                   ssh_conn_id="LocalConnection",
                   dag=dag,
                   command=f"docker run --rm --network=host \
                            -e 'mssql_server={mssql_server}' \
                            -e 'mssql_database={mssql_database}' \
                            -e 'mssql_user={mssql_user}' \
                            -e 'mssql_pw={mssql_pw}' \
                            -e 'vertica_server={vertica_server}' \
                            -e 'vertica_database={vertica_database}' \
                            -e 'vertica_user={vertica_user}' \
                            -e 'vertica_pw={vertica_pw}' \
                            -e 'EXEC_DATE={EXEC_DATE}' \
                            your_docker_image")