from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.sensors.filesystem import FileSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta, date
import json
import os


default_args: dict = {
    'depends_on_past': False,
    'email': ['geronimomorais1617@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG('windturbine', description='DAG que retorna os dados da turbina',
          schedule_interval=None, start_date=datetime(date.today().year, date.today().month, date.today().day),
          catchup=False, default_args=default_args, default_view='graph', doc_md='## DAG de registro de dados da turbina eólica')

group_verify_temp = TaskGroup('group_verify_temp', dag=dag)
group_database_postgre = TaskGroup('group_database_postgre', dag=dag)

file_sensor_task = FileSensor(task_id='verified_file', filepath=Variable.get(
    'path_file'), fs_conn_id='fs_default', poke_interval=10, dag=dag)


def get_archive(**kwargs) -> None:
    with open(Variable.get('path_file')) as f:
        data = json.load(f)
        kwargs['ti'].xcom_push(key='idtemp', value=data['idtemp'])
        kwargs['ti'].xcom_push(key='powerfactor', value=data['powerfactor'])
        kwargs['ti'].xcom_push(key='hydraulicpressure',
                               value=data['hydraulicpressure'])
        kwargs['ti'].xcom_push(key='temperature', value=data['temperature'])
        kwargs['ti'].xcom_push(key='timestamp', value=data['timestamp'])
    os.remove(Variable.get('path_file'))


get_file = PythonOperator(task_id='process_file',
                          python_callable='get_archive',
                          provide_contex=True,
                          dag=dag)

create_table = PostgresOperator(task_id='create_table',
                                postgres_conn_id='postgres',
                                sql='''create table if not exists sensors (idtemp varchar, powerfactor varchar,
                                hydraulicpressure real, temperature varchar, timestamp varchar ''', task_group=group_database_postgre,
                                dag=dag)  # criar conexão com esse nome

