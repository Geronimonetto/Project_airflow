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
from typing import Any


default_args = {
    'depends_on_past': False,
    'email': ['geronimomorais1617@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=10)
}

dag = DAG('windturbine', description='DAG que retorna os dados da turbina',
          schedule_interval='*/10 * * * *', start_date=datetime(date.today().year, date.today().month, date.today().day),
          catchup=False, default_args=default_args, default_view='graph', doc_md='## DAG de registro de dados da turbina eólica')

group_verify_temp = TaskGroup('group_verify_temp', dag=dag)
group_database_postgre = TaskGroup('group_database_postgre', dag=dag)

file_sensor_task = FileSensor(task_id='file_sensor_task', filepath=Variable.get(
    'path_file'), fs_conn_id='fs_default', poke_interval=10, dag=dag)


def get_archive(**kwargs):
    with open(Variable.get('path_file')) as f:
        data: Any = json.load(f)
        kwargs['ti'].xcom_push(key='idtemp', value=data['idtemp'])
        kwargs['ti'].xcom_push(key='powerfactor', value=data['powerfactor'])
        kwargs['ti'].xcom_push(key='hydraulicpressure',
                               value=data['hydraulicpressure'])
        kwargs['ti'].xcom_push(key='temperature', value=data['temperature'])
        kwargs['ti'].xcom_push(key='timestamp', value=data['timestamp'])
    os.remove(Variable.get('path_file'))


def avalia_temp(**context):
    number = float(context['ti'].xcom_pull(
        task_ids='get_file', key='temperature'))
    if number >= 24:
        return 'group_verify_temp.send_email_alert'
    return 'group_verify_temp.send_email_normal'


get_file = PythonOperator(task_id='get_file',
                          python_callable=get_archive,
                          provide_context=True,
                          dag=dag)

create_table = PostgresOperator(task_id='create_table',
                                postgres_conn_id='postgres_connect',
                                sql='''create table if not exists sensors (idtemp varchar, powerfactor varchar,
                                hydraulicpressure real, temperature varchar, timestamp varchar ''', task_group=group_database_postgre,
                                dag=dag)  # criar conexão com esse nome

insert_data_table = PostgresOperator(task_id='insert_data_table', postgres_conn_id='postgres_connect',
                                     parameters=(
                                         '{{ ti.xcom_pull(task_ids="get_file", key="idtemp")  }}',
                                         '{{ ti.xcom_pull(task_ids="get_file", key="powerfactor")  }}',
                                         '{{ ti.xcom_pull(task_ids="get_file", key="hydraulicpressure")  }}',
                                         '{{ ti.xcom_pull(task_ids="get_file", key="temperature")  }}',
                                         '{{ ti.xcom_pull(task_ids="get_file", key="timestamp")  }}'
                                     ),
                                     sql='''
                                    INSERT INTO sensors (idtemp, powerfactor,hydraulicpressure, temperature, timestamp)
                                    VALUES(%s, %s, %s, %s, %s);
                                    ''',
                                     task_group=group_database_postgre,
                                     dag=dag)

send_email_alert = EmailOperator(task_id='send_email_alert', to='geronimomorais1617@gmail.com',
                                 subject='Airflow alert',
                                 html_content='''
                                <h3> Alerta de temperatura. </h3>
                                <p> DAG: Windturbine</p>
                                ''',
                                 task_group=group_verify_temp,
                                 dag=dag)


send_email_normal = EmailOperator(task_id='send_email_normal', to='geronimomorais1617@gmail.com',
                                  subject='Airflow Advise',
                                  html_content='''
                                <h3> Temperatura OK </h3>
                                <p> DAG: Windturbine</p>
                                ''',
                                  task_group=group_verify_temp,
                                  dag=dag)

check_temp_branch = BranchPythonOperator(task_id='check_temp_branch',
                                         python_callable=avalia_temp,
                                         provide_context=True,
                                         dag=dag,
                                         task_group=group_verify_temp
                                         )

with group_verify_temp:
    check_temp_branch >> [send_email_alert, send_email_normal]

with group_database_postgre:
    create_table >> insert_data_table

file_sensor_task >> get_file
get_file >> group_verify_temp
get_file >> group_database_postgre
