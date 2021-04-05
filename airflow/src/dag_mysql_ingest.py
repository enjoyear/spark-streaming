from datetime import timedelta

from airflow import DAG
from airflow.operators.mysql_operator import MySqlOperator
from airflow.utils.dates import days_ago

from fun.hello_db_operator import HelloDBOperator

"""
# List all connections
airflow connections list

# Add a connection
airflow connections --add --conn_id 'my_prod_db' --conn_uri 'my-conn-type://login:password@host:port/schema?param1=val1&param2=val2'

# Trigger this dag
airflow dags trigger hello --exec-date 2021-03-29
"""

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(30),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}

dag = DAG('mysql_ingest', default_args=default_args, schedule_interval=None)

mysql_task = MySqlOperator(task_id='mysql_ingest',
                           mysql_conn_id='mysql_test',
                           sql='select * from authors',
                           database='mysql',
                           dag=dag)

hello_task = HelloDBOperator(task_id='sample-task',
                             name='foo_bar',
                             mysql_conn_id='mysql_test',
                             database='mysql',
                             dag=dag)
