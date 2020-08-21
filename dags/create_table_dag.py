from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator


dag = DAG(
    'create_table_dag',
    description = 'Create table for database',
    schedule_interval = None
)

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_table_task = PostgresOperator(
    task_id = "Create_tables",
    dag = dag,
    postgres_conn_id = "redshift",
    sql = "create_tables.sql"
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_table_task >> end_operator