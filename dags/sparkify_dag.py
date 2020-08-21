from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.helpers.sql_queries import SqlQueries

default_args = {
    "owner": "udacity",
    "depends_on_past": False,
    "start_date": datetime(2018, 11, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
    "catchup": False
}

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *' # @hourly
)


start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id = "Stage_events",
    dag = dag,             
    s3_bucket = "udacity-dend/",
    s3_key = "log_data/",
    table = "staging_events", 
    json = "log_json_path.json"
)


stage_songs_to_redshift = StageToRedshiftOperator(
    task_id = "Stage_songs",
    dag = dag,
    s3_bucket = "udacity-dend/",
    s3_key = "song_data/",
    table = "staging_songs"
)

load_songplays_table = LoadFactOperator(
    task_id = "Load_songplays_fact_table",
    dag = dag,
    table = "songplays"
)

load_user_dimension_table = LoadDimensionOperator(
    task_id = "Load_user_dim_table",
    dag = dag,
    table = "users",
    mode = "delete-load"
)

load_song_dimension_table = LoadDimensionOperator(
    task_id = "Load_song_dim_table",
    dag = dag,
    table = "songs",
    mode = "delete-load"
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id = "Load_artist_dim_table",
    dag = dag,
    table = "artists",
    mode = "delete-load"
)

load_time_dimension_table = LoadDimensionOperator(
    task_id = "Load_time_dim_table",
    dag = dag,
    table = "time",
    mode = "delete-load"
)

run_quality_checks = DataQualityOperator(
    task_id = "Run_data_quality_checks",
    dag = dag,
    tables = ["songplays", "users", "songs", "artists", "time"]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

staging_tasks =  [stage_events_to_redshift, stage_songs_to_redshift]

dimension_table_loading_tasks = [load_song_dimension_table,
                                 load_artist_dimension_table,
                                 load_time_dimension_table,
                                 load_user_dimension_table]

start_operator >> staging_tasks \
               >> load_songplays_table \
               >> dimension_table_loading_tasks \
               >> run_quality_checks \
               >> end_operator

