from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(
            self,
            table:str, 
            redshift_conn_id = "redshift",
            *args, **kwargs) -> None:

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

        select_stmt = SqlQueries.songplays_table_insert
        self.load_query = f"""
        insert into {table} (playid, start_time, userid, "level", songid,
                    artistid, sessionid, location, user_agent)
        {select_stmt};
        """

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift_hook.run(self.load_query)
