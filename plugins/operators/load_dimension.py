from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from helpers import SqlQueries

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(
            self,
            table: str,
            mode: str,
            redshift_conn_id = "redshift",
            *args, **kwargs) -> None:
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id

        if mode == "append-only":
            mode_stmt = None
        if mode == "delete-load":
            mode_stmt = f"TRUNCATE {table};"

        select_stmt = getattr(SqlQueries(), f"{table}_table_insert")
        self.load_query = f"""
            {mode_stmt}
            INSERT INTO {table} {select_stmt};
        """

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        redshift_hook.run(self.load_query)