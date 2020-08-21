import logging
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(
            self,
            tables: list,  
            redshift_conn_id = "redshift",
            *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables

    def execute(self, context):
        for table in self.tables:
            self.check_greater_than_zero(table)
            self.check_no_null_values(table)

    
    def check_greater_than_zero(self, table):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift_hook.get_records(f"select count(*) from {table}")
        
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {table} returned no results")
        
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {table} contained 0 rows")

        logging.info(f"**Table {table} passed data quality check: not an empty table.")
        
    
    def check_no_null_values(self, table):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        query_result = redshift_hook.get_records(f"""
            select column_name
            from information_schema.columns   
            where table_name = '{table}'
            and is_nullable = 'NO'
        """)
        non_nullable_columns = [i[0] for i in query_result]

        logging.info(f"Table {table} has non_nullable columns: {non_nullable_columns}")

        null_records = {}
        for column in non_nullable_columns:
            query_result = redshift_hook.get_records(f"select count(*) - count({column}) from {table}")
            num_nulls = query_result[0][0]
            if num_nulls > 0:
                null_records[column] = num_nulls
        
        if len(null_records) > 0:
            msg = f"Data quality check failed. {table} contains null values.\n"
            for col, count in null_records.items():
                msg += f"{col}: {count}\n"
            raise ValueError(msg)

        logging.info(f"**Table {table} passed data quality check: no null.")