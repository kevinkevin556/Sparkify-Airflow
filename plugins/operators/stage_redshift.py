from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ['s3_key']

    stage_query = ("""
        COPY {table_name}
        FROM '{data_source}'
        {authorization}
        {format_options};
    """)
    
    # Authorization
    iam = "IAM_ROLE {arn}"
    access_keys = "ACCESS_KEY_ID '{}' \n" + \
                  "SECRET_ACCESS_KEY '{}' "
    
    # format_options
    ignore_header = "IGNOREHEADER {ignore_lines}"
    delimiter = "DELIMITER '{delimiter_char}'"
    json = "JSON '{json_option}'"

    @apply_defaults
    def __init__(
            self, 
            s3_bucket: str,
            s3_key: str, 
            table: str,
            json = "auto",
            delimiter = ",",
            ignore_header = 1,
            aws_credentials_id = "aws_credentials",
            redshift_conn_id = "redshift",
            *args, **kwargs) -> None:

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # AWS settings
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        
        # SQL query related
        self.table = table
        self.json_option = json if json in ["auto",None] else f"s3://{s3_bucket}{json}" 
        self.delimiter_char = delimiter
        self.ignore_lines = ignore_header

    def execute(self, context):
        # set aws
        aws_hook = AwsHook(self.aws_credentials_id)
        cred = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        s3_path = "s3://{}".format(self.s3_bucket) + self.s3_key

        # copy table
        if self.json_option:
            stage_query = self.stage_query.format(
                table_name = self.table,
                data_source = s3_path,
                authorization = self.access_keys.format(cred.access_key, cred.secret_key),
                format_options = self.json.format(json_option=self.json_option)
            )
        else:
            stage_query = self.stage_query.format(
                table_name = self.table,
                data_source = s3_path,
                authorization = self.access_keys.format(cred.access_key, cred.secret_key),
                format_options = self.delimiter.format(delimiter_char=self.delimiter_char) + \
                                "\n" + self.ignore_header.format(ignore_lines=self.ignore_lines)
            )
        redshift_hook.run(stage_query)