from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Adapted from Data Pipelines Exercises 1-4, Udacity Data Engineering Degree
class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}'
        FORMAT AS JSON '{}'
        ;
    """

    @apply_defaults
    def __init__(self,
                 aws_credentials_id="",
                 s3_bucket="",
                 s3_key="",
                 table="",
                 json_path='auto',
                 region='us-west-2',
                 redshift_conn_id='redshift',
                 truncate=True,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)

        self.aws_credentials_id = aws_credentials_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.table = table
        self.json_path = json_path
        self.region = region
        self.redshift_conn_id = redshift_conn_id
        self.truncate = truncate

    def execute(self, context):
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)


        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)

        if self.truncate:
            self.log.info(f"Truncating table: {self.table}")
            redshift_hook.run('TRUNCATE TABLE {}'.format(self.table))
            
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_path
        )


        self.log.info(f"Copying data from S3 path {s3_path}, to Redshift table: {self.table}")
        redshift_hook.run(formatted_sql)

