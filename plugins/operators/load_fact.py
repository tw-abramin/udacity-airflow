from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    insert_sql = """
        INSERT INTO {}
        {};
    """
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 table="",
                 sql="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        self.log.info(f"Truncating table: {self.table}")
        redshift_hook.run('TRUNCATE {}'.format(self.table))


        formatted_sql = LoadFactOperator.insert_sql.format(self.table, self.sql)
        self.log.info(f"Loading Fact Table: {formatted_sql}")
        redshift_hook.run(formatted_sql)