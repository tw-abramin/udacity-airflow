from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    insert_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "redshift",
                 table="",
                 sql="",
                 load_type='append',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.sql = sql
        self.load_type = load_type

    def execute(self, context):
        redshift_hook = PostgresHook(self.redshift_conn_id)
        formatted_sql = LoadDimensionOperator.insert_sql.format(self.table, self.sql)
        if (self.load_type is 'delete-load'):
            self.log.info(f"Truncating table: {self.table}")
            redshift_hook.run('TRUNCATE {}'.format(self.table))
            
        self.log.info(f"Loading Dimension Table: {formatted_sql}")
        redshift_hook.run(formatted_sql)