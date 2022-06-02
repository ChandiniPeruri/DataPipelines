from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 sql="",
                 table="",
                 truncate="True",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.table = table
        self.truncate = truncate
        
        
    def execute(self, context):
        self.log.info("Loading data into dimension table {}".format(self.table))
        postgres_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.truncate:
            self.log.info("Truncate table" + self.table)
            postgres_hook.run("TRUNCATE TABLE {}".format(self.table))
        self.log.info(f"Load Dimension table {self.table}")
        postgres_hook.run("INSERT INTO {} {}".format(self.table, self.sql))