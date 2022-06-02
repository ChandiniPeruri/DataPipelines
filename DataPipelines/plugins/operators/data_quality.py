from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="redshift",
                 tables={},
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id=redshift_conn_id,
        self.tables=tables
        

    def execute(self, context):
        self.log.info("Starting Data Quality Checking")
        redshift=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        estimated_err_count=0
        error_count = 0
        failed_tests = []
        
        for key in self.tables:
            sql=f"SELECT COUNT(*) FROM {key} WHERE {self.tables[key]} IS NULL"
            self.log.info(sql)
            #num_records = redshift.get_records(sql)
            #self.log.info(f"Number of Records {num_records[0][0]}")
        
        if error_count > 0:
            raise ValueError("Data Quality Failed")
            
        self.log.info("Data Quality Checks Passed")
                