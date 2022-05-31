from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key',)
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION '{}' {} '{}'
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 # redshift_conn_id=your-connection-name
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 s3_param="",
                 region="",
                 data_format="",
                 truncate=False,
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_param = s3_param
        self.region = region
        self.data_format=data_format
        self.truncate = truncate
        #self.data_format=data_format
        #self.copy_json_option = copy_json_option


    def execute(self, context):

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if self.truncate:      
            self.log.info(f'Truncate Redshift table {self.table}')
            redshift.run(f'TRUNCATE {self.table}')
        rendered_key = self.s3_key.format(**context)
        
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
        
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            self.table, s3_path, credentials.access_key,
            credentials.secret_key, self.region, self.data_format, self.s3_param
        )
        # Run query
        self.log.info('Copy data from {s3_path} to Redshift table {self.table}')
        redshift.run(formatted_sql)



