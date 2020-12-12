from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    
    import_s3_song="""COPY {table_name} FROM '{s3_link}'
                 ACCESS_KEY_ID '{key_id}'
                 SECRET_ACCESS_KEY '{secret_id}'
                 REGION 'us-west-2'
                 TIMEFORMAT AS 'epochmillisecs'
                 FORMAT AS JSON 'auto' """
    
    import_s3_event="""COPY {table_name} FROM '{s3_link}'
                       ACCESS_KEY_ID '{key_id}'
                       SECRET_ACCESS_KEY '{secret_id}'
                       TIMEFORMAT AS 'epochmillisecs'
                       TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
                       REGION 'us-west-2'
                       FORMAT AS JSON 's3://udacity-dend/log_json_path.json'"""

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 table_name='',
                 s3_bucket='',
                 s3_key='',
                 redshift_id='',
                 credentials='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.table_name=table_name
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.redshift_id=redshift_id
        self.credentials=credentials

    def execute(self, context):
        
        aws_hook=AwsHook(self.credentials)
        credentials=aws_hook.get_credentials()
        redshift=PostgresHook(postgres_conn_id=self.redshift_id)
        
        self.log.info("Clearing & Restoring.")
        redshift.run("DELETE FROM {}".format(self.table_name))
        
        rendered_key=self.s3_key.format(**context)
        s3_path='s3://{}/{}'.format(self.s3_bucket, rendered_key)

        self.log.info("Importing.")
        if self.table_name == 'staging_events':
            formatted_sql=StageToRedshiftOperator.import_s3_event.format(
                                        table_name=self.table_name,
                                        s3_link=s3_path,
                                        key_id=credentials.access_key, 
                                        secret_id=credentials.secret_key)
            redshift.run(formatted_sql)
            self.log.info("Importing log data complete")
        elif self.table_name == 'staging_songs':
            formatted_sql=StageToRedshiftOperator.import_s3_song.format(
                                        table_name=self.table_name,
                                        s3_link=s3_path,
                                        key_id=credentials.access_key, 
                                        secret_id=credentials.secret_key)
            redshift.run(formatted_sql)
            self.log.info("Importing song data complete")
        else:
            self.log.info('Unknown s3 link - {}'.format(s3_link))






