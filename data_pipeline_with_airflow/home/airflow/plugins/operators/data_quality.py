from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_id='',
                 table_name='',
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_id=redshift_id
        self.table_name=table_name

    def execute(self, context):
        redshift=PostgresHook(self.redshift_id)
        sql="SELECT COUNT(*) FROM {tablename}".format(tablename=self.table_name)
        records=redshift.get_records(sql)
        self.log.info('Testing.')
        if len(records) < 1 or len(records[0]) < 1:
            raise ValueError(f"Data quality check failed. {self.table_name} has no values")
        num_records = records[0][0]
        if num_records < 1:
            raise ValueError(f"Data quality check failed. {self.table_name} contained 0 rows")
        self.log.info(f"Data quality on table {self.table_name} check passed with {records[0][0]} records")