from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_id='',
                 sql='',
                 mode='',
                 table_name='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_id=redshift_id
        self.sql=sql
        self.mode=mode
        self.table_name=table_name

    def execute(self, context):
        self.log.info("Setting.")
        redshift=PostgresHook(postgres_conn_id=self.redshift_id)
        
        self.log.info('Importing.')
        if self.mode == 'insert':
            self.log.info('inserting mode.')
            insert_sql="INSERT INTO {tablename} ".format(tablename=self.table_name) + self.sql
            self.log.info(insert_sql)
            redshift.run(insert_sql)
        else:
            self.log.info('Importing mode. Deleting existed data.')
            redshift.run("DELETE FROM {tablename}".format(tablename=self.table_name))
            self.log.info('Importing data.')
            insert_sql="INSERT INTO {tablename} ".format(tablename=self.table_name) + self.sql
            redshift.run(insert_sql)
