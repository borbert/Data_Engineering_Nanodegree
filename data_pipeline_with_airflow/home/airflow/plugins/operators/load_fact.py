from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_id='',
                 sql='',
                 mode='',
                 table='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_id=redshift_id
        self.sql=sql
        self.mode=mode
        self.table=table

    def execute(self, context):
        self.log.info('Setting.')
        redshift=PostgresHook(postgres_conn_id=self.redshift_id)
        
        self.log.info('Loading table.')
        if self.mode == 'insert':
            redshift.run("INSERT INTO {}".format(self.table) + self.sql)
        else:
            redshift.run("DELETE FROM {}".format(self.table))
            redshift.run("INSERT INTO {}".format(self.table) + self.sql)