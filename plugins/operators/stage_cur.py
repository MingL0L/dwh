from airflow.hooks.postgres_hook import PostgresHook
from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
import pandas as pd
import requests
import io
from s3fs.core import S3FileSystem
from io import StringIO
import boto3

class StageCurOperator(BaseOperator):
    ui_color = '#358140'
    stage_sql_template="""
    TRUNCATE {destination_table};
    COPY {destination_table} 
    FROM '{file_path}'
    ACCESS_KEY_ID '{aws_key}'
    SECRET_ACCESS_KEY '{aws_secret}'
    CSV
    IGNOREHEADER 1
    REGION '{region}';
    """
    
    @apply_defaults
    def __init__(self,
                 conn_id=None,
                 aws_credentials_id=None,
                 src_file_path=None,
                 destination_table=None,
                 region='us-west-2',
                 *args, **kwargs):

        super(StageCurOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.conn_id = conn_id
        self.aws_credentials_id = aws_credentials_id
        self.src_file_path = src_file_path
        self.destination_table = destination_table
        self.region = region
        
    def execute(self, context):
        self.log.info("StageCurOperator CSV from {}".format(self.src_file_path))
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        s3 = S3FileSystem(anon=False, key=credentials.access_key, secret=credentials.secret_key)
        # Load CSV
        s = requests.get(self.src_file_path).content
        df = pd.read_csv(io.StringIO(s.decode('utf-8')), sep=';', error_bad_lines=False)
        df['date'] = pd.to_datetime(df['Titre :'], format='%d/%m/%Y',errors='coerce')
        # Extract cur_code
        df_cur = df.iloc[:2].T.reset_index().iloc[1:-1].rename(columns={0:"Serial_code", 1:"cur_code"})
        df_cur['cur_code'] = df_cur['cur_code'].str.extract(r'^.*\((.*)\)$')
        # Get all currency values
        df_cur_all = df[df.date.notnull()].drop('Titre :', axis=1).melt(id_vars=['date'])
        # Remove NA
        df_cur_all = df_cur_all.dropna()
        # Remove '-'
        df_cur_all = df_cur_all[df_cur_all['value']!='-']
        df_cur_all.value = df_cur_all.value.str.replace(',', '.').astype(float)
        # Get all currency values
        df_cur_all = df_cur_all.rename(columns={'variable':'index'})\
                            .join(df_cur.set_index('index'), on='index')\
                            .drop(columns=['index'])
        
        s3_dest_df_path = "s3://{}/{}.csv".format('dwh-cur', 'cur')
        with s3.open(s3_dest_df_path, mode='wb') as s3_dest_file:
            self.log.info("Started writing {}".format(df_cur_all.shape))
            #s3_dest_file.write(df_cur_all.to_csv(None, index=False).encode())
            s3_dest_file.write(df_cur_all.to_csv(None, index=False).encode())
            self.log.info("Completed writing {}".format(df_cur_all.shape))
        
        redshift=PostgresHook(postgres_conn_id=self.conn_id)        
        stage_sql = StageCurOperator.stage_sql_template.format(
            destination_table=self.destination_table,
            file_path=s3_dest_df_path,
            aws_key=credentials.access_key,
            aws_secret=credentials.secret_key,
            region=self.region
        )
        redshift.run(stage_sql)
        