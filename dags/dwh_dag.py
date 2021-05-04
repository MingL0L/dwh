from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator

from operators.stage_cur import StageCurOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator

from helpers import SqlQueries


#AWS_KEY = os.environ.get('AWS_KEY')
#AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'Ming',
    'start_date': datetime(2021, 4, 1),
}

dag = DAG('dwh',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          start_date=datetime(2021,4,1,0,0,0,0),
          #end_date=datetime(2018,12,1,0,0,0,0),
          schedule_interval='@daily',
          max_active_runs=1
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql="dwh_create_tables.sql"
)

stage_currency_to_redshift = StageCurOperator(
    task_id='Stage_currency',
    dag=dag,
    conn_id="redshift",
    aws_credentials_id = 'aws_credentials',
    src_file_path="https://raw.githubusercontent.com/MingL0L/dwh/main/Webstat_Export_20210504.csv",
    destination_table="stage_cur",
    provide_context=True
)

dim_currency_to_redshift = PostgresOperator(
    task_id='dim_currency',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
        TRUNCATE dim_currency;
        INSERT INTO dim_currency SELECT * 
        FROM 
	        (SELECT S.cur_code, S.value AS one_euro_value, last_updated_date, serial_code
		      FROM (SELECT cur_code, MAX(date) AS last_updated_date
 	  		       FROM stage_cur
 	  	          GROUP BY cur_code) AS L
		          JOIN stage_cur S
			         ON S.cur_code = L.cur_code AND S.date = L.last_updated_date);
    """
)

fact_exc_to_redshift = PostgresOperator(
    task_id='fact_exchange_rate',
    dag=dag,
    postgres_conn_id="redshift",
    sql="""
        INSERT INTO fact_exchange_rate_history SELECT history_date,from_cur_code,to_cur_code,exchange_rate
        FROM 
	        (SELECT
		        date as history_date, 
		        value,
    	       cur_code as from_cur_code,
		        LAG(from_cur_code, 1) OVER (
                ORDER BY from_cur_code
                ) to_cur_code,
    	       LAG(value, 1) OVER (
                ORDER BY from_cur_code
                ) to_cur_code_val,
    	       (to_cur_code_val/value) AS exchange_rate 
	         FROM
		        stage_cur
            WHERE date >'{{ prev_ds }}'AND date<'{{ next_ds }}'
            )
         WHERE exchange_rate is not null;
    """
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> create_tables
create_tables >> stage_currency_to_redshift
stage_currency_to_redshift >> dim_currency_to_redshift
dim_currency_to_redshift >> fact_exc_to_redshift
fact_exc_to_redshift >> end_operator