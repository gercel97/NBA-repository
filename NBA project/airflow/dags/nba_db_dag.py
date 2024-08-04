from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import pandas as pd

#def for the connection and data grab from the postgres db
def get_nba_data():
    sql_stmt = "SELECT * FROM nba"
    pg_hook=PostgresHook(
            postgres_conn_id='postgres',
            schema='postgres'
    )
    pg_conn=pg_hook.get_conn()
    cursor=pg_conn.cursor()
    cursor.execute(sql_stmt)
    return cursor.fetchall()

#def for data processing

def process_nba_data(ti):
    nba=ti.xcom_pull(task_ids=['get_nba_data'])
    if not nba:
        raise Exception('No data')

    nba=pd.DataFrame(
            data=nba[0],
            columns=['id','full_name','abbreviation','nickname','city','state','year_founded','wins 1981_to_2023','loses_1981_to_2023','current_season_wins','current_season_loses','avg_pts','region','division','championships_won' ]
    )
    nba = nba[
           (nba['current_season_wins'] >20) & (nba['current_season_loses'] < nba['current_season_wins']) & (nba['region'] == "East")


    ]
    nba.to_csv(Variable.get('nba_csv_location'),index=False)

with DAG(

        dag_id='nba_db_dag',
        schedule_interval='@daily',
        start_date=datetime(year=2023,month=6,day=13),
        catchup=False
)as dag:
    #Gets data from the table of NBA
    task_get_nba_data=PythonOperator(
            task_id='get_nba_data',
            python_callable=get_nba_data,
            do_xcom_push=True
    )
    #Processing of NBA data
    task_process_nba_data = PythonOperator(
            task_id='process_nba_data',
            python_callable=process_nba_data
    )
    #truncate table in Postgres
    task_truncate_table=PostgresOperator(
            task_id='trukncate_tgt_table',
            postgres_conn_id='postgres',
            sql="TRUNCATE TABLE nba_next"
    )
    #save to Postgres
    task_load_nba_data=BashOperator(
            task_id='load_nba_data',
            bash_command=(
               'PGPASSWORD=postgres psql -h 65.21.252.21 -d postgres -U postgres -c " '
               'COPY nba_next(id,full_name,abbreviation,nickname,city,state,year_founded,wins_1981_to_2023,loses_1981_to_2023,current_season_wins,current_season_loses,avg_pts,region,division,championships_won)'
               "FROM '/tmp/nba.csv' "
               " DELIMITER ',' "
               'CSV HEADER" '
           )
    )
