from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.dates import days_ago
from airflow.hooks.postgres_hook import PostgresHook
import requests
from requests import Response
import datetime
import os
import csv
import hashlib
import re
import xml.dom.minidom as minidom
from airflow.operators.bash_operator import BashOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='csv_parsing34',
    schedule_interval='*/30 * * * *',
    catchup = False,
    start_date=days_ago(1)
)


AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')


def insert_file_extension_func():
    ps_pg_hook = PostgresHook(postgres_conn_id="con_ya_postgresql")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()

    insert_data_query = '''INSERT INTO file_extension
	(id_extension, name_extension)
	 VALUES(%s,%s)
	 on conflict(id_extension) do nothing; 
	'''

    with open(AIRFLOW_HOME + "/data/ext_list.csv", encoding="cp1251") as r_file:
        file_reader = csv.reader(r_file)
        for row in file_reader:
          is_ext = re.match(r'[\*\.]\S+', row[0])
          if is_ext != None:
               clean_word = is_ext.group(0).lstrip('*').lstrip('.')
              # print(clean_word)
               add = (hashlib.md5(clean_word.encode('utf-8')).hexdigest(),
                      clean_word)
               cursor.execute(insert_data_query, add)
        conn_ps.commit()


def insert_srv_data_func():
    ps_pg_hook = PostgresHook(postgres_conn_id="con_ya_postgresql")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()
    create_table_query = '''INSERT INTO cvs_srv
	(srv_id, srv_name, srv_town, srv_geotag)
	VALUES(%s, %s, %s, %s)  on conflict(srv_id) do nothing; '''


    with open(AIRFLOW_HOME + "/data/srv_list_0815.csv", encoding="cp1251") as r_file:
        file_reader = csv.reader(r_file, delimiter=";")
        count = 0
        for row in file_reader:
             if count == 0:
                 cursor.execute(create_table_query, row)
             count += 1
        conn_ps.commit()


def insert_listing_data_func():
    ps_pg_hook = PostgresHook(postgres_conn_id="con_ya_postgresql")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()

    insert_data_query = '''INSERT INTO cvs_file_call
	(DateTimeReceived, SenderName, TimeSent, FileName, FolderLevel2, Pathf, Importance, ext)
	 VALUES(%s,%s,%s,%s,%s,%s,%s,%s); 
	---CALL update_id_extension();
	'''

    with open(AIRFLOW_HOME + "/data/data_0812.csv", encoding="cp1251") as r_file:
        file_reader = csv.reader(r_file, delimiter=";")
        count = 0
        for row in file_reader:
             if count != 0:
                 cursor.execute(insert_data_query, row)
             count += 1
        conn_ps.commit()


bash_task1 = BashOperator(
    task_id="bash_task",
        bash_command="bash cp /etc/airflow/data/data_s.csv /etc/airflow/data/data3435454656.txt'"
    bash_command="bash cp /home/external_data/ext_data/*.* /etc/airflow/data/ext_data/ext.csv'"
    bash_command="bash cp /home/external_data/srv_data/*.* /etc/airflow/main_data/data/srv.csv'"
    bash_command="bash cp /home/external_data/main_data/*.* /etc/airflow/data/main_data/main.csv'"
    bash_command="bash rm /home/external_data/srv_data/*.* -fR'"
    bash_command="bash rm /home/external_data/main_data/*.* -fR'"
    bash_command="bash rm /home/external_data/ext_data/*.* -fR'",
    dag=dag
)


create_table_file_extension = PostgresOperator(
    task_id='create_table_file_extension',
    postgres_conn_id="con_ya_postgresql",
    sql=''' DROP TABLE IF EXISTS file_extension; 
	    CREATE TABLE IF NOT EXISTS file_extension(
            id_extension varchar, 
	    name_extension varchar,
	    date_load timestamp NOT NULL DEFAULT NOW()::timestamp,
	    CONSTRAINT file_extension_PK PRIMARY KEY (id_extension) 
		)''', 
    dag =dag
)
create_table_cvs_srv = PostgresOperator(
    task_id='create_table_cvs_srv',
    postgres_conn_id="con_ya_postgresql",
    sql='''CREATE TABLE IF NOT EXISTS cvs_srv(
            date_load timestamp NOT NULL DEFAULT NOW()::timestamp,
            srv_id varchar,
            srv_name  varchar,
            srv_town varchar,
            srv_geotag varchar,
            CONSTRAINT cvs_srv PRIMARY KEY (srv_id)                  
        );''', dag=dag
)

create_table_cvs_file_call = PostgresOperator(
    task_id='create_table_cvs_file_call',
    postgres_conn_id="con_ya_postgresql",
    sql=''' --DROP TABLE IF EXISTS cvs_file_call; 
	    CREATE TABLE IF NOT EXISTS cvs_file_call(
            date_load timestamp NOT NULL DEFAULT NOW()::timestamp,
            DateTimeReceived varchar, 
	    SenderName varchar,
            TimeSent varchar, 
            FileName  varchar,
            FolderLevel2 varchar,
            Pathf varchar,
            Importance varchar,
	    id_extension varchar DEFAULT 'None',
            ext  varchar);
	    DELETE FROM cvs_file_call
''', 
    dag =dag
)
insert_file_ext_data =  PythonOperator( 
	task_id='insert_file_ext_data', 
	python_callable=insert_file_extension_func, 
	provide_context=True, 
	dag=dag
)
insert_file_call_data =  PythonOperator( 
	task_id='insert_file_call_data', 
	python_callable=insert_listing_data_func, 
	provide_context=True, 
	dag=dag
)
insert_srv_data =  PythonOperator( 
	task_id='insert_srv_data', 
	python_callable=insert_srv_data_func, 
	provide_context=True, 
	dag=dag
)



bash_task1 >> create_table_file_extension >> create_table_cvs_file_call >> insert_file_ext_data >> insert_file_call_data
bash_task1 >> create_table_cvs_srv >> insert_srv_data
