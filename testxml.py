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
import pandas as pd
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

dag = DAG(
    dag_id='vulnerability',
    schedule_interval='*/30 * * * *',
    #0 18 * */3 *
    catchup = False,
    start_date=days_ago(1)
)


AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')


def insert_vulnerability_func():
    ps_pg_hook = PostgresHook(postgres_conn_id="con_ya_postgresql")
    conn_ps = ps_pg_hook.get_conn()
    cursor = conn_ps.cursor()

    insert_data_query = '''INSERT INTO vulnerability
	(svr_name, o_system, program, vuln_id, CVE, vuln_number, vuln_level, sec_bulletin)
	 VALUES(%s,%s,%s,%s,%s,%s,%s,%s)
	 --on conflict(id_extension) do nothing; 
	'''

    context = minidom.parse(AIRFLOW_HOME + "/data/vlnb.xml")
    elements = context.getElementsByTagName('Row')
    count = 0
    for elem in elements:
        node = elem.getElementsByTagName('Data')
        add = list()
        if count > 1:
            for v_data in node[0:8]:
                add.append(v_data.firstChild.data)
            cursor.execute(insert_data_query, add)
        count += 1
    conn_ps.commit()


create_table_vulnerability = PostgresOperator(
    task_id='create_table_vulnerability',
    postgres_conn_id="con_ya_postgresql",
    sql=''' DROP TABLE IF EXISTS vulnerability; 
	    CREATE TABLE IF NOT EXISTS vulnerability(
            svr_name varchar, 
	    o_system varchar,
	    program varchar,
	    vuln_id varchar,
	    CVE varchar,
	    vuln_number varchar,
	    vuln_level varchar,
	    sec_bulletin  varchar,
	    date_load timestamp NOT NULL DEFAULT NOW()::timestamp
	    --CONSTRAINT file_extension_PK PRIMARY KEY (id_extension) 
		)''', 
    dag =dag
)
insert_vulnerability_data =  PythonOperator( 
	task_id='insert_vulnerability_data', 
	python_callable=insert_vulnerability_func, 
	provide_context=True, 
	dag=dag
)


create_table_vulnerability >> insert_vulnerability_data
