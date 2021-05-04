import logging

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.models import TaskInstance
from airflow.operators.python_operator import PythonOperator

from itertools import groupby
from operator import itemgetter
import datetime as dt
import os
from operators.bq import BigQueryToXOperator

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryCreateExternalTableOperator,
    BigQueryDeleteDatasetOperator,
    BigQueryDeleteTableOperator,
    BigQueryGetDatasetOperator,
    BigQueryGetDatasetTablesOperator,
    BigQueryPatchDatasetOperator,
    BigQueryUpdateDatasetOperator,
    BigQueryUpdateTableOperator,
    BigQueryUpsertTableOperator,
    BigQueryGetDataOperator
)
from airflow.utils.dates import days_ago

from sta.sta_client import STAMQTTClient, make_st_time, STAClient

DATASET_NAME = 'nmbgmr_sites'
TABLE_NAME = 'SiteMetaData'

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2021, 5, 3),
    'retries': 0,
    'retry_delay': dt.timedelta(seconds=10),
    'provide_context': True,
    'depends_on_past': True,

}


def make_sta_client():
    connection = BaseHook.get_connection("nmbgmr_sta_conn_id")
    stac = STAClient(connection.host, connection.login, connection.password)
    return stac


FIELDS = ['Easting', 'PointID', 'AltDatum', 'Altitude', 'Northing', 'OBJECTID', 'SiteNames']
with DAG('NMBGMR_SiteMetadata0.2',
         schedule_interval='@hourly',
         catchup= False,
         default_args=default_args) as dag:
    def get_sql(**context):
        fields = ','.join(FIELDS)
        sql = f'''select {fields} from {DATASET_NAME}.{TABLE_NAME}'''

        newdate = context['prev_execution_date']
        logging.info(f'prevdate ={newdate}')
        ti = TaskInstance(get_sql, newdate)
        previous_max_objectid = ti.xcom_pull(task_ids='etl', key='return_value', include_prior_dates=True)

        logging.info(f'preov max {previous_max_objectid}')

        if previous_max_objectid:
            sql = f'{sql} where OBJECTID>{previous_max_objectid}'

        sql = f'{sql} order by OBJECTID LIMIT 100'

        return sql, FIELDS


    def do_etl(**context):
        ti = context['ti']

        data = ti.xcom_pull(task_ids='get-sites', key='return_value')
        stac = make_sta_client()

        for record in data:
            logging.info(record)
            # record = dict(zip(FIELDS, record))
            logging.info(record)
            properties = {k: record[k] for k in ('Altitude', 'AltDatum')}

            name = record['PointID']
            description = 'No Description'
            e = record['Easting']
            n = record['Northing']
            z = 13
            logging.info(f'PointID={name}, Easting={e},Northing={n}')
            lid = stac.add_location(name, description, properties, utm=(e, n, z))

            name = 'Water Well'
            description = 'No Description'
            properties = {}
            logging.info(f'Add thing to {lid}')
            stac.add_thing(name, description, properties, lid)

        return record['OBJECTID']


    get_sql = PythonOperator(task_id='get-sql', python_callable=get_sql)
    get_sites = BigQueryToXOperator(task_id='get-sites')
    etl = PythonOperator(task_id='etl', python_callable=do_etl)

    get_sql >> get_sites >> etl
