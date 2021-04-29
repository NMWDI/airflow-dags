import logging

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator

from itertools import groupby
from operator import itemgetter
import datetime as dt
import os

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

DATASET_NAME = 'nmbgmr_gwl'

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2021, 4, 20),
    'retries': 1,
    'retry_delay': dt.timedelta(seconds=10),
    'provide_context': True
}


with DAG('GWL_ETL', default_args=default_args) as dag:
    def extract(**kw):
        ti = kw['ti']

        data = ti.xcom_pull(task_ids='get-manual')

        connection = BaseHook.get_connection("nmbgmr_sta_conn_id")
        stac = STAClient(connection.host)

        key = itemgetter(0)

        pointids = {}
        keys = ('pointid', 'datemeasured', 'dtw')

        for pointid, records in groupby(sorted(data, key=key), key=key):
            vs = [dict(zip(keys, values)) for values in records]

            # get the last available measurement from STA
            datastream = 3
            lobs = stac.get_last_observation(datastream)

            cobs = make_st_time(vs[-1]['datemeasured'])

            logging.info(f'last obs {lobs}, current: {cobs}')
            if not lobs or lobs > cobs:
                n = len(vs)
                logging.info(f'setting records for {pointid} n={n}')
                pointids[pointid] = vs

        ti.xcom_push('pointids', pointids)


    def transform(**kw):
        ti = kw['ti']
        values = ti.xcom_pull(task_ids='extract', key='pointids')

        connection = BaseHook.get_connection("nmbgmr_sta_conn_id")
        staclient = STAMQTTClient(connection.host)
        for k, v in values.items():
            staclient.add_observations({'datastream_id': 3, 'records': v})

        # print(f'got values {values}')
        # data = json.loads(data)
        # print(f'got data {data}')


    # get_dataset = BigQueryGetDatasetOperator(task_id="get-dataset", dataset_id=DATASET_NAME)
    get_manual = BigQueryGetDataOperator(task_id='get-manual',
                                         dataset_id=DATASET_NAME,
                                         bigquery_conn_id=None,
                                         selected_fields="PointID,DateMeasured,DepthToWaterBGS",
                                         table_id='Manual')
    extract_task = PythonOperator(task_id='extract', python_callable=extract)
    transform_task = PythonOperator(task_id='transform', python_callable=transform)

    get_manual >> extract_task >> transform_task
