# ===============================================================================
# Copyright 2021 ross
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# ===============================================================================
import logging

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator

from itertools import groupby
from operator import itemgetter
import datetime as dt

from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryGetDataOperator
)

from util import make_sta_client, make_stamqtt_client
from sta.sta_client import STAMQTTClient, make_st_time

DATASET_NAME = 'nmbgmr_gwl'

default_args = {
    'owner': 'Jake Ross',
    'start_date': dt.datetime(2021, 5, 14),
    'retries': 1,
    'retry_delay': dt.timedelta(seconds=10),
    'provide_context': True
}


def tl_gwl(context, task_id, thing_name, datastream_name, sensor_args):
    ti = context['ti']
    data = ti.xcom_pull(task_ids=task_id)

    stac = make_sta_client()

    key = itemgetter(0)

    keys = ('pointid', 'datemeasured', 'dtw')

    stam = make_stamqtt_client()

    obsprop_id = stac.add_observed_property('Groundwater Depth', 'depth to water below ground surface')
    sensor_id = stac.add_sensor(*sensor_args)
    for pointid, records in groupby(sorted(data, key=key), key=key):
        vs = [dict(zip(keys, values)) for values in records]

        # get the data stream for this pointid.
        # pointid give us Location. assume Thing.name eq 'Water Well'
        # assume observed
        datastream_id = stac.get_datastream(pointid, thing_name, datastream_name)
        if datastream_id:
            # # get the last available measurement from STA
            lobs = stac.get_last_observation(datastream_id)
        else:
            lobs = None
            # get location
            location_id = stac.get_location_id(pointid)
            # get thing
            thing_id = stac.get_thing_id(thing_name, location_id)
            if not thing_id:
                logging.info('Location/Thing does not exist {}/{}. skipping datastream'.format(pointid, thing_name))
                continue

            datastream_id = stac.add_datastream(datastream_name, thing_id, obsprop_id, sensor_id)

        if datastream_id:
            cobs = make_st_time(vs[-1]['datemeasured'])
            logging.info(f'last obs {lobs}, current: {cobs}')
            if not lobs or lobs > cobs:
                n = len(vs)
                logging.info(f'setting records for {pointid} n={n}')
                stam.add_observations(datastream_id, vs)
            # for k, v in vs.items():
            #     stam.add_observations({'datastream_id': datastream_id, 'records': v})


with DAG('NMBGMR_GWL_ETL0.1', default_args=default_args) as dag:
    def transform_manual(**context):
        task_id = 'get_manual'
        thing_name = 'Water Well'
        datastream_name = 'Depth Below Surface - Manual'

        tl_gwl(context, task_id, thing_name, datastream_name,
               ('Manual', 'Manual measurement of groundwater depth by field technician'))

    def transform_pressure(**context):
        task_id = 'get_pressure'
        thing_name = 'Water Well'
        datastream_name = 'Depth Below Surface - Continuous'

        tl_gwl(context, task_id, thing_name, datastream_name,
               ('Pressure Transducer', ' Continuous measurement of groundwater depth by pressure transducer'))

    def transform_acoustic(**context):
        task_id = 'get_acoustic'
        thing_name = 'Water Well'
        datastream_name = 'Depth Below Surface - Continuous'

        tl_gwl(context, task_id, thing_name, datastream_name,
               ('Acoustic', ' Continuous measurement of groundwater depth by pressure transducer'))


    gm = BigQueryGetDataOperator(task_id='get_manual',
                                 dataset_id='levels',
                                 bigquery_conn_id=None,
                                 selected_fields="PointID,DateMeasured,DepthToWaterBGS",
                                 table_id='nmbgmrManual')
    tm = PythonOperator(task_id='transform_manual', python_callable=transform_manual)

    # gp = BigQueryGetDataOperator(task_id='get_pressure',
    #                              dataset_id='levels',
    #                              bigquery_conn_id=None,
    #                              selected_fields="PointID,DateMeasured,DepthToWaterBGS",
    #                              table_id='nmbgmrPressure')
    # tp = PythonOperator(task_id='transform_pressure', python_callable=transform_pressure)
    #
    # ga = BigQueryGetDataOperator(task_id='get_acoustic',
    #                              dataset_id='levels',
    #                              bigquery_conn_id=None,
    #                              selected_fields="PointID,DateMeasured,DepthToWaterBGS",
    #                              table_id='nmbgmrPressure')
    # ta = PythonOperator(task_id='transform_acoustic', python_callable=transform_acoustic)

    gm >> tm
    # gp >> tp
    # ga >> ta

# ============= EOF =============================================
