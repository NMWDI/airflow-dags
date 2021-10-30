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
import datetime as dt
import time
from itertools import groupby
from operator import itemgetter

from airflow import DAG
from airflow.contrib.hooks.bigquery_hook import BigQueryHook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator, PythonVirtualenvOperator

from util import get_prev, make_sta_client, make_total_records, BGS_OBSERVED_PROPERTY, GWL_DATASTREAM, \
    GWL_DESCRIPTION, RAW_OBSERVED_PROPERTY, RAW_GWL_DATASTREAM, RAW_GWL_DESCRIPTION, MANUAL_SENSOR_DESCRIPTION, \
    make_gwl, PRESSURE_SENSOR_DESCRIPTION, make_gwl_payload, WH_GWL_DATASTREAM, WH_GWL_DESCRIPTION, AWH_GWL_DATASTREAM, \
    AWH_GWL_DESCRIPTION, WH_OBSERVED_PROPERTY, AWH_OBSERVED_PROPERTY, make_location_gwl, PRESSURE_GWL_DATASTREAM, \
    get_pressure_pointids

default_args = {
    'owner': 'Jake Ross',
    'start_date': dt.datetime(2021, 10, 5),
    'retries': 0,
    'retry_delay': dt.timedelta(seconds=10),
    'provide_context': True,
    'depends_on_past': False,
    'wait_for_downstream': True

}

TASK_ID = 'nmbgmr_pressure_etl'

with DAG('NMBGMR_WELL_PRESSURE_DATASTREAMS_0.1.0',
         # schedule_interval='@daily',
         # schedule_interval='*/7 * * * *',
         schedule_interval='0 4 * * *',
         catchup=False,
         default_args=default_args) as dag:
    def datastream_etl(**context):
        # dataset = Variable.get('bq_levels')
        # table_name = Variable.get('nmbgmr__tbl')
        # table_name = 'nmbgmrPressureGWL'

        bq = BigQueryHook()
        conn = bq.get_conn()
        cursor = conn.cursor()

        fields = ['WellID', 'PointID', 'OBJECTID',
                  'WaterHead',
                  'WaterHeadAdjusted',
                  'DataSource',
                  'DepthToWaterBGS',
                  'MeasuringAgency',
                  'DateTimeMeasured',
                  ]

        dataset = 'levels'
        table_name = 'nmbgmrPressureGWL'

        stac = make_sta_client(use_local=True)
        bgs_obsprop_id = stac.add_observed_property(*BGS_OBSERVED_PROPERTY)
        wh_obsprop_id = stac.add_observed_property(*WH_OBSERVED_PROPERTY)
        awh_obsprop_id = stac.add_observed_property(*AWH_OBSERVED_PROPERTY)

        sensor_id = stac.add_sensor('Pressure', PRESSURE_SENSOR_DESCRIPTION)

        components = ['phenomenonTime', 'resultTime', 'result']
        pointids = get_pressure_pointids(cursor, dataset, table_name)
        for location in stac.get_locations(fs="properties/agency eq 'NMBGMR'"):
            name = location['name']
            iotid = location['@iot.id']
            logging.info(f'examining {name} id={iotid}')

            if name not in pointids:
                continue

            records = make_location_gwl(location['name'], cursor, fields, dataset, table_name)
            if not records:
                continue
            logging.info(f'found nrecords={len(records)}')

            st = time.time()
            rs = [dict(zip(fields, record)) for record in records]
            record = rs[0]

            thing_id = stac.get_thing_id(name='Water Well', location_id=location['@iot.id'])
            if thing_id:
                properties = {'topic': 'Water Quantity',
                              'agency': 'NMBGMR'}

                for t in ('WellID', 'PointID', 'MeasuringAgency', 'DataSource'):
                    properties[t] = record[t]

                ds_id, added = stac.add_datastream(PRESSURE_GWL_DATASTREAM, GWL_DESCRIPTION, thing_id, bgs_obsprop_id,
                                                   sensor_id,
                                                   properties)

                last_obs = None
                if not added:
                    last_obs = stac.get_last_observation(ds_id)

                payload = make_gwl_payload(stac, rs, 'DepthToWaterBGS', last_obs)
                stac.add_observations(ds_id, components, payload)

                wh_id, added = stac.add_datastream(WH_GWL_DATASTREAM, WH_GWL_DESCRIPTION,
                                                   thing_id, wh_obsprop_id, sensor_id,
                                                   properties)

                payload = make_gwl_payload(stac, rs, 'WaterHead', last_obs)
                stac.add_observations(wh_id, components, payload)

                awh_id, added = stac.add_datastream(AWH_GWL_DATASTREAM, AWH_GWL_DESCRIPTION,
                                                    thing_id, awh_obsprop_id, sensor_id,
                                                    properties)

                payload = make_gwl_payload(stac, rs, 'WaterHeadAdjusted', last_obs)
                stac.add_observations(awh_id, components, payload)

                logging.info(f'added datastreams {ds_id}, {wh_id}, {awh_id} to {thing_id} {time.time() - st}')


    nmbgmr_etl = PythonOperator(python_callable=datastream_etl, task_id=TASK_ID)
    # cleanup = PythonOperator(task_id='cleanup', python_callable=cleanup)

    # nmbgmr_etl >> cleanup
# ============= EOF =============================================
