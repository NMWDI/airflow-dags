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
    make_gwl, make_gwl_payload

default_args = {
    'owner': 'Jake Ross',
    'start_date': dt.datetime(2021, 10, 5),
    'retries': 0,
    'retry_delay': dt.timedelta(seconds=10),
    'provide_context': True,
    'depends_on_past': False,
    'wait_for_downstream': True

}

TASK_ID = 'nmbgmr_manual_etl'

with DAG('NMBGMR_WELL_MANUAL_DATASTREAMS_0.1.0',
         # schedule_interval='@daily',
         # schedule_interval='*/7 * * * *',
         schedule_interval='0 2 * * *',
         catchup=False,
         default_args=default_args) as dag:
    def datastream_etl(**context):
        # dataset = Variable.get('bq_levels')
        # table_name = Variable.get('nmbgmr__tbl')
        # table_name = 'nmbgmrPressureGWL'

        bq = BigQueryHook()
        conn = bq.get_conn()
        cursor = conn.cursor()

        fields = ['WellID', 'PointID', 'OBJECTID', 'DataSource', 'DataQuality', 'LevelStatus', 'MeasuringAgency',
                  'DepthToWater',
                  'DepthToWaterBGS',
                  'DateTimeMeasured', 'MeasurementMethod']
        dataset = 'levels'
        table_name = 'nmbgmrManualGWL'

        records, total_records, max_objectid = make_gwl(cursor, fields, dataset, table_name, context, TASK_ID)

        cnt = 0

        stac = make_sta_client(use_local=True)
        obsprop_id = stac.add_observed_property(*BGS_OBSERVED_PROPERTY)
        raw_obsprop_id = stac.add_observed_property(*RAW_OBSERVED_PROPERTY)
        sensor_id = stac.add_sensor('Manual', MANUAL_SENSOR_DESCRIPTION)

        components = ['phenomenonTime', 'resultTime', 'result', 'parameters', 'resultQuality']
        key = itemgetter(fields.index('PointID'))
        for pointid, records in groupby(sorted(records, key=key), key=key):
            rs = [dict(zip(fields, record)) for record in records]
            nr = len(rs)
            cnt += nr
            thing_id = stac.get_thing_id(name='Water Well', location_name=pointid)
            logging.info(f'*********** PointID={pointid}, cnt={cnt}, nr={nr} total_records={total_records}')
            if thing_id:
                # for mm, rs in groupby(records, key=itemgetter(fields.index('MeasurementMethod'))):
                st = time.time()

                properties = {'topic': 'Water Quantity',
                              'agency': 'NMBGMR'}

                ds_id, added = stac.add_datastream(GWL_DATASTREAM, GWL_DESCRIPTION,
                                                   thing_id, obsprop_id, sensor_id,
                                                   properties)

                last_obs = None
                if not added:
                    last_obs = stac.get_last_observation(ds_id)

                def additional(x):
                    return ({k: x[k] for k in ('MeasurementMethod',
                                               'DataSource',
                                               'LevelStatus',
                                               'MeasuringAgency')},
                            x['DataQuality'],)

                payload = make_gwl_payload(stac, rs, 'DepthToWaterBGS', last_obs,
                                           additional=additional)
                stac.add_observations(ds_id, components, payload)

                rds_id, added = stac.add_datastream(RAW_GWL_DATASTREAM, RAW_GWL_DESCRIPTION,
                                                    thing_id, raw_obsprop_id, sensor_id,
                                                    properties)

                payload = make_gwl_payload(stac, rs, 'DepthToWater', last_obs,
                                           additional=additional)
                stac.add_observations(rds_id, components, payload)

                logging.info(f'--------- added datastreams {ds_id}, {rds_id} to {thing_id} {time.time() - st}')

        if not records or cnt >= total_records:
            # start back at begin to reexamine sites
            logging.info(f'max objectid reset to 0, cnt={cnt}, total_records={total_records}')
            max_objectid = 0
        logging.info(f'max_objectid={max_objectid}')
        return max_objectid


    nmbgmr_etl = PythonOperator(python_callable=datastream_etl, task_id=TASK_ID)

# ============= EOF =============================================
