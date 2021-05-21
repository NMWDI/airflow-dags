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
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from itertools import groupby
from operator import itemgetter, attrgetter
from operators.bq import BigQueryToXOperator
import datetime as dt

from util import make_sta_client, make_stamqtt_client, get_prev
from sta.sta_client import STAMQTTClient, make_st_time

POINTID = 'PointID'
DATEMEASURED = 'DateMeasured'
DTW = 'DepthToWaterBGS'
OBJECTID = 'OBJECTID'


class BigQueryETLLevels(BigQueryToXOperator):
    def __init__(self, thing_name, datastream_name, sensor_args, *args, **kw):
        super(BigQueryETLLevels, self).__init__(*args, **kw)
        self._sensor_args = sensor_args
        self._thing_name = thing_name
        self._datastream_name = datastream_name

    def execute(self, context):
        vs = super(BigQueryETLLevels, self).execute(context)
        if vs:
            max_objectid, nt = self._handle(context, vs)
            logging.info(f'max objectid={max_objectid}')
            _, _, _, wsql = context['ti'].xcom_pull(task_ids=self.sql_task_id, key='return_value')
            try:
                backfill_iterations = int(Variable.get('backfill_iterations', default_var=10))
            except ValueError:
                backfill_iterations = 10

            for i in range(backfill_iterations):
                params = {'leftbounds': max_objectid}
                cursor = self._execute_cursor(wsql, params)
                records = self._get_records(cursor)
                if records:
                    logging.info(f'nrecords={len(records)}')
                    nmax_objectid, n = self._handle(context, records)
                    nt += n
                    logging.info(f'maxobjectid={max_objectid} new max objectid={nmax_objectid} nt={nt} n={n}')
                    max_objectid = max(max_objectid, nmax_objectid)
                else:
                    break

            logging.info(f'nrecords added={nt}')
            return max_objectid
        else:
            return get_prev(context, self.task_id)

    def _handle(self, context, data):
        stac = make_sta_client()
        thing_name = self._thing_name
        datastream_name = self._datastream_name

        # key = itemgetter(0)
        key = itemgetter(POINTID)
        dkey = itemgetter(OBJECTID)

        # stam = make_stamqtt_client()

        obsprop_id = stac.add_observed_property('Groundwater Depth', 'depth to water below ground surface')
        sensor_id = stac.add_sensor(*self._sensor_args)
        # tcobs = None
        max_objectid = None
        n = 0
        for pointid, records in groupby(sorted(data, key=key), key=key):
            pointid = pointid.upper()
            vs = sorted(records, key=dkey)
            vs = [vi for vi in vs if vi[DTW] is not None]
            n += len(vs)
            cmax_objectid = max([vi[OBJECTID] for vi in vs])
            # cmax_objectid = vs[-1][OBJECTID]
            # logging.info(f'last obs {max_objectid}, current: {cmax_objectid}')
            if max_objectid is None or cmax_objectid > max_objectid:
                max_objectid = cmax_objectid

            # get the data stream for this pointid.
            # pointid give us Location. assume Thing.name eq 'Water Well'
            # assume observed
            datastream_id = stac.get_datastream(pointid, thing_name, datastream_name)
            if not datastream_id:
                # get location
                location_id = stac.get_location_id(pointid)
                if location_id:
                    # get thing
                    thing_id = stac.get_thing_id(thing_name, location_id)
                    if not thing_id:
                        logging.info(
                            'Location/Thing does not exist {}/{}. skipping datastream'.format(pointid, thing_name))

                        continue

                else:
                    logging.info('Location does not exist {}'.format(pointid))
                    continue

                datastream_id = stac.add_datastream(datastream_name, thing_id, obsprop_id, sensor_id)

            if datastream_id:
                if vs:
                    # n = len(vs)
                    # logging.info(f'setting records for {pointid} n={n}')

                    # payloads = [{'result': float(vi[DTW]),
                    #              'phenomenonTime': make_st_time(vi[DATEMEASURED])} for vi in vs]
                    components = ['phenomenonTime', 'resultTime', 'result']
                    obs = [(make_st_time(vi[DATEMEASURED]), make_st_time(vi[DATEMEASURED]), float(vi[DTW])) for vi in vs]
                    stac.add_observations(datastream_id, components, obs)

                    # cmax_objectid = max([vi[OBJECTID] for vi in vs])
                    # # cmax_objectid = vs[-1][OBJECTID]
                    # #logging.info(f'last obs {max_objectid}, current: {cmax_objectid}')
                    # if max_objectid is None or cmax_objectid > max_objectid:
                    #     max_objectid = cmax_objectid

        if max_objectid is not None:
            return max_objectid, n
        else:
            return get_prev(context, self.task_id), n


default_args = {
    'owner': 'Jake Ross',
    'start_date': dt.datetime(2021, 5, 20),
    'retries': 1,
    'retry_delay': dt.timedelta(seconds=10),
    'provide_context': True,
    'depends_on_past': True,
    'wait_for_downstream': True
}

with DAG('NMBGMR_GWL_ETL0.2',
         # schedule_interval='*/10 * * * *',
         schedule_interval='@daily',
         max_active_runs=1,
         catchup=False,
         default_args=default_args) as dag:
    def get_sql_manual(**context):
        fields = [POINTID, OBJECTID, DATEMEASURED, DTW]
        dataset = Variable.get('bq_levels')
        table_name = Variable.get('nmbgmr_manual_level_tbl')
        fs = ','.join(fields)
        sql = f'''select {fs} from {dataset}.{table_name} where {DTW} is not NULL '''

        wsql = f'{sql} and {OBJECTID}>%(leftbounds)s'
        previous_max_objectid = get_prev(context, 'etl_levels')
        if previous_max_objectid:
            sql = f'{sql} and {OBJECTID}>%(leftbounds)s'

        wsql = f'{wsql} order by {OBJECTID} asc LIMIT 200'
        sql = f'{sql} order by {OBJECTID} asc LIMIT 200'
        return sql, fields, {'leftbounds': previous_max_objectid}, wsql


    # def transform_manual(**context):
    #     task_id = 'transform_manual'
    #     source_task_id = 'get_manual'
    #     thing_name = 'Water Well'
    #     datastream_name = 'Depth Below Surface - Manual'
    #
    #     return tl_gwl(context, task_id, source_task_id, thing_name, datastream_name,
    #                   ('Manual', 'Manual measurement of groundwater depth by field technician'))
    #
    #
    # def transform_pressure(**context):
    #     task_id = 'get_pressure'
    #     thing_name = 'Water Well'
    #     datastream_name = 'Depth Below Surface - Continuous'
    #
    #     tl_gwl(context, task_id, thing_name, datastream_name,
    #            ('Pressure Transducer', ' Continuous measurement of groundwater depth by pressure transducer'))
    #
    #
    # def transform_acoustic(**context):
    #     task_id = 'get_acoustic'
    #     thing_name = 'Water Well'
    #     datastream_name = 'Depth Below Surface - Continuous'
    #
    #     tl_gwl(context, task_id, thing_name, datastream_name,
    #            ('Acoustic', ' Continuous measurement of groundwater depth by pressure transducer'))

    # gm = BigQueryGetDataOperator(task_id='get_manual',
    #                              dataset_id='levels',
    #                              bigquery_conn_id=None,
    #                              selected_fields="PointID,DateMeasured,DepthToWaterBGS",
    #                              table_id='nmbgmrManual')
    gs = PythonOperator(task_id='get_sql', python_callable=get_sql_manual)
    # gm = BigQueryToXOperator(task_id='get_manual', sql_task_id='get_sql')
    gm = BigQueryETLLevels('Water Well',
                           'Depth Below Surface - Manual',
                           ('Manual', 'Manual measurement of groundwater depth by field technician'),
                           task_id='etl_levels', sql_task_id='get_sql')
    # tm = PythonOperator(task_id='transform_manual', python_callable=transform_manual)

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

    gs >> gm
    # gp >> tp
    # ga >> ta

# ============= EOF =============================================

# def tl_gwl(context, task_id, source_task_id, thing_name, datastream_name, sensor_args):
#     ti = context['ti']
#     data = ti.xcom_pull(task_ids=source_task_id)
#
#     stac = make_sta_client()
#
#     # key = itemgetter(0)
#     key = itemgetter(POINTID)
#     dkey = itemgetter(DATEMEASURED)
#
#     stam = make_stamqtt_client()
#
#     obsprop_id = stac.add_observed_property('Groundwater Depth', 'depth to water below ground surface')
#     sensor_id = stac.add_sensor(*sensor_args)
#     tcobs = None
#     for pointid, records in groupby(sorted(data, key=key), key=key):
#         # vs = [dict(zip(keys, values)) for values in records]
#
#         # get the data stream for this pointid.
#         # pointid give us Location. assume Thing.name eq 'Water Well'
#         # assume observed
#         datastream_id = stac.get_datastream(pointid, thing_name, datastream_name)
#         if datastream_id:
#             # # get the last available measurement from STA
#             lobs = stac.get_last_observation(datastream_id)
#         else:
#             lobs = None
#             # get location
#             location_id = stac.get_location_id(pointid)
#             if location_id:
#                 # get thing
#                 thing_id = stac.get_thing_id(thing_name, location_id)
#                 if not thing_id:
#                     logging.info('Location/Thing does not exist {}/{}. skipping datastream'.format(pointid, thing_name))
#                     continue
#             else:
#                 logging.info('Location does not exist {}'.format(pointid))
#                 continue
#
#             datastream_id = stac.add_datastream(datastream_name, thing_id, obsprop_id, sensor_id)
#
#         if datastream_id:
#             vs = sorted(records, key=dkey)
#             vs = [vi for vi in vs if vi[DTW] is not None]
#
#             if lobs:
#                 vs = [vi for vi in vs if make_st_time(vi[DATEMEASURED]) > lobs]
#
#             if vs:
#                 n = len(vs)
#                 logging.info(f'setting records for {pointid} n={n}')
#
#                 payloads = [{'result': float(vi[DTW]),
#                              'phenomenonTime': make_st_time(vi[DATEMEASURED])} for vi in vs]
#                 stam.add_observations(datastream_id, payloads)
#
#                 cobs = make_st_time(vs[-1][DATEMEASURED])
#                 logging.info(f'last obs {lobs}, current: {cobs}')
#                 if tcobs is None or cobs >= tcobs:
#                     tcobs = cobs
#
#     if tcobs:
#         return tcobs
#     else:
#         return get_prev(context, task_id)
