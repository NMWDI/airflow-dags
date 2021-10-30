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
from operator import attrgetter

from airflow.hooks.base_hook import BaseHook
from airflow.models import TaskInstance


def make_gwl_payload(stac, rs, tag, last_obs, additional=None):
    if last_obs:
        rs = [ri for ri in rs if stac.make_st_time(ri['DateTimeMeasured']) > last_obs]

    if rs:
        if not additional:
            def additional(x):
                return tuple()

        payload = [(stac.make_st_time(r['DateTimeMeasured']),
                    stac.make_st_time(r['DateTimeMeasured']),
                    f'{float(r[tag]):0.2f}' if r[tag] is not None else None) + additional(r) for r in rs]

        return payload


def get_pressure_pointids(cursor, dataset, table_name):
    cursor.flush_results()
    sql = f'''select PointID from {dataset}.{table_name} group by PointID'''

    cursor.execute(sql)
    ret = cursor.fetchall()
    cursor.flush_results()
    return [r[0] for r in ret]


def make_location_gwl(pointid, cursor, fields, dataset, table_name):
    cursor.flush_results()
    fs = ','.join(fields)
    sql = f'''select {fs} from {dataset}.{table_name} where PointID=%(pointid)s'''

    cursor.execute(sql, {'pointid': pointid})
    ret = cursor.fetchall()
    cursor.flush_results()
    return ret


def make_gwl(cursor, fields, dataset, table_name, context, task_id):
    cursor.flush_results()
    fs = ','.join(fields)
    sql = f'''select {fs} from {dataset}.{table_name}'''

    previous_max_objectid = get_prev(context, task_id)

    logging.info(f'previous max objectid={previous_max_objectid}')
    params = {}
    if previous_max_objectid:
        sql = f'{sql} where OBJECTID>%(leftbounds)s'
        params['leftbounds'] = previous_max_objectid

    total_records = make_total_records(cursor, dataset, table_name, previous_max_objectid)
    # limit = int(Variable.get('nmbgmr_s_limit', 100))
    limit = 50000

    sql = f'{sql} order by PointID,ObjectID LIMIT {limit}'

    logging.info(f'sql: {sql}')
    cursor.execute(sql, params)

    logging.info(f'total records={total_records}, limit={limit}')
    # data = cursor.fetchall()
    if limit > total_records:
        logging.info('doing a complete overwrite')

    records = cursor.fetchall()

    max_objectid = 0
    if records:
        max_objectid = max((r[fields.index('OBJECTID')] for r in records))

    cursor.flush_results()
    return records, total_records, max_objectid


def make_total_records(cursor, dataset, table_name, objectid=None):
    cursor.flush_results()
    sql = f'''SELECT count(*) from {dataset}.{table_name}'''
    params = {}
    if objectid:
        params['leftbounds'] = objectid
        sql = f'{sql} where OBJECTID>%(leftbounds)s'

    cursor.execute(sql, params)
    cnt = int(cursor.fetchone()[0])
    cursor.flush_results()
    return cnt


def make_stamqtt_client():
    connection = BaseHook.get_connection('nmbgmr_sta_conn_id')
    from sta.sta_client import STAMQTTClient
    staclient = STAMQTTClient(connection.host)
    return staclient


def make_sta_client(sta_key='nmbgmr_sta_conn_id', use_local=False):
    if use_local:
        from sta_local.sta_client import STAClient
    else:
        from sta.sta_client import STAClient

    connection = BaseHook.get_connection(sta_key)
    stac = STAClient(connection.host, connection.login, connection.password,
                     connection.port)
    return stac


def get_prev(context, task_id):
    newdate = context['prev_execution_date']
    logging.info(f'prevdate ={newdate}')
    ti = TaskInstance(context['task'], newdate)
    previous_max = ti.xcom_pull(task_ids=task_id, key='return_value', include_prior_dates=True)
    logging.info(f'prev max {previous_max}')
    return previous_max


GWL_DATASTREAM = 'Groundwater Levels'
PRESSURE_GWL_DATASTREAM = 'Groundwater Levels(Pressure)'
ACOUSTIC_GWL_DATASTREAM = 'Groundwater Levels(Acoustic)'
GWL_DESCRIPTION = 'Measurement of groundwater depth in a water well, as measured below ground surface'
RAW_GWL_DATASTREAM = 'Raw Groundwater Depth'
RAW_GWL_DESCRIPTION = 'Uncorrected measurement of groundwater depth in a water well, as measured from a reference ' \
                      'measuring point'
WH_GWL_DATASTREAM = 'Groundwater Head'
WH_GWL_DESCRIPTION = 'Measurement of water above the transducer. Not Quality Controlled'

AWH_GWL_DATASTREAM = 'Adjusted Groundwater Head'
AWH_GWL_DESCRIPTION = 'Measurement of water above the transducer. Quality Controlled'

WATER_WELL = 'Water Well'
BGS_OBSERVED_PROPERTY = ('Depth to Water Below Ground Surface', 'depth to water below ground surface')
WH_OBSERVED_PROPERTY = ('Groundwater Head', 'Water pressure measured by transducer')
AWH_OBSERVED_PROPERTY = ('Adjusted Groundwater Head', 'Water pressure measured by transducer corrected by manual '
                                                      'measurements')
RAW_OBSERVED_PROPERTY = ('Raw Depth to Water', 'uncorrected measurement of depth to water from measuring point')
MANUAL_SENSOR_DESCRIPTION = 'Manual measurement of groundwater depth by steel tape, electronic probe or other'
PRESSURE_SENSOR_DESCRIPTION = '''Continuous (periodic automated) measurement depth to water in Feet below ground 
surface (converted from pressure reading from depth below ground surface in feet). Not Provisional. Quality 
Controlled'''
ACOUSTIC_SENSOR_DESCRIPTION = '''Continuous (periodic automated) measurement depth to water in Feet below ground 
surface (converted from acoustic device). Not Provisional. Quality Controlled '''
# MANUAL_SENSOR = ('Manual', 'Manual measurement of groundwater depth by steel tape, electronic probe or other')
# ============= EOF =============================================
