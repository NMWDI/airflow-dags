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
from airflow.operators.python_operator import PythonOperator

from util import get_prev, make_sta_client
from operators.bq import BigQueryToXOperator

default_args = {
    'owner': 'Jake Ross',
    'start_date': dt.datetime(2021, 10, 5),
    'retries': 0,
    'retry_delay': dt.timedelta(seconds=10),
    'provide_context': True,
    'depends_on_past': False,
    'wait_for_downstream': True

}


def make_total_records(cursor):
    cursor.flush_results()
    dataset = Variable.get('bq_locations')
    # table_name = Variable.get('nmbgmr_screen_tbl')
    table_name = Variable.get('nmbgmr_site_tbl')

    sql = f'''SELECT count(*) from {dataset}.{table_name}'''
    cursor.execute(sql)
    cnt = int(cursor.fetchone()[0])
    cursor.flush_results()
    return cnt


def make_screens(cursor, objectid):
    dataset = Variable.get('bq_locations')
    table_name = Variable.get('nmbgmr_screen_tbl')
    site_table_name = Variable.get('nmbgmr_site_tbl')

    # sql = f'select PointID, ScreenTop, ScreenBottom, ScreenDescription from {dataset}.{table_name} ' \
    #       f'where PointID in (%(pids)s) order by PointID'
    columns = 'ws.PointID', 'ScreenTop', 'ScreenBottom', 'ScreenDescription'
    cs = ','.join(columns)
    # sql = f'select {cs} from {dataset}.{table_name} ' \
    #       f'order by PointID'

    sql = f'select {cs} from {dataset}.{table_name} as ws ' \
          f'join {dataset}.{site_table_name} as wd on wd.WellID= ws.WellID ' \
          f'where wd.OBJECTID>%(OBJECTID)s ' \
          f'order by ws.PointID'

    # pids = ','.join([f'"{w}"' for w in pids])

    # logging.info(sql)
    # logging.info(pids)
    cursor.execute(sql, parameters={'OBJECTID': objectid or 0})
    records = cursor.fetchall()

    screens = {}
    for wi, s in groupby(records, key=itemgetter(0)):
        # logging.info(wi)
        # s = list(s)
        # logging.info(s)
        screens[wi] = [{c: si for c, si in zip(columns[1:], ss[1:])} for ss in s]
    cursor.flush_results()
    return screens


with DAG('NMBGMR_WELL_LOCATION_THINGS_0.2.5',
         # schedule_interval='@daily',
         # schedule_interval='*/7 * * * *',
         schedule_interval='0 */12 * * *',
         catchup=False,
         default_args=default_args) as dag:
    def nmbgmr_etl(**context):
        fields = ['Easting', 'PointID', 'AltDatum', 'Altitude', 'WellID',
                  'Northing', 'OBJECTID', 'SiteNames', 'WellDepth', 'CurrentUseDescription',
                  'StatusDescription', 'FormationZone']
        dataset = Variable.get('bq_locations')
        table_name = Variable.get('nmbgmr_site_tbl')
        fs = ','.join(fields)
        sql = f'''select {fs} from {dataset}.{table_name}'''

        previous_max_objectid = get_prev(context, 'nmbgmr-etl')
        if previous_max_objectid:
            sql = f'{sql} where OBJECTID>%(leftbounds)s'

        limit = int(Variable.get('nmbgmr_sites_limit', 100))

        sql = f'{sql} order by OBJECTID LIMIT {limit}'
        params = {'leftbounds': previous_max_objectid}

        bq = BigQueryHook()
        conn = bq.get_conn()
        cursor = conn.cursor()
        st = time.time()
        screens = make_screens(cursor, previous_max_objectid)
        logging.info(f'got screens {len(screens)} {time.time() - st}')

        total_records = make_total_records(cursor)
        logging.info(f'total records={total_records}, limit={limit}')
        # data = cursor.fetchall()
        if limit > total_records:
            logging.info('doing a complete overwrite')

        stac = make_sta_client()
        cursor.execute(sql, params)
        fetched_any = False
        gst = time.time()
        cnt = 0
        while 1:
            record = cursor.fetchone()
            if not record:
                break

            fetched_any = True
            record = dict(zip(fields, record))
            logging.info(record)
            properties = {k: record[k] for k in ('Altitude', 'AltDatum')}
            properties['agency'] = 'NMBGMR'
            name = record['PointID'].upper()
            description = 'Location of well where measurements are made'
            e = record['Easting']
            n = record['Northing']
            z = 13
            # logging.info(f'PointID={name}, Easting={e},Northing={n}')
            st = time.time()
            lid, added = stac.add_location(name, description, properties, utm=(e, n, z))
            logging.info(f'added location {lid} {time.time() - st}')
            properties['geoconnex'] = f'https://geoconnex.us/nmwdi/st/locations/{lid}'
            stac.patch_location(lid, {'properties': properties})

            name = 'Water Well'
            description = 'Well drilled or set into subsurface for the purposes ' \
                          'of pumping water or monitoring groundwater'

            properties = {'WellDepth': record['WellDepth'],
                          'GeologicFormation': record['FormationZone'],
                          'Use': record['CurrentUseDescription'],
                          'Status': record['StatusDescription'],
                          'Screens': screens.get(record['PointID'], []),
                          'agency': 'NMBGMR'}
            # logging.info(f'Add thing to {lid}')
            st = time.time()
            stac.add_thing(name, description, properties, lid)
            logging.info(f'added thing to {lid} {time.time() - st}')
            previous_max_objectid = record['OBJECTID']
            cnt += 1

        t = time.time() - gst
        logging.info(f'total upload time={t} n={cnt} avg={cnt / t}')
        conn.close()
        if not fetched_any or cnt >= total_records:
            # start back at begin to reexamine sites
            previous_max_objectid = 0

        return previous_max_objectid


    nmbgmr_etl = PythonOperator(task_id='nmbgmr-etl', python_callable=nmbgmr_etl)

# ============= EOF =============================================
