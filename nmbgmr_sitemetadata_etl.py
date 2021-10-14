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
    # 'depends_on_past': True,
    'wait_for_downstream': True

}


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
    cursor.execute(sql, parameters={'OBJECTID': objectid})
    records = cursor.fetchall()
    logging.info(len(records))
    screens = {}
    for wi, s in groupby(records, key=itemgetter(0)):
        # logging.info(wi)
        # s = list(s)
        # logging.info(s)
        screens[wi] = [{c: si for c, si in zip(columns[1:], ss[1:])} for ss in s]

    return screens


with DAG('NMBGMR_WELL_LOCATION_THINGS_0.1.18',
         # schedule_interval='@daily',
         schedule_interval='*/7 * * * *',
         catchup=False,
         default_args=default_args) as dag:
    def nmbgmr_get_sql(**context):
        fields = ['Easting', 'PointID', 'AltDatum', 'Altitude', 'WellID',
                  'Northing', 'OBJECTID', 'SiteNames', 'WellDepth', 'CurrentUseDescription',
                  'StatusDescription', 'FormationZone']
        dataset = Variable.get('bq_locations')
        table_name = Variable.get('nmbgmr_site_tbl')
        fs = ','.join(fields)
        sql = f'''select {fs} from {dataset}.{table_name}'''

        previous_max_objectid = get_prev(context, 'nmbgmr-etl')
        # wsql = f'{sql} where OBJECTID>%(leftbounds)s'
        if previous_max_objectid:
            sql = f'{sql} where OBJECTID>%(leftbounds)s'

        limit = Variable.get('nmbgmr_sites_limit', 100)
        # wsql = f'{wsql} order by OBJECTID LIMIT {limit}'
        sql = f'{sql} order by OBJECTID LIMIT {limit}'
        return sql, fields, {'leftbounds': previous_max_objectid}


    def nmbgmr_etl(**context):
        ti = context['ti']

        # instead of
        data = ti.xcom_pull(task_ids='nmbgmr-get-sites', key='return_value')
        if data:
            stac = make_sta_client()

            st = time.time()
            bq = BigQueryHook()
            conn = bq.get_conn()
            cursor = conn.cursor()

            previous_max_objectid = get_prev(context, 'nmbgmr-etl')
            screens = make_screens(cursor, previous_max_objectid)
            logging.info(f'got screens  {time.time() - st}')
            # for k, v in screens.items():
            #     logging.info(f'{k},{v}')

            for record in data:
                # logging.info(record)

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

                # st = time.time()
                # screens = make_screens(cursor, record['WellID'])
                # logging.info(f'got screens for {record["WellID"]} {time.time()-st}')

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

            conn.close()
            return record['OBJECTID']
        else:
            # DAG will start back at the OBJECTID=0 and all the sites will be reexamined
            return 0

    nmbgmr_get_sql = PythonOperator(task_id='nmbgmr-get-sql', python_callable=nmbgmr_get_sql)
    nmbgmr_get_sites = BigQueryToXOperator(task_id='nmbgmr-get-sites', sql_task_id='nmbgmr-get-sql')
    nmbgmr_etl = PythonOperator(task_id='nmbgmr-etl', python_callable=nmbgmr_etl)
    nmbgmr_get_sql >> nmbgmr_get_sites >> nmbgmr_etl

# ============= EOF =============================================
