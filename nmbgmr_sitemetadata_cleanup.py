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
#
#
# def make_total_records(cursor):
#     cursor.flush_results()
#     dataset = Variable.get('bq_locations')
#     # table_name = Variable.get('nmbgmr_screen_tbl')
#     table_name = Variable.get('nmbgmr_site_tbl')
#
#     sql = f'''SELECT count(*) from {dataset}.{table_name}'''
#     cursor.execute(sql)
#     cnt = int(cursor.fetchone()[0])
#     cursor.flush_results()
#     return cnt
#
#
# def make_screens(cursor, objectid):
#     dataset = Variable.get('bq_locations')
#     table_name = Variable.get('nmbgmr_screen_tbl')
#     site_table_name = Variable.get('nmbgmr_site_tbl')
#
#     # sql = f'select PointID, ScreenTop, ScreenBottom, ScreenDescription from {dataset}.{table_name} ' \
#     #       f'where PointID in (%(pids)s) order by PointID'
#     columns = 'ws.PointID', 'ScreenTop', 'ScreenBottom', 'ScreenDescription'
#     cs = ','.join(columns)
#     # sql = f'select {cs} from {dataset}.{table_name} ' \
#     #       f'order by PointID'
#
#     sql = f'select {cs} from {dataset}.{table_name} as ws ' \
#           f'join {dataset}.{site_table_name} as wd on wd.WellID= ws.WellID ' \
#           f'where wd.OBJECTID>%(OBJECTID)s ' \
#           f'order by ws.PointID'
#
#     # pids = ','.join([f'"{w}"' for w in pids])
#
#     # logging.info(sql)
#     # logging.info(pids)
#     cursor.execute(sql, parameters={'OBJECTID': objectid or 0})
#     records = cursor.fetchall()
#
#     screens = {}
#     for wi, s in groupby(records, key=itemgetter(0)):
#         # logging.info(wi)
#         # s = list(s)
#         # logging.info(s)
#         screens[wi] = [{c: si for c, si in zip(columns[1:], ss[1:])} for ss in s]
#     cursor.flush_results()
#     return screens


with DAG('NMBGMR_WELL_LOCATION_THINGS_CLEANUP_0.0.1',
         # schedule_interval='@daily',
         # schedule_interval='*/7 * * * *',
         schedule_interval='0 */12 * * *',
         catchup=False,
         default_args=default_args) as dag:
    def cleanup(**context):
        """
        get all locations from source and destination
        :param context:
        :return:
        """
        bq = BigQueryHook()
        conn = bq.get_conn()
        cursor = conn.cursor()

        dataset = Variable.get('bq_locations')
        table_name = Variable.get('nmbgmr_site_tbl')
        delete_enabled = bool(int(Variable.get('st_delete_enabled', default_var=0)))

        sql = f'select OBJECTID, PointID from {dataset}.{table_name}'
        cursor.execute(sql)
        src = cursor.fetchall()

        logging.info(f'src len={len(src)}')
        stac = make_sta_client()
        dst = stac.get_locations(fs="properties/agency eq 'NMBGMR'", orderby='id asc')
        logging.info(f'dst len={len(dst)}')
        deleted = []
        for l in dst:
            logging.info(f'checking {l}')
            if not next((s for s in src if s[1] == l['name']), None):
                logging.info(f'****** Requires removal {l}')
                if delete_enabled:
                    if stac.delete_location(l['@iot.id']):
                        deleted.append((l['@iot.id'], l))

        logging.info('Deleted ============================')
        for d in deleted:
            logging.info(d)
        logging.info('====================================')

    cleanup = PythonOperator(task_id='cleanup', python_callable=cleanup)

# ============= EOF =============================================
