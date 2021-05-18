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

from airflow import DAG
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

from util import get_prev, make_sta_client
from operators.bq import BigQueryToXOperator

default_args = {
    'owner': 'Jake Ross',
    'start_date': dt.datetime(2021, 5, 3),
    'retries': 0,
    'retry_delay': dt.timedelta(seconds=10),
    'provide_context': True,
    # 'depends_on_past': True,

}

with DAG('NMBGMRSiteMetadata0.1',
         schedule_interval='@daily',
         catchup=False,
         default_args=default_args) as dag:
    def nmbgmr_get_sql(**context):
        fields = ['Easting', 'PointID', 'AltDatum', 'Altitude', 'Northing', 'OBJECTID', 'SiteNames']
        dataset = Variable.get('bq_locations')
        table_name = Variable.get('nmbgmr_site_tbl')
        fs = ','.join(fields)
        sql = f'''select {fs} from {dataset}.{table_name}'''

        previous_max_objectid = get_prev(context, 'nmbgmr-etl')
        if previous_max_objectid:
            sql = f'{sql} where OBJECTID>%(leftbounds)s'

        sql = f'{sql} order by OBJECTID LIMIT 100'
        return sql, fields, {'leftbounds': previous_max_objectid}


    def nmbgmr_etl(**context):
        ti = context['ti']

        data = ti.xcom_pull(task_ids='nmbgmr-get-sites', key='return_value')
        if data:
            stac = make_sta_client()

            for record in data:
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
        else:
            return get_prev(context, 'nmbgmr-etl')


    nmbgmr_get_sql = PythonOperator(task_id='nmbgmr-get-sql', python_callable=nmbgmr_get_sql)
    nmbgmr_get_sites = BigQueryToXOperator(task_id='nmbgmr-get-sites', sql_task_id='nmbgmr-get-sql')
    nmbgmr_etl = PythonOperator(task_id='nmbgmr-etl', python_callable=nmbgmr_etl)

    nmbgmr_get_sql >> nmbgmr_get_sites >> nmbgmr_etl

# ============= EOF =============================================
