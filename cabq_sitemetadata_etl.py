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
from airflow.operators.python_operator import PythonOperator

from util import make_sta_client, get_prev
from operators.bq import BigQueryToXOperator

default_args = {
    'owner': 'me',
    'start_date': dt.datetime(2021, 5, 3),
    'retries': 0,
    'retry_delay': dt.timedelta(seconds=10),
    'provide_context': True,
    # 'depends_on_past': True,

}


with DAG('CABQSiteMetadata0.1',
         schedule_interval='@hourly',
         catchup= False,
         default_args=default_args) as dag:

    def cabq_get_sql(**context):
        fields = ['loc_name', 'latitude', 'longitude', '_airbyte_emitted_at']

        dataset_name = 'cabq_gwl'
        table_name = 'GWL'

        # dataset = Variable.get('bg_locations')
        # table_name = Variable.get('cabq_site_tbl')

        fs = ','.join(fields)
        sql = f'''select {fs} from {dataset_name}.{table_name}'''

        previous_max_objectid = get_prev(context, 'cabq-etl')
        if previous_max_objectid:
            if isinstance(previous_max_objectid, float):
                previous_max_objectid = dt.datetime.utcfromtimestamp(previous_max_objectid)

            if isinstance(previous_max_objectid, dt.datetime):
                previous_max_objectid = previous_max_objectid.isoformat()

            sql = f"{sql} where _airbyte_emitted_at>%(leftbounds)s"

        sql = f'{sql} order by _airbyte_emitted_at LIMIT 100'

        logging.info(f'sql {sql}')
        logging.info(f'fields {fields}')

        return sql, fields, {'leftbounds': previous_max_objectid}

    def cabq_etl(**context):
        ti = context['ti']

        data = ti.xcom_pull(task_ids='cabq-get-sites', key='return_value')
        if data:
            stac = make_sta_client()

            for record in data:
                logging.info(record)

                name = record['loc_name']
                description = 'No Description'
                properties = {}
                lat = record['latitude']
                lon = record['longitude']

                logging.info(f'name={name}, lat={lat},lon={lon}')
                lid = stac.add_location(name, description, properties, latlon=(lat, lon))

                name = 'Water Well'
                description = 'No Description'
                properties = {}
                logging.info(f'Add thing to {lid}')
                stac.add_thing(name, description, properties, lid)
            return record['_airbyte_emitted_at']
        else:
            return get_prev(context, 'cabq-etl')


    cabq_get_sql = PythonOperator(task_id='cabq-get-sql', python_callable=cabq_get_sql)
    cabq_get_sites = BigQueryToXOperator(task_id='cabq-get-sites', sql_task_id='cabq-get-sql')
    cabq_etl = PythonOperator(task_id='cabq-etl', python_callable=cabq_etl)

    cabq_get_sql >> cabq_get_sites >> cabq_etl

# ============= EOF =============================================
