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

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from bq_etl_levels import default_args, get_sql, BigQueryETLLevels
from util import GWL_DATASTREAM, WATER_WELL, BGS_OBSERVED_PROPERTY, GWL_DESCRIPTION

with DAG('NMBGMR_ACOUSTIC_ETL0.5',
         schedule_interval='@daily',
         max_active_runs=1,
         catchup=False,
         default_args=default_args) as dag:
    def get_sql_acoustic(**context):
        tabblename = 'nmbgmr_acoustic_level_tbl'
        task_id = 'etl_acoustic_levels'
        return get_sql(task_id, tabblename, context)


    gsa = PythonOperator(task_id='get_acoustic_sql', python_callable=get_sql_acoustic)

    datastream = {'name': GWL_DATASTREAM,
                  'properties': {'agency': 'NMBGMR'},
                  'description': GWL_DESCRIPTION},
    sensor = {'name': 'Acoustic',
              'description': 'Continuous measurement depth to water in Feet below ground surface '
                             '(converted from acoustic device, QC’ed)'}
    ga = BigQueryETLLevels(WATER_WELL, datastream, sensor, BGS_OBSERVED_PROPERTY,
                           task_id='etl_acoustic_levels', sql_task_id='get_acoustic_sql')

    gsa >> ga

# ============= EOF =============================================
