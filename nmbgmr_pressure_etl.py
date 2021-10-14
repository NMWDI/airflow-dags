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

with DAG('NMBGMR_PRESSURE_ETL0.6',
         schedule_interval='@daily',
         max_active_runs=1,
         catchup=False,
         default_args=default_args) as dag:
    def get_sql_pressure(**context):
        tabblename = 'nmbgmr_pressure_level_tbl'
        task_id = 'etl_pressure_levels'
        return get_sql(task_id, tabblename, context)


    gsp = PythonOperator(task_id='get_pressure_sql', python_callable=get_sql_pressure)

    datastream = {'name': GWL_DATASTREAM,
                  'description': GWL_DESCRIPTION,
                  'properties': {'agency': 'NMBGMR'}}

    sensor = {'name': 'Pressure',
              'description': 'Continuous (periodic automated) measurement depth to water in Feet below ground surface '
                             '(converted from pressure reading from depth below ground surface in feet). Not '
                             'Provisional. Quality Controlled'}

    gp = BigQueryETLLevels(WATER_WELL, datastream, sensor, BGS_OBSERVED_PROPERTY,
                           task_id='etl_pressure_levels', sql_task_id='get_pressure_sql')

    gsp >> gp

# ============= EOF =============================================
