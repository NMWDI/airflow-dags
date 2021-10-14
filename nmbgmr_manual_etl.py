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

with DAG('NMBGMR_MANUAL_ETL0.7',
         # schedule_interval='*/10 * * * *',
         schedule_interval='@daily',
         max_active_runs=1,
         catchup=False,
         default_args=default_args) as dag:
    def get_sql_manual(**context):
        tabblename = 'nmbgmr_manual_level_tbl'
        task_id = 'etl_manual_levels'
        return get_sql(task_id, tabblename, context)


    gsm = PythonOperator(task_id='get_manual_sql', python_callable=get_sql_manual)
    gm = BigQueryETLLevels('Water Well',
                           ('Ground Water Levels', {'agency': 'NMBGMR'}),
                           ('Manual', 'Manual measurement of groundwater depth by field technician'),
                           ('Depth to Water Below Land Surface', 'depth to water below land surface'),
                           task_id='etl_manual_levels', sql_task_id='get_manual_sql')
    gsm >> gm

# ============= EOF =============================================
