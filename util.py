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

from airflow.hooks.base_hook import BaseHook
from airflow.models import TaskInstance

from sta.sta_client import STAClient, STAMQTTClient


def make_stamqtt_client():
    connection = BaseHook.get_connection('nmbgmr_sta_conn_id')
    staclient = STAMQTTClient(connection.host)
    return staclient


def make_sta_client(sta_key='nmbgmr_sta_conn_id'):
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
GWL_DESCRIPTION = 'Measurement of groundwater depth in a water well, as measured below ground surface'
WATER_WELL = 'Water Well'
BGS_OBSERVED_PROPERTY = ('Depth to Water Below Ground Surface', 'depth to water below ground surface')
# ============= EOF =============================================
