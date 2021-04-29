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
from datetime import datetime
import paho.mqtt.client as mqtt
import json

import requests


def make_st_time(ts):
    for fmt in ('%Y-%m-%d',):
        try:
            t = datetime.strptime(ts, fmt)
            return f'{t.isoformat()}.000Z'
        except BaseException:
            pass


class STAClient:
    def __init__(self, host):
        self._host = host

    def get_last_observation(self, datastream_id):
        url = self._make_url(f'Datastreams({datastream_id})/Observations?$orderby=phenomenonTime desc&$top=1')
        logging.info(f'request url: {url}')
        resp = requests.get(url)
        v = resp.json()
        logging.info(f'v {v}')

        vs = v.get('value')
        logging.info(f'vs {vs}')
        if vs:
            return vs[0].get('phenomenonTime')

    def _make_url(self, tag):
        return f'http://{self._host}:8090/FROST-Server/v1.1/{tag}'


class STAMQTTClient:
    def __init__(self, host):
        self._client = mqtt.Client('STA')
        self._client.connect(host)

    def add_observations(self, d):
        datastream_id = d['datastream_id']
        records = d['records']

        client = self._client

        for r in records:
            if r['dtw']:
                payload = {'result': float(r['dtw']),
                           'phenomenonTime': make_st_time(r['datemeasured'])}
                client.publish(f'v1.0/Datastreams({datastream_id})/Observations',
                               payload=json.dumps(payload))
# ============= EOF =============================================
