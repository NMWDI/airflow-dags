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
import pyproj
import requests
import re

projections = {}

IDREGEX = re.compile(r'(?P<id>\(\d+\))')


def make_st_time(ts):
    for fmt in ('%Y-%m-%d',):
        try:
            t = datetime.strptime(ts, fmt)
            return f'{t.isoformat()}.000Z'
        except BaseException:
            pass


def make_geometry_point_from_utm(e, n, zone=None, ellps=None, srid=None):
    if zone:
        if zone in projections:
            p = projections[zone]
        else:
            if ellps is None:
                ellps = 'WGS84'
            p = pyproj.Proj(proj='utm', zone=int(zone), ellps=ellps)
            projections[zone] = p
    elif srid:
        # get zone
        if srid in projections:
            p = projections[srid]
            projections[srid] = p
        else:
            # p = pyproj.Proj(proj='utm', zone=int(zone), ellps='WGS84')
            p = pyproj.Proj('EPSG:{}'.format(srid))

    lon, lat = p(e, n, inverse=True)
    return make_geometry_point_from_latlon(lat, lon)


def make_geometry_point_from_latlon(lat, lon):
    return {'type': 'Point', 'coordinates': [lon, lat]}


class STAClient:
    def __init__(self, host, user, pwd):
        self._host = host
        self._user = user
        self._pwd = pwd

    def get_last_thing(self):
        pass

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

    def add_location(self, name, description, properties, utm=None, latlon=None):
        lid = self.get_location_id(name)
        if lid is None:

            geometry = None
            if utm:
                geometry = make_geometry_point_from_utm(*utm)
            elif latlon:
                geometry = make_geometry_point_from_latlon(*latlon)

            if geometry:
                payload = {'name': name,
                           'description': description,
                           'properties': properties,
                           'location': geometry,
                           'encodingType': 'application/vnd.geo+json'
                           }
                return self._add('Locations', payload)
            else:
                logging.info('failed to construct geometry. need to specify utm or latlon')
                raise Exception
        else:
            return lid

    def add_thing(self, name, description, properties, location_id):
        tid = self.get_thing_id(name, location_id)
        if tid is None:
            payload = {'name': name,
                       'description': description,
                       # 'properties': properties,
                       'Locations': [{'@iot.id': location_id}]}
            return self._add('Things', payload)

        return tid

    def get_location_id(self, name):
        return self._get_id('Locations', name)

    def get_thing_id(self, name, location_id=None):
        tag = 'Things'
        if location_id:
            tag = f'Locations({location_id})/{tag}'

        return self._get_id(tag, name)

    def _get_id(self, tag, name):
        vs = self._get_item_by_name(tag, name)
        if vs:
            iotid = vs[0]['@iot.id']
            logging.info(f'Got tag={tag} name={name} iotid={iotid}')
            return iotid

    def _get_item_by_name(self, tag, name):
        tag = f"{tag}?$filter=name eq '{name}'"
        url = self._make_url(tag)
        resp = requests.get(url, auth=('read', 'read'))
        logging.info(f'Get item {tag} name={name}')
        return resp.json()['value']

    def _add(self, tag, payload, extract_iotid=True):
        url = self._make_url(tag)
        logging.info(f'Add url={url}')
        logging.info(f'Add payload={payload}')

        resp = requests.post(url,
                             # auth=(self._user, self._pwd),
                             json=payload)

        if extract_iotid:
            m = IDREGEX.search(resp.headers.get('location', ''))
            # logging.info(f'Response={resp.json()}')

            if m:
                iotid = m.group('id')[1:-1]
            # else:
            # iotid = resp.json()['@iot.id']

            logging.info(f'added {tag} {iotid}')
            return iotid


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
