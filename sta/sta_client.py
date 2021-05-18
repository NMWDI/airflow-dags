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

from .definitions import OM_Measurement, FOOT

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


def iotid(iid):
    return {'@iot.id': iid}


class STAClient:
    def __init__(self, host, user, pwd, port):
        self._host = host
        self._user = user
        self._pwd = pwd
        self._port = port

    def add_observed_property(self, name, description, **kw):
        obsprop_id = self.get_observed_property(name)
        if obsprop_id is None:
            payload = {'name': name,
                       'description': description,
                       'definition': 'No Definition',
                       }
            obsprop_id = self._add('ObservedProperties', payload)

        return obsprop_id

    def add_sensor(self, name, description):
        sensor_id = self.get_sensor(name)
        if sensor_id is None:
            payload = {'name': name,
                       'description': description,
                       'encodingType': 'application/pdf',
                       'metadata': 'Not Metadata'
                       }
            sensor_id = self._add('Sensors', payload)

        return sensor_id

    def add_datastream(self, datastream_name, thing_id, obsprop_id, sensor_id,
                       unit=None, otype=None):
        if unit is None:
            unit = FOOT
        if otype is None:
            otype = OM_Measurement

        payload = {'Thing': iotid(thing_id),
                   'ObservedProperty': iotid(obsprop_id),
                   'Sensor': iotid(sensor_id),
                   'unitOfMeasurement': unit,
                   'observationType': otype,
                   'description': 'No Description',
                   'name': datastream_name}

        return self._add('Datastreams', payload)

    def get_sensor(self, name):
        return self._get_id('Sensors', name)

    def get_observed_property(self, name):
        return self._get_id('ObservedProperties', name)

    def get_datastream(self, pointid, thing_name, datastream_name):
        location_id = self._get_id('Locations', pointid)
        if location_id:
            thing_id = self.get_thing_id(thing_name, location_id)
            datastream_id = self.get_datastream_id(datastream_name, thing_id)
            return datastream_id

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
        return f'http://{self._host}:{self._port}/FROST-Server/v1.1/{tag}'

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

    def get_datastream_id(self, name, thing_id):
        tag = 'Datastreams'
        if thing_id:
            tag = f'Things({thing_id})/{tag}'
        return self._get_id(tag, name)

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
                logging.info(f'added {tag} {iotid}')
                return iotid
            else:
                logging.info(f'failed adding {tag} {payload}')
                logging.info(f'Response={resp.json()}')


class STAMQTTClient:
    def __init__(self, host):
        self._client = mqtt.Client('STA')
        self._client.connect(host)

    def add_observations(self, datastream_id, payloads):
        client = self._client
        for payload in payloads:
            client.publish(f'v1.0/Datastreams({datastream_id})/Observations',
                           payload=json.dumps(payload))
# ============= EOF =============================================
