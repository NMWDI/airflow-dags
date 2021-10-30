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
import datetime

from airflow import DAG
from airflow.models import Variable

from dags.zobs.bq_etl_levels import default_args
from operators.bq import BigQueryBaseOperator
from util import get_prev, make_sta_client

FMT = '%Y/%m/%d %H:%M:%S+00'


def as_int(v):
    try:
        return int(v)
    except (ValueError, TypeError):
        return v


def as_float(v):
    try:
        return float(v)
    except (ValueError, TypeError):
        return v


def clean(props):
    for k, v in props.items():
        if isinstance(v, str):
            props[k] = ds = v.strip()

            try:
                dt = datetime.datetime.strptime(ds, FMT)
                props[k] = dt.isoformat()
            except ValueError:
                pass

    return props


class PODSETL(BigQueryBaseOperator):
    def _handle_execute(self, context):
        dataset = Variable.get('bq_pods')
        table_name = Variable.get('ose_pods_tbl')

        fields = ['pod_basin',
                  'pod_nbr',
                  'pod_suffix',
                  'ref',
                  'pod_name',
                  'tws',
                  'rng',
                  'sec',
                  'qtr_4th',
                  'qtr_16th',
                  'qtr_64th',
                  'qtr_256th',
                  'qtr_1024th',
                  'qtr_4096',
                  'blk',
                  'zone',
                  'x',
                  'y',
                  'grant',
                  'legal',
                  'county',
                  'license_nbr',
                  'driller',
                  'start_date',
                  'finish_date',
                  'plug_date',
                  'pcw_rcv_date',
                  'elevation',
                  'depth_well',
                  'grnd_wtr_src',
                  'percent_shallow',
                  'depth_water',
                  'log_file_date',
                  'sched_date',
                  'usgs_map_code',
                  'usgs_map_suffix',
                  'usgs_map_quad1',
                  'usgs_map_quad2',
                  'use_of_well',
                  'pump_type',
                  'pump_serial',
                  'discharge',
                  'photo',
                  'photo_date',
                  'photo_punch',
                  'aquifer',
                  'sys_date',
                  'measure',
                  'subdiv_name',
                  'subdiv_location',
                  'municipality',
                  'municipality_loc',
                  'restrict',
                  'usgs_pod_nbr',
                  'lat_deg',
                  'lat_min',
                  'lat_sec',
                  'lon_deg',
                  'lon_min',
                  'lon_sec',
                  'surface_code',
                  'estimate_yield',
                  'pod_status',
                  'casing_size',
                  'ditch_name',
                  'utm_zone',
                  'easting',
                  'northing',
                  'datum',
                  'utm_source',
                  'utm_accuracy',
                  'xy_source',
                  'xy_accuracy',
                  'lat_lon_source',
                  'lat_lon_accuracy',
                  'tract_nbr',
                  'map_nbr',
                  'surv_map',
                  'other_loc',
                  'pod_rec_nbr',
                  'cfs_start_mday',
                  'cfs_end_mday',
                  'cfs_cnv_factor',
                  'cs_code',
                  'wrats_s_id',
                  'utm_error',
                  'pod_sub_basin',
                  'well_tag',
                  'static_level',
                  ]

        joins = [('ps.description', 'pod_status_description', 'ose_codes.pod_status as ps on ps.code=pod_status'),
                 ('gws.description', 'groundwater_source_description', 'ose_codes.gw_source as gws on '
                                                                       'gws.code=grnd_wtr_src'),
                 ('ss.description', 'surface_source_description', 'ose_codes.surface_source as ss on '
                                                                  'CAST(ss.code as STRING)=surface_code'),
                 ('c.description', 'county_description', 'ose_codes.counties as c on c.code=county'),
                 # ('acc.description', 'utm_accuracy_description', 'ose_codes.accuracy as acc on acc.code=utm_accuracy'),
                 ('src.description', 'utm_source_description', 'ose_codes.source as src on src.code=utm_source'),

                 ]

        joinfields, joinkeys, joinsql = zip(*joins)
        joinfields = [f'TRIM({f})' for f in joinfields]

        keys = fields[:]
        fields.extend(joinfields)
        keys.extend(joinkeys)

        joinsql = [f'left join {s}' for s in joinsql]

        joinsql = '\n'.join(joinsql)

        fs = ','.join(fields)
        base_sql = f'''select {fs} 
        from {dataset}.{table_name}
        {joinsql}'''

        previous_max_objectid = get_prev(context, 'ose_pods-etl')

        params = {}
        if previous_max_objectid:
            params = {'leftbounds': previous_max_objectid}
            sql = f'{base_sql} where pod_rec_nbr is not NULL and easting is not NULL and northing is ' \
                  f'not NULL and CAST(pod_rec_nbr as INTEGER)>%(leftbounds)s'
        else:
            sql = f'{base_sql} where pod_rec_nbr is not NULL and easting is not NULL and northing is ' \
                  f'not NULL'

        nbackfill_iterations = 20
        stac = make_sta_client('ose_sta_conn_id')
        for backfill in range(nbackfill_iterations):
            cursor = self._get_cursor()
            s = f'{sql} order by CAST(pod_rec_nbr as INTEGER) limit 100'

            cursor.execute(s, parameters=params)
            logging.info(f'sql={s}')
            logging.info(f'parameters={params}')

            vs = sorted([dict(zip(keys, row)) for row in cursor.fetchall()], key=lambda x: int(x['pod_rec_nbr']))
            cursor.close()
            sql = f'{base_sql} where pod_rec_nbr is not NULL and easting is not NULL and northing is ' \
                  f'not NULL and CAST(pod_rec_nbr as INTEGER)>%(leftbounds)s'

            logging.info(f'backfill iteration {backfill}. nrecords={len(vs) if vs else 0}')
            if vs:
                for vi in vs:
                    # logging.info(vi)
                    self._handle_pod(stac, vi)

                previous_max_objectid = int(vs[-1]['pod_rec_nbr'])
                params = {'leftbounds': previous_max_objectid}
                logging.info(f'previous max objectid={previous_max_objectid}')
            else:
                previous_max_objectid = None
                break

        return previous_max_objectid

    def _handle_pod(self, stac, pod):
        if pod['pod_basin'] in ('SP', 'SD'):
            return

        # name = '{}.{}'.format(pod['pod_basin'], int(pod['pod_nbr']))
        # suffix = pod['pod_suffix'].strip()
        # if suffix:
        #     name = '{}.{}'.format(name, suffix)
        name = f'POD-{pod["pod_rec_nbr"]}'

        utm = (pod['easting'], pod['northing'], pod['utm_zone'])
        description = 'Location of an OSE POD'
        properties = {'elevation': as_float(pod['elevation']),
                      'county_code': pod['county'],
                      'county': pod['county_description'],
                      'legal': pod['legal'],
                      'grant': pod['grant'],
                      'block': pod['blk'],
                      'nm_plane_zone': pod['zone'],
                      'plss_township_id': pod['tws'],
                      'plss_range_id': pod['rng'],
                      'plss_sec': as_int(pod['sec']),
                      'plss_qtr_4th': as_int(pod['qtr_4th']),
                      'plss_qtr_16th': as_int(pod['qtr_16th']),
                      'plss_qtr_64th': as_int(pod['qtr_64th']),
                      'plss_qtr_1024th': as_int(pod['qtr_1024th']),
                      'plss_qtr_4096th': as_int(pod['qtr_4096']),
                      'subdivision': pod['subdiv_name'],
                      'subdivision_location': pod['subdiv_location'],
                      'municipality': pod['municipality'],
                      'pod_unique_id': int(pod['pod_rec_nbr']),
                      'utm_source': pod['utm_source'],
                      'utm_source_description': pod['utm_source_description'],
                      'utm_accuracy': pod['utm_accuracy'],
                      'tract': pod['tract_nbr'],
                      'map': pod['map_nbr'],
                      'surv_map': pod['surv_map'],
                      'other': pod['other_loc'],
                      'pod_sub_basin': pod['pod_sub_basin'],
                      'ditch_name': pod['ditch_name'],
                      'pod_name': pod['pod_name']
                      # 'utm_accuracy_description': pod['utm_accuracy_description']
                      }

        properties = clean(properties)
        location_id, added = stac.add_location(name, description, properties, utm=utm)
        if location_id is None:
            logging.info('failed adding location name={}'.format(name))
            return

        if not added:
            logging.info('location already exists name={}, id={}'.format(name, location_id))
            stac.patch_location(location_id, {'properties': properties})

        name = pod['pod_name'] or 'POD'
        description = 'OSE Groundwater Point of Diversion'
        properties = {'use_of_well': pod['use_of_well'],
                      'casing_size': as_float(pod['casing_size']),
                      'estimate_yield': as_float(pod['estimate_yield']),
                      'groundwater_source': pod['groundwater_source_description'],
                      'groundwater_source_code': pod['grnd_wtr_src'],
                      'surface_source_code': pod['surface_code'],
                      'surface_water_source': pod['surface_source_description'],
                      'pod_status_code': pod['pod_status'],
                      'pod_status': pod['pod_status_description'],
                      'well_drill_start_date': pod['start_date'],
                      'well_drill_finish_date': pod['finish_date'],
                      'well_plug_date': pod['plug_date'],
                      'well_proof_of_completion_date': pod['pcw_rcv_date'],
                      'well_depth': as_float(pod['depth_well']),
                      'percent_shallow': as_float(pod['percent_shallow']),
                      'pump_type': pod['pump_type'],
                      'aquifer': pod['aquifer'],
                      'well_tag': pod['well_tag'],
                      'static_level': as_float(pod['static_level']),
                      'driller_license': as_int(pod['license_nbr']),
                      'driller': pod['driller'],
                      'pod_unique_id': int(pod['pod_rec_nbr']),
                      'pod_basin': pod['pod_basin'],
                      'pod_number': as_int(pod['pod_nbr']),
                      'pod_suffix': pod['pod_suffix'],
                      'ref': pod['ref'],
                      'water_depth': as_float(pod['depth_water']),
                      'discharge': as_float(pod['discharge']),
                      'pod_name': pod['pod_name']
                      }

        stac.add_thing(name, description, clean(properties), location_id, check=not added)


with DAG('OSEPODS0.1',
         # schedule_interval=None,
         schedule_interval='*/10 * * * *',
         catchup=False,
         default_args=default_args) as dag:
    osepods_etl = PODSETL(task_id='ose_pods-etl')

# ============= EOF =============================================
