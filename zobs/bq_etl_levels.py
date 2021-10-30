# # ===============================================================================
# # Copyright 2021 ross
# #
# # Licensed under the Apache License, Version 2.0 (the "License");
# # you may not use this file except in compliance with the License.
# # You may obtain a copy of the License at
# #
# # http://www.apache.org/licenses/LICENSE-2.0
# #
# # Unless required by applicable law or agreed to in writing, software
# # distributed under the License is distributed on an "AS IS" BASIS,
# # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# # See the License for the specific language governing permissions and
# # limitations under the License.
# # ===============================================================================
# import datetime as dt
# import logging
# from itertools import groupby
# from operator import itemgetter
#
# from airflow.models import Variable
#
# from operators.bq import BigQueryToXOperator
# from sta_local.sta_client import make_st_time
# from util import make_sta_client, get_prev
#
# POINTID = 'PointID'
# DATEMEASURED = 'DateMeasured'
# DTW = 'DepthToWaterBGS'
# OBJECTID = 'OBJECTID'
#
#
# default_args = {
#     'owner': 'Jake Ross',
#     'start_date': dt.datetime(2021, 6, 23),
#     'retries': 1,
#     'retry_delay': dt.timedelta(seconds=10),
#     'provide_context': True,
#     'depends_on_past': True,
#     'wait_for_downstream': True
# }
#
#
# class BigQueryETLLevels(BigQueryToXOperator):
#     def __init__(self, thing_name, datastream_args, sensor_args, obsprop_args, *args, **kw):
#         super(BigQueryETLLevels, self).__init__(*args, **kw)
#         self._sensor_args = sensor_args
#         self._thing_name = thing_name
#         self._datastream_args = datastream_args
#         self._observed_property_args = obsprop_args
#
#     def execute(self, context):
#         vs = super(BigQueryETLLevels, self).execute(context)
#         if vs:
#             max_objectid, nt = self._handle(context, vs)
#             logging.info(f'max objectid={max_objectid}')
#             _, _, _, wsql = context['ti'].xcom_pull(task_ids=self.sql_task_id, key='return_value')
#             try:
#                 backfill_iterations = int(Variable.get('backfill_iterations', default_var=10))
#             except ValueError:
#                 backfill_iterations = 10
#
#             for i in range(backfill_iterations):
#                 params = {'leftbounds': max_objectid}
#                 cursor = self._execute_cursor(wsql, params)
#                 records = self._get_records(cursor)
#                 if records:
#                     logging.info(f'nrecords={len(records)}')
#                     nmax_objectid, n = self._handle(context, records)
#                     nt += n
#                     logging.info(f'maxobjectid={max_objectid} new max objectid={nmax_objectid} nt={nt} n={n}')
#                     max_objectid = max(max_objectid, nmax_objectid)
#                 else:
#                     break
#
#             logging.info(f'nrecords added={nt}')
#             return max_objectid
#         else:
#             return get_prev(context, self.task_id)
#
#     def _handle(self, context, data):
#         stac = make_sta_client()
#         thing_name = self._thing_name
#         datastream_name = self._datastream_args['name']
#
#         # key = itemgetter(0)
#         key = itemgetter(POINTID)
#         dkey = itemgetter(OBJECTID)
#
#         # stam = make_stamqtt_client()
#
#         obsprop_id = stac.add_observed_property(*self._observed_property_args)
#         sensor_id = stac.add_sensor(self._sensor_args['name'],
#                                     self._sensor_args['description'])
#         # tcobs = None
#         max_objectid = None
#         n = 0
#         for pointid, records in groupby(sorted(data, key=key), key=key):
#             pointid = pointid.upper()
#             vs = sorted(records, key=dkey)
#             vs = [vi for vi in vs if vi[DTW] is not None]
#             n += len(vs)
#             logging.info(f'setting records for {pointid} n={n}')
#
#             cmax_objectid = max([vi[OBJECTID] for vi in vs])
#             # cmax_objectid = vs[-1][OBJECTID]
#             # logging.info(f'last obs {max_objectid}, current: {cmax_objectid}')
#             if max_objectid is None or cmax_objectid > max_objectid:
#                 max_objectid = cmax_objectid
#
#             # get the data stream for this pointid.
#             # pointid give us Location. assume Thing.name eq 'Water Well'
#             # assume observed
#             datastream_id = stac.get_datastream(pointid, thing_name, datastream_name, self._sensor_args['name'])
#             if not datastream_id:
#                 # get location
#                 location_id = stac.get_location_id(pointid)
#                 if location_id:
#                     # get thing
#                     thing_id = stac.get_thing_id(thing_name, location_id)
#                     if not thing_id:
#                         logging.info(
#                             'Location/Thing does not exist {}/{}. skipping datastream'.format(pointid, thing_name))
#
#                         continue
#
#                 else:
#                     logging.info('Location does not exist {}'.format(pointid))
#                     continue
#
#                 datastream_id = stac.add_datastream(self._datastream_args, thing_id, obsprop_id, sensor_id)
#
#             if datastream_id:
#                 if vs:
#
#                     components = ['phenomenonTime', 'resultTime', 'result']
#                     obs = [(make_st_time(vi[DATEMEASURED]), make_st_time(vi[DATEMEASURED]), float(vi[DTW])) for vi in vs]
#                     stac.add_observations(datastream_id, components, obs)
#
#         if max_objectid is not None:
#             return max_objectid, n
#         else:
#             return get_prev(context, self.task_id), n
#
#
# def get_sql(task_id, table_name, context):
#     fields = [POINTID, OBJECTID, DATEMEASURED, DTW]
#     dataset = Variable.get('bq_levels')
#     table_name = Variable.get(table_name)
#     fs = ','.join(fields)
#     sql = f'''select {fs} from {dataset}.{table_name} where {DTW} is not NULL '''
#
#     wsql = f'{sql} and {OBJECTID}>%(leftbounds)s'
#     previous_max_objectid = get_prev(context, task_id)
#     if previous_max_objectid:
#         sql = f'{sql} and {OBJECTID}>%(leftbounds)s'
#
#     wsql = f'{wsql} order by {OBJECTID} asc LIMIT 200'
#     sql = f'{sql} order by {OBJECTID} asc LIMIT 200'
#     return sql, fields, {'leftbounds': previous_max_objectid}, wsql
#
#
# # ============= EOF =============================================
