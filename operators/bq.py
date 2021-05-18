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

from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.bigquery_hook import BigQueryHook


class BigQueryToXOperator(BaseOperator):
    template_fields = ['sql']
    ui_color = '#99ffcc'

    @apply_defaults
    def __init__(self, sql=None, keys=None, sql_task_id=None,
                 parameters=None, handler=None,
                 bigquery_conn_id='bigquery_default',
                 delegate_to=None,
                 *args, **kw):
        super(BigQueryToXOperator, self).__init__(*args, **kw)
        self.sql = sql
        self.keys = keys
        self.parameters = parameters
        self.sql_task_id = sql_task_id
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to
        self.handler = handler

    def execute(self, context):
        """
        Run query and handle results row by row.
        """
        cursor, keys = self._query_bigquery(context['ti'])
        # for row in cursor.fetchall():
        #     # Zip keys and row together because the cursor returns a list of list (not list of dicts)
        #     row_dict = dumps(dict(zip(self.keys,row))).encode('utf-8')

        #     # Do what you want with the row...
        #     handle_row(row_dict)
        vs = [dict(zip(keys, row)) for row in cursor.fetchall()]
        if self.handler:
            vs = self.handler(vs)
        return vs

    def _query_bigquery(self, ti):
        """
        Queries BigQuery and returns a cursor to the results.
        """
        bq = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                          use_legacy_sql=False)
        conn = bq.get_conn()
        cursor = conn.cursor()
        sql, keys, params = self.sql, self.keys, self.parameters
        if sql is None:
            sql, keys, params = ti.xcom_pull(task_ids=self.sql_task_id, key='return_value')

        cursor.execute(sql, parameters=params)
        return cursor, keys


# ============= EOF =============================================
