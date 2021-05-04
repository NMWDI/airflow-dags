from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.bigquery_hook import BigQueryHook


class BigQueryToXOperator(BaseOperator):
    template_fields = ['sql']
    ui_color = '#99ffcc'

    @apply_defaults
    def __init__(
            self,
            sql=None,
            keys=None,
            bigquery_conn_id='bigquery_default',
            delegate_to=None,
            *args,
            **kwargs):
        super(BigQueryToXOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.keys = keys  # A list of keys for the columns in the result set of sql
        self.bigquery_conn_id = bigquery_conn_id
        self.delegate_to = delegate_to

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
        return [dict(zip(keys, row)) for row in cursor.fetchall()]

    def _query_bigquery(self, ti):
        """
        Queries BigQuery and returns a cursor to the results.
        """
        bq = BigQueryHook(bigquery_conn_id=self.bigquery_conn_id,
                          use_legacy_sql=False)
        conn = bq.get_conn()
        cursor = conn.cursor()
        sql, keys = self.sql, self.keys
        if sql is None:
            sql, keys = ti.xcom_pull(task_ids='get-sql', key='return_value')
        cursor.execute(sql)
        return cursor, keys
