import datetime

import airflow
from airflow.models import DAG
from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators.bigquery_to_gcs import BigQueryToCloudStorageOperator
from airflow.models import Variable
from datetime import timedelta
import re
import json
import requests

gcppj = Variable.get('gcp-pj') # ①Variablesから値を取得
gcpds = Variable.get('gcp-ds')
gcs = Variable.get('gcs')
yesterday = format(datetime.date.today()-timedelta(days=1),'%Y%m%d')
webhook_url = Variable.get('slack_webhook_url')
env = Variable.get('env')

# ②ワークフロー失敗時のアラート通知
def failured(status):
    dag_name = re.findall(r'.*\:\s(.*)\>', str(status['dag']))[0]
    task_name = re.findall(r'.*\:\s(.*)\>', str(status['task']))[0]
    data = {
            'username': 'composer-sample',
            'channel': 'slack-channel',
            'attachments': [{
                'fallback': f'{dag_name}:{task_name}',
                'color': '#e53935',
                'title': f'{dag_name}:{task_name}',
                'text': f'[{env}]{task_name} was failed...'
                }]
            }
    requests.post(webhook_url, json.dumps(data))

default_args = {
    'owner': 'sample-workflow',
    'depends_on_past': True, # ③前のタスクが正常に完了したら次のタスクを実行
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=3),
    'start_date': datetime.datetime(2021, 7, 9)
    'on_failure_callback': failured
}
dag = DAG(
    'sample-workflow',
    schedule_interval='0 4 * * *', # ④UTC時間でスケジュールを設定
    default_args=default_args,
    catchup=False,
)

# テーブル作成
create_table = bigquery_operator.BigQueryOperator(
    task_id='create_table',
    use_legacy_sql=False,
    write_disposition='WRITE_APPEND',
    allow_large_results=True,
    sql='/sql/sample.sql', # ⑤実行するSQLのファイルを指定
    params={'gcppj':gcppj, 'gcpds':gcpds, 'yesterday':yesterday}, # ⑥渡すパラメーターの設定
    destination_dataset_table='{}.{}.analytics_table'.format(gcppj, gcpds),
    execution_timeout=timedelta(minutes=15), # ⑦実行制限時間
    dag=dag
)

# gcs出力
export_csv = BigQueryToCloudStorageOperator(
    task_id='export_csv',
    source_project_dataset_table='{}.{}.analytics_table'.format(gcppj, gcpds),
    destination_cloud_storage_uris='gs://{}/analytics_{}.csv'.format(gcs, yesterday),
    compression=None,
    export_format='CSV',
    field_delimiter=',',
    print_header=True,
    bigquery_conn_id='bigquery_default',
    delegate_to=None,
    labels=None,
    execution_timeout=timedelta(minutes=10),
    dag=dag
    )

# ⑧依存関係定義
create_table >> export_csv