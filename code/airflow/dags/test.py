import pendulum
import os
import pandas as pd
import json
import requests
from datetime import datetime
import boto3
from io import BytesIO, StringIO
import codecs

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.decorators import task, dag

from utils import MAPPING_NAMES, TRANSFORM_FUNCTIONS, get_best_model



@dag("test", schedule="*/5 * * * *", catchup=False, start_date=datetime(2023, 1, 1))
def taskflow():
    s3 = boto3.resource('s3', 
            endpoint_url='http://minio:9000',
            aws_access_key_id='user',
            aws_secret_access_key='password',
            aws_session_token=None,
            verify=False)

    s3_sensor = S3KeySensor(
        task_id='wait_for_test_file',
        poke_interval=60,
        timeout=180,
        soft_fail=False,
        retries=2,
        bucket_key='test_auto.csv',
        bucket_name='test',
        aws_conn_id='aws_default')

    @task(task_id="extract", retries=2)
    def extract():
        obj = s3.Object(bucket_name='test', key='test_auto.csv').get()
        csvstring = obj['Body'].read().decode("utf-8")
        s3.Object(bucket_name='test', key='test_auto.csv').delete()
        return csvstring

    @task(task_id="predict", retries=2)
    def tranform_and_predict(csv):
        df = pd.read_csv(StringIO(csv))
        drop_columns = ['label', 'amt']
        needed_functions = {k: TRANSFORM_FUNCTIONS[k] for k in TRANSFORM_FUNCTIONS.keys() - set(drop_columns)}
        df = df.rename(columns=MAPPING_NAMES).transform(needed_functions)
       
        model = get_best_model('drivers')
        results = model.predict(df)
        df['predicted'] = results

        # a little verbose to upload a file....
        bio = BytesIO()
        StreamWriter = codecs.getwriter('utf-8')
        s = StreamWriter(bio)
        df.to_csv(s, index=False)
        bio.seek(0)
        obj = s3.Object(bucket_name='results', key='result.csv').put(Body=bio)
        


    extracted = extract()
    predicted = tranform_and_predict(extracted)
    s3_sensor >> extracted >> predicted

dag = taskflow()
