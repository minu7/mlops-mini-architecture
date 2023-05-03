import pendulum
import os
import pandas as pd
import json
import requests
from datetime import datetime
import boto3
from io import StringIO


from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow import DAG
from airflow.decorators import task, dag
from airflow import settings
from airflow.models import Connection

from utils import MAPPING_NAMES, TRANSFORM_FUNCTIONS

from sqlalchemy import create_engine


@dag("ingest", schedule="*/5 * * * *", catchup=False, start_date=datetime(2023, 1, 1))
def taskflow():
    s3_sensor = S3KeySensor(
        task_id='wait_for_train_file',
        poke_interval=60,
        timeout=180,
        soft_fail=False,
        retries=2,
        bucket_key='train_auto.csv',
        bucket_name='train',
        aws_conn_id='aws_default')

    @task(task_id="extract", retries=2)
    def extract():
        s3 = boto3.resource('s3', 
            endpoint_url='http://minio:9000',
            aws_access_key_id='user',
            aws_secret_access_key='password',
            aws_session_token=None,
            verify=False)
        obj = s3.Object(bucket_name='train', key='train_auto.csv').get()
        csvstring = obj['Body'].read().decode("utf-8")
        s3.Object(bucket_name='train', key='train_auto.csv').delete()
        return csvstring

    @task(task_id="transform", retries=2)
    def tranform_and_load(csv):
        # if we expect really large file: arrow, spark, ..
        df = pd.read_csv(StringIO(csv))
        df = df.rename(columns=MAPPING_NAMES).transform(TRANSFORM_FUNCTIONS)
        engine = create_engine('postgresql://user:password@postgres:5432/feast')
        df.to_sql('drivers', engine, if_exists='append', index=False)

    extracted = extract()
    to_load = tranform_and_load(extracted)
    s3_sensor >> extracted >> to_load

dag = taskflow()
