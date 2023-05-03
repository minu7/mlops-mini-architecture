import pendulum
import os
import pandas as pd
import json
import requests
from datetime import datetime
import boto3
from io import StringIO
from sqlalchemy import create_engine
from feast import FeatureStore

import mlflow
import tempfile
import warnings

from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow import DAG
from airflow.decorators import task, dag
from airflow import settings
from airflow.models import Connection
from feast.infra.offline_stores.contrib.postgres_offline_store.postgres import (
    PostgreSQLOfflineStore
)

from utils import create_model

@dag("train", catchup=False, start_date=datetime(2023, 1, 1))
def taskflow():

    @task(task_id="train", retries=2)
    def train():
        mlflow.set_tracking_uri("http://mlflow:5000")
        mlflow.sklearn.autolog()

        fs = FeatureStore(repo_path="/opt/airflow/feast/drivers/feature_repo")
        training_df = PostgreSQLOfflineStore.pull_latest_from_table_or_query(
            config=fs.config,
            data_source=fs.get_data_source('drivers_source'),
            join_key_columns=[fs.get_entity('driver').join_key],
            feature_name_columns=[f.name for f in fs.get_feature_view('drivers').features],
            timestamp_field=fs.get_data_source('drivers_source').timestamp_field,
            created_timestamp_column=None,
            start_date=datetime(2023, 1, 1),
            end_date=datetime.now(),
        ).to_df()
        print(len(training_df))

        clf = create_model()

        experiment_name = 'drivers'
        existing_exp = mlflow.get_experiment_by_name(experiment_name)
        if not existing_exp:
            experiment_id = mlflow.create_experiment(experiment_name)
        else:
            experiment_id = existing_exp.experiment_id
        
        timestamp = datetime.now().isoformat().split(".")[0].replace(":", ".")
        with mlflow.start_run(experiment_id=experiment_id, run_name=timestamp) as run:
            clf.fit(training_df, training_df['label'])
            cv_results = clf.cv_results_
            best_index = clf.best_index_
            for score_name in [score for score in cv_results if "mean_test" in score]:
                mlflow.log_metric(score_name, cv_results[score_name][best_index])
                mlflow.log_metric(score_name.replace("mean","std"), cv_results[score_name.replace("mean","std")][best_index])

            tempdir = tempfile.TemporaryDirectory().name
            os.mkdir(tempdir)
            filename = "%s-%s-cv_results.csv" % ('RandomForest', timestamp)
            csv = os.path.join(tempdir, filename)
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                pd.DataFrame(cv_results).to_csv(csv, index=False)
            
            mlflow.log_artifact(csv, "cv_results")

            # MLflow already log best estimator, it is needed if we want different env
            # mlflow.sklearn.log_model(clf.best_estimator_, 'RandomForest', conda_env={
			# 	'name': 'mlflow-env',
			# 	'channels': ['defaults'],
			# 	'dependencies': [
			# 		'python=3.8.10', {'pip': ['scikit-learn==1.2.1','pandas==1.5.3']}
			# 	]
            # })

    train()
dag = taskflow()
