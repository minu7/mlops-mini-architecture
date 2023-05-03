FROM apache/airflow:2.5.1-python3.8
RUN python -m pip install --upgrade pip
RUN pip install feast
RUN pip install -U scikit-learn
RUN pip install mlflow
ADD airflow/dags /opt/airflow/dags
ADD feast /opt/airflow/feast