import pandas as pd
import mlflow
from mlflow.entities import ViewType

def get_best_model(experiment_name, metric = 'mean_test_f1'):
    mlflow.set_tracking_uri("http://mlflow:5000")
    df = mlflow.search_runs(experiment_names=[experiment_name], run_view_type=ViewType.ACTIVE_ONLY)
    df = df.sort_values(by=[f'metrics.{metric}'], ascending=False)
    for _, row in df.iterrows(): # there are a lot of anonymous experiment created by autologging (probably), i must investigate on this
        try:
            model = mlflow.sklearn.load_model("runs:/" + row['run_id'] + "/best_estimator")
            if model is not None:
                print(f"loaded model = {row['run_id']}")
                break
        except:
            pass # not the best solution for sure..., TODO: rewrite this

    return model