from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OrdinalEncoder

def create_model():
    class PreprocessDF():
        def __init__(self):
            self.encoders = {
                'sex': OrdinalEncoder(),
                'job': OrdinalEncoder(),
                'car_type': OrdinalEncoder()
            }
            # ensure the order and needed columns
            self.needed_columns = ['kids_driv', 'age', 'home_kids', 'yoj', 'income',
                'parent1', 'home_val', 'm_status', 'sex', 'education_level', 'job',
                'trav_time', 'commercial_car_use', 'blue_book', 'tif', 'car_type',
                'red_car', 'old_claim', 'clm_freq', 'revoced', 'mvr_pts', 'car_age',
                'urban_city']
    
        def fit(self, df, y = None):
            for column in df.columns:
                if column in self.encoders:
                    self.encoders[column].fit(df[[column]])
            return self

        def transform(self, input_df, y = None):
            df = input_df.copy() # creating a copy to avoid changes to original dataset
            for column in self.encoders:
                df[column] = self.encoders[column].transform(df[[column]])
            return df[self.needed_columns].astype('float32')
    # it guarantees that model and preprocessing needed are always togheter
    model = Pipeline(steps=[
            ('preprocess', PreprocessDF()),
            ('classifier', RandomForestClassifier())
        ])
    # search_params = {'classifier__criterion':['gini','entropy'], 'classifier__max_depth':[1, 4, 10, 30, 50, 80, 100, 200, 300], 'classifier__n_estimators': [1, 4, 10, 30, 50, 80, 100, 200, 300]}
    search_params = {'classifier__criterion':['gini','entropy'], 'classifier__max_depth':[10, 50, 100], 'classifier__n_estimators': [10, 80]}
    # best model with f1, other metrics are only monitored
    clf = GridSearchCV(model, search_params, scoring=['f1', 'accuracy', 'balanced_accuracy', 'precision', 'recall', 'roc_auc'], refit='f1', cv=5)
    return clf