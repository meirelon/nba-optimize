import os
from math import sqrt
from dateutil import rrule, parser
from itertools import compress

import numpy as np

from sklearn.preprocessing import LabelBinarizer, LabelEncoder, OneHotEncoder, MultiLabelBinarizer, Imputer, RobustScaler
from sklearn.base import BaseEstimator, TransformerMixin
from sklearn.pipeline import Pipeline, FeatureUnion
from sklearn.feature_selection import VarianceThreshold


from sklearn.model_selection import train_test_split, cross_val_score
from sklearn.metrics import mean_squared_error

# import matplotlib.pyplot as plt

#deploying pickle files to gcs
import pickle
from datetime import datetime
from gcloud import storage
from tempfile import NamedTemporaryFile

def generate_date_list(date1, date2):
    return [x.strftime("%Y-%m-%d") for x in list(rrule.rrule(rrule.DAILY,
                             dtstart=parser.parse(date1),
                             until=parser.parse(date2)))]


def model_bakeoff(model, df, dependent_var, test_size, random_state=42):
    X = df.drop([dependent_var], axis=1)
    y = df[dependent_var]

    numerics = X.select_dtypes([np.number]).columns
    categoricals = [x for x in X.columns if x not in  numerics]

    categorical_pipeline = FeatureUnion(categorical_binarizer(categoricals))

    numerical_pipeline = Pipeline([("selector", DataFrameSelector(numerics)),
                                                 ("imputer", Imputer(strategy="median")),
                                                 ("rob_scaler", RobustScaler())])

    complete_pipeline = FeaturePipeline([
        ('join_features', FeatureUnion([
            ('numerical', numerical_pipeline),
            ('categorical', categorical_pipeline)
        ]))
    ])

    X_transformed = complete_pipeline.fit_transform(X)

    X_train, X_test, y_train, y_test = train_test_split(X_transformed, y, test_size=test_size, random_state=random_state)

    model.fit(X=X_train, y=y_train)
    y_hat = model.predict(X_test)
    testing_accuracy = sqrt(mean_squared_error(y_pred=y_hat, y_true=y_test))
    training_accuracy = sqrt(mean_squared_error(y_pred=model.predict(X_train), y_true=y_train))
    is_overfit = abs(testing_accuracy - training_accuracy) > 1

    # plt.scatter(y_hat, y_test)
    # plt.show()

    return({"model":model, "accuracy":testing_accuracy, "overfit":is_overfit})




class DataFrameSelector(BaseEstimator, TransformerMixin):
    def __init__(self, attribute_names):
        self.attribute_names=attribute_names
    def fit(self, X, y=None):
        return self
    def transform(self, X):
        return X[self.attribute_names].values

class FeaturePipeline(Pipeline):
    def get_feature_names(self):
        feature_names = []

        mask = []
        for step_name, step in self.steps:
            if type(step) is LabelBinarizer:
                if step.y_type_ == 'multiclass':
                    feature_names = [f for f in step.classes_]
                if step.y_type == 'binary':
                    feature_names = ['binary']

            if type(step) is DataFrameSelector:
                feature_names = [f for f in step.attribute_names]

            if hasattr(step, 'get_feature_names'):
                feature_names.extent([f for f in step.get_feature_names()])

            if hasattr(step, 'get_support'):
                if len(mask) > 0:
                    mask = mask & step.get_support()
                else:
                    mask = step.get_support()

            if len(mask) > 0:
                feature_names = list(compress(feature_names, mask))
            return feature_names

def categorical_binarizer(categorical_features):
        pipelines = []
        for f in categorical_features:
            pipelines.append((f, Pipeline([("selector", DataFrameSelector(f)),
                                          ("Binarizer", LabelBinarizer())])))
        return(pipelines)


def deploy_pickle(obj, project_id, bucket, destination_path, filename):
    client = storage.Client(project=project_id)
    with NamedTemporaryFile(mode='wb') as temp:
        pickle.dump(obj, temp)
        temp.seek(0)
        gcs_path = os.path.join(destination_path, datetime.today().strftime("%Y%m%d"), '{filename}.pkl'.format(filename=filename))
        client.bucket(bucket).blob(gcs_path).upload_from_filename(temp.name)
