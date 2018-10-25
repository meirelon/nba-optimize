from math import sqrt
from dateutil import rrule, parser

from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error

import matplotlib.pyplot as plt

def generate_date_list(date1, date2):
    return [x.strftime("%Y-%m-%d") for x in list(rrule.rrule(rrule.DAILY,
                             dtstart=parser.parse(date1),
                             until=parser.parse(date2)))]


def model_bakeoff(model, df, dependent_var, test_size, random_state=42):
    X = df.drop([dependent_var], axis=1)
    y = df[dependent_var]
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=test_size, random_state=random_state)

    model.fit(X=X_train, y=y_train)
    y_hat = model.predict(X_test)
    testing_accuracy = sqrt(mean_squared_error(y_pred=y_hat, y_true=y_test))
    training_accuracy = sqrt(mean_squared_error(y_pred=model.predict(X_train), y_true=y_train))
    is_overfit = abs(testing_accuracy - training_accuracy) > 1

    plt.scatter(y_hat, y_test)
    plt.show()

    return({"model":model, "accuracy":testing_accuracy, "overfit":is_overfit})
