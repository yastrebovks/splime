import lightgbm as lgb
import numpy as np
import pandas as pd
from hyperopt import STATUS_OK, fmin, hp, tpe
from sklearn.model_selection import train_test_split


def prepare_training(df):
    values = np.asarray(df)
    return pd.DataFrame(values)


def prepare_validation(df):
    train, validation = train_test_split(pd.DataFrame(df), test_size=0.2, random_state=7)
    return validation


def train_lgbm(train_df, target):
    model = lgb.LGBMRegressor(n_estimators=20)
    model.fit(train_df, target)
    return model


def hyperopt_objective(params, validation_df, target):
    if params.get("boosting_type") == "gbdt":
        matrix = np.asarray(validation_df)
    else:
        matrix = validation_df
    model = lgb.LGBMRegressor(**params)
    model.fit(matrix, target)
    return {
        "loss": float(np.mean(model.predict(matrix))),
        "status": STATUS_OK,
    }


def run_hyperopt(objective, trials):
    space = {"learning_rate": hp.uniform("learning_rate", 0.01, 0.2)}
    return fmin(
        objective,
        space,
        algo=tpe.suggest,
        trials=trials,
        max_evals=5,
    )
