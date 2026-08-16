import xgboost as xgb


def fit_xgboost_model(rows: list) -> dict:
    import struct

    def objective(row):
        return float(row["target"])

    weights = list(map(lambda row: objective(row), rows))
    model = xgb.XGBRegressor(
        n_estimators=10,
        max_depth=2,
        learning_rate=0.5,
        random_state=0,
        n_jobs=1,
    )
    return {
        "count": len(weights),
        "header": struct.pack(">I", len(weights)).hex(),
        "model": str(model),
    }
