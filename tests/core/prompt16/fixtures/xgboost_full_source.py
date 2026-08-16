import dominate
import numpy
import sklearn.metrics
import sympy
import xgboost


def save_numpy_npy(value: numpy.ndarray) -> bytes:
    import io

    buffer = io.BytesIO()
    numpy.save(buffer, value)
    return buffer.getvalue()


def load_numpy_npy(value: bytes) -> numpy.ndarray:
    import io

    return numpy.load(io.BytesIO(value))


def save_sympy_pickle(value: sympy.core.expr.Expr) -> bytes:
    import pickle

    return pickle.dumps(value)


def load_sympy_pickle(value: bytes) -> sympy.core.expr.Expr:
    import pickle

    return pickle.loads(value)


def save_xgboost_model_ubj(value: xgboost.sklearn.XGBRegressor) -> bytes:
    return value.save_raw(raw_format="ubj")


def load_xgboost_model_ubj(value: bytes) -> xgboost.sklearn.XGBRegressor:
    model = xgboost.XGBRegressor()
    model.load_model(bytearray(value))
    return model


def save_png_bytes(value: bytes) -> bytes:
    return value


def load_png_bytes(value: bytes) -> bytes:
    return value


def mk_random_data(n_features: int, n_samples: int, seed: int) -> numpy.ndarray:
    generator = numpy.random.default_rng(seed)
    return generator.normal(size=(n_samples, n_features))


def mk_random_formula(n_features: int, formula_depth: int, seed: int) -> sympy.core.expr.Expr:
    symbols = sympy.symbols(f"x0:{n_features}")

    def step(value: sympy.core.expr.Expr, index: int) -> sympy.core.expr.Expr:
        return value + symbols[index % n_features] ** 2

    formula = symbols[seed % n_features]
    for index in range(formula_depth):
        formula = step(formula, index)
    return formula


def mk_target(xs: numpy.ndarray, formula: sympy.core.expr.Expr) -> numpy.ndarray:
    symbols = sorted(formula.free_symbols, key=lambda item: item.name)
    target = sympy.lambdify(symbols, formula, "numpy")
    return numpy.asarray([target(*row[: len(symbols)]) for row in xs])


def fit_xgboost_model(xs: numpy.ndarray, ys: numpy.ndarray) -> xgboost.sklearn.XGBRegressor:
    model = xgboost.XGBRegressor(
        n_estimators=20,
        learning_rate=0.2,
        max_depth=3,
        random_state=0,
        n_jobs=1,
    )
    model.fit(xs, ys)
    return model


def run_model(model: xgboost.sklearn.XGBRegressor, xs: numpy.ndarray) -> numpy.ndarray:
    return model.predict(xs)


def mk_metrics(ys: numpy.ndarray, ys_pred: numpy.ndarray) -> dict:
    return {
        "r2": float(sklearn.metrics.r2_score(ys, ys_pred)),
        "rmse": float(sklearn.metrics.root_mean_squared_error(ys, ys_pred)),
    }


def plot_errors(xs: numpy.ndarray, ys: numpy.ndarray, ys_pred: numpy.ndarray) -> bytes:
    import struct
    import zlib

    payload = struct.pack(">I", len(xs)) + bytes(str(float(numpy.mean(ys - ys_pred))), "utf-8")
    return zlib.compress(payload)


def mk_report(metrics: dict, img_errors: bytes) -> str:
    document = dominate.document(title="XGBoost report")
    with document:
        dominate.tags.p(f"R²: {metrics['r2']}")
        dominate.tags.p(f"Plot bytes: {len(img_errors)}")
    return str(document)
