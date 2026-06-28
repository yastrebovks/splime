from dataclasses import dataclass


@dataclass(frozen = True)
class DBase: pass


def mk_dispatcher():
    handlers = []

    def register(p):
        def decorator(f):
            handlers.append((p, f))
            return f
        return decorator

    def dispatch(x, *args, **kwargs):
        for p, f in handlers:
            if p(x):
                return f(x, *args, **kwargs)
        raise ValueError(x)

    dispatch.register = register
    return dispatch
