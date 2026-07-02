from dataclasses import dataclass
from typing import Any, Callable, Protocol, TypeVar, cast


@dataclass(frozen = True)
class DBase: pass


_F = TypeVar('_F', bound = Callable[..., Any])


class Dispatcher(Protocol):
    def register(self, p: Callable[[Any], bool]) -> Callable[[_F], _F]: ...

    def __call__(self, x: Any, *args: Any, **kwargs: Any) -> Any: ...


def mk_dispatcher() -> Dispatcher:
    handlers: list[tuple[Callable[[Any], bool], Callable[..., Any]]] = []

    def register(p: Callable[[Any], bool]) -> Callable[[_F], _F]:
        def decorator(f: _F) -> _F:
            handlers.append((p, f))
            return f
        return decorator

    def dispatch(x: Any, *args: Any, **kwargs: Any) -> Any:
        for p, f in handlers:
            if p(x):
                return f(x, *args, **kwargs)
        raise ValueError(x)

    dispatch.register = register
    return cast(Dispatcher, dispatch)
