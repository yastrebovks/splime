from dataclasses import dataclass
from typing import Any, Callable, Protocol, TypeVar


@dataclass(frozen=True)
class DBase:
    pass


_F = TypeVar("_F", bound=Callable[..., Any])


class Dispatcher(Protocol):
    def register(self, p: Callable[[Any], bool]) -> Callable[[_F], _F]: ...

    def __call__(self, x: Any, *args: Any, **kwargs: Any) -> Any: ...


class NamedDBase(Protocol):
    name: str


class _Dispatcher:
    def __init__(self) -> None:
        self._handlers: list[tuple[Callable[[Any], bool], Callable[..., Any]]] = []

    def register(self, p: Callable[[Any], bool]) -> Callable[[_F], _F]:
        def decorator(f: _F) -> _F:
            self._handlers.append((p, f))
            return f

        return decorator

    def __call__(self, x: Any, *args: Any, **kwargs: Any) -> Any:
        for p, f in self._handlers:
            if p(x):
                return f(x, *args, **kwargs)
        raise ValueError(x)


def mk_dispatcher() -> Dispatcher:
    return _Dispatcher()
