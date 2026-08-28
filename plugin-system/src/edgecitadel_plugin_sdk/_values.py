"""Private immutable containers for portable SDK values."""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Never, cast


class _FrozenDict(dict[str, object]):
    def __init__(
        self,
        values: Mapping[str, object] | Iterable[tuple[str, object]] = (),
    ) -> None:
        items = values.items() if isinstance(values, Mapping) else values
        dict.__init__(self, ((key, _freeze(value)) for key, value in items))

    def _immutable(self, *args: object, **kwargs: object) -> Never:
        raise TypeError("SDK value mappings are immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable
    __ior__ = _immutable


def _freeze(value: object) -> object:
    if isinstance(value, Mapping):
        return _FrozenDict(
            (cast(str, key), _freeze(nested_value))
            for key, nested_value in value.items()
        )
    if isinstance(value, (list, tuple)):
        return tuple(_freeze(item) for item in value)
    return value


def _freeze_mapping(value: Mapping[str, object]) -> Mapping[str, object]:
    return _FrozenDict(value)
