"""Validate task arguments against the function signature."""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import Any, cast, get_type_hints

from pydantic import BaseModel, ConfigDict, ValidationError, create_model

from akadze.exc import EnqueueError


def argument_model(fn: Callable[..., Any]) -> type[BaseModel]:
    signature = inspect.signature(fn)
    hints = get_type_hints(fn)
    fields: dict[str, Any] = {}
    for name, param in signature.parameters.items():
        if param.kind in (param.VAR_POSITIONAL, param.VAR_KEYWORD):
            raise EnqueueError(f"task {fn.__name__} cannot take *args or **kwargs")
        if param.kind is param.POSITIONAL_ONLY:
            raise EnqueueError(f"task {fn.__name__} cannot take positional-only arguments")
        annotation = hints.get(name, Any)
        default: Any = ... if param.default is inspect.Parameter.empty else param.default
        fields[name] = (annotation, default)
    model = create_model(
        f"{fn.__name__}Args",
        __config__=ConfigDict(extra="forbid"),
        **fields,
    )
    return cast(type[BaseModel], model)


def dump_arguments(
    model: type[BaseModel],
    arguments: dict[str, Any],
    *,
    task: str,
) -> dict[str, Any]:
    try:
        parsed = model.model_validate(arguments)
    except ValidationError as exc:
        raise EnqueueError(f"invalid arguments for task {task}") from exc
    dumped = parsed.model_dump(mode="json")
    if not isinstance(dumped, dict):
        raise EnqueueError(f"invalid arguments for task {task}")
    return dumped


def load_arguments(model: type[BaseModel], payload: dict[str, Any]) -> dict[str, Any]:
    try:
        parsed = model.model_validate(payload)
    except ValidationError:
        raise RuntimeError("invalid arguments") from None
    dumped = parsed.model_dump()
    if not isinstance(dumped, dict):
        raise RuntimeError("invalid arguments")
    return dumped
