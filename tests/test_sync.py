import inspect

from oddssea import AsyncClient, Client


def test_same_methods_and_parameters():
    # Client's methods are written out by hand so editors show them as methods.
    # This keeps them in step with AsyncClient's.
    names = [name for name, _ in inspect.getmembers(AsyncClient, inspect.isfunction)
             if not name.startswith("_") and name != "aclose"]
    for name in names:
        async_params = list(inspect.signature(getattr(AsyncClient, name)).parameters.values())
        sync_params = list(inspect.signature(getattr(Client, name)).parameters.values())
        assert [(p.name, p.kind, p.default) for p in sync_params] == \
               [(p.name, p.kind, p.default) for p in async_params], name
        assert getattr(Client, name).__doc__ == getattr(AsyncClient, name).__doc__, name
