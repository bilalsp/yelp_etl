"""
Utilities for input validation
"""
from functools import wraps
import inspect

from src.utils.exception import InvalidActionException


def _get_default_args(func):
    """Get default arguments of function"""
    signature = inspect.signature(func)

    return {
        k: v.default
        for k, v in signature.parameters.items()
        if v.default is not inspect.Parameter.empty
    }


def validate_action(func):
    """Validate action for ETL job"""
    @wraps(func)
    def wrapper(*args, **kwargs):
        kwda = _get_default_args(func)
        kwda.update(kwargs)

        if kwda['action'].lower() in ['return', 'bigquery']:
            return func(*args, **kwargs)
        else:
            raise InvalidActionException('Invalid action on ETL job.')

    return wrapper
