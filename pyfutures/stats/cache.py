import functools
import json
from datetime import datetime
from pathlib import Path


def cache_pickle_daily(dir, filename):
    """
    A Decorator that pickles the returned output to disk
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            today = datetime.utcnow().strftime("%Y-%m-%d")
            path = Path.home() / "Desktop" / "pyfutures_cache" / dir / today / f"{filename}.pkl"

            if path.exists():
                print(f"Loading {filename} from cache - {path}")
                with open(path, "rb") as f:
                    return pickle.load(f)

            result = func(*args, **kwargs)

            print(f"Caching {filename} - {path}")
            path.parent.mkdir(parents=True, exist_ok=True)
            pickle.dump(result, open(path, "wb"))

            return result

        return wrapper

    return decorator


def cache_json_daily(dir, filename):
    """
    An explicit async version of the cache decorator.
    """

    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args, **kwargs):
            # Get the result based on whether func is async or not

            today = datetime.utcnow().strftime("%Y-%m-%d")
            path = Path.home() / "Desktop" / "pyfutures_cache" / dir / today / f"{filename}.json"

            if path.exists():
                print(f"Loading {filename} from cache - {path}")
                with open(path) as f:
                    return json.load(f)

            result = func(*args, **kwargs)

            print(f"Caching {filename} - {path}")
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as f:
                json.dump(result, f, indent=4)  # Optionally format with indentation

            return result

        return wrapper

    return decorator


# Async version with explicit async syntax
def async_cache_json_daily(dir, filename):
    """
    An explicit async version of the cache decorator.
    """

    def decorator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            # Get the result based on whether func is async or not

            today = datetime.utcnow().strftime("%Y-%m-%d")
            path = Path.home() / "Desktop" / "pyfutures_cache" / dir / today / f"{filename}.json"

            if path.exists():
                print(f"Loading {filename} from cache - {path}")
                with open(path) as f:
                    return json.load(f)

            result = await func(*args, **kwargs)

            print(f"Caching {filename} - {path}")
            path.parent.mkdir(parents=True, exist_ok=True)
            with open(path, "w") as f:
                json.dump(result, f, indent=4)  # Optionally format with indentation

            return result

        return wrapper

    return decorator
