from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import Logger
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from nautilus_trader.test_kit.stubs.component import TestComponentStubs
from nautilus_trader.common.component import MessageBus
from nautilus_trader.cache.cache import Cache

from pyfutures.adapters.interactive_brokers.client.client import InteractiveBrokersClient
import logging
import asyncio
import functools
import types


from ibapi.contract import Contract
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel

init_logging(level_stdout=LogLevel.DEBUG)


# class MetaLogger(type):
#     @staticmethod
#     def _decorator(fun):
#         @functools.wraps(fun)
#         def wrapper(*args, **kwargs):
#             print(fun.__name__, args, kwargs)
#             return fun(*args, **kwargs)
#         return wrapper
#
#     def __new__(mcs, name, bases, attrs):
#         for key in attrs.keys():
#             print(key)
#             if callable(attrs[key]):
#                 # if attrs[key] is callable, then we can easily wrap it with decorator
#                 # and substitute in the future attrs
#                 # only for extra clarity (though it is wider type than function)
#                 fun = attrs[key]
#                 attrs[key] = MetaLogger._decorator(fun)
#         # and then invoke __new__ in type metaclass
#         return super().__new__(mcs, name, bases, attrs)
#


from functools import wraps


def wrapper(method):
    # @wraps(method)
    print("wrapper applied")

    def wrapped(*args, **kwargs):
        print("log to file")
        method(*args, **kwargs)

    return wrapped


class DecorateMethods(type):
    """Decorate all methods of the superclass with the decorator provided"""

    def __new__(cls, name, bases, attrs, **kwargs):
        try:
            decorator = kwargs["decorator"]
        except KeyError:
            raise ValueError('Please provide the "decorator" argument')

        exclude = kwargs.get("exclude", [])

        # Iterate through attrs to access instance methods
        for attr_name, attr_value in attrs.items():
            print(attr_name)
            if isinstance(attr_value, types.FunctionType) and attr_name not in exclude and not attr_name.startswith("__"):
                attrs[attr_name] = decorator(attr_value)

        return super(DecorateMethods, cls).__new__(cls, name, bases, attrs)


def wrap_methods(client):
    print(client.__dict__)
    for k, v in client.__dict__.items():
        if isinstance(k, types.FunctionType) and not k.startswith("__"):
            client[k] = wrapper(client[k])
    return client


# def log_to_file(func):
#     @functools.wraps(func)  # Preserve metadata
#     def wrapper(*args, **kwargs):
#         print("log to file")
#         print(args, kwargs)  # Access actual arguments
#         result = func(*args, **kwargs)
#         return result
#     return wrapper

# class LoggingInteractiveBrokersClient(InteractiveBrokersClient,metaclass=DecorateMethods,decorator=wrapper):
#     def __init__(
#         self,
#         loop: asyncio.AbstractEventLoop,
#         msgbus: MessageBus,
#         cache: Cache,
#         clock: LiveClock,
#         host: str = "127.0.0.1",
#         port: int = 7497,
#         client_id: int = 1,
#         api_log_level: int = logging.ERROR,
#         request_timeout_seconds: int | None = None,
#     ):
#         super().__init__(
#             loop=loop,
#             msgbus=msgbus,
#             cache=cache,
#             clock=clock,
#             host=host,
#             port=port,
#             client_id=client_id,
#             api_log_level=api_log_level,
#             request_timeout_seconds=request_timeout_seconds,
#         )


async def main():
    contract = Contract()
    contract.secType = "FUT"
    contract.exchange = "SNFE"
    contract.symbol = "XT"
    contract.tradingClass = "XT"
    contract.currency = "AUD"

    await client.connect()
    await client.request_contract_details(contract=contract)


client = InteractiveBrokersClient(
    loop=asyncio.get_event_loop(),
    msgbus=MessageBus(
        TestIdStubs.trader_id(),
        LiveClock(),
    ),
    cache=TestComponentStubs.cache(),
    clock=LiveClock(),
    port=4002,
)


client = wrap_methods(client)


asyncio.get_event_loop().run_until_complete(main())
