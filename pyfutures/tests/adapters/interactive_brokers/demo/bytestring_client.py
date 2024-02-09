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


from ibapi.contract import Contract as Contract
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel



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
class DecorateMethods(type):
    """ Decorate all methods of the class with the decorator provided """

    def __new__(cls, name, bases, attrs, **kwargs):
        try:
            decorator = kwargs['decorator']
        except KeyError:
            raise ValueError('Please provide the "decorator" argument, eg. '
                             'MyClass(..., metaclass=DecorateMethods, decorator=my_decorator)')

        exclude = kwargs.get('exclude', [])

        for attr_name, attr_value in attrs.items():

            if isinstance(attr_value, types.FunctionType) and \
                    attr_name not in exclude and \
                    not attr_name.startswith('__'):
                attrs[attr_name] = decorator(attr_value)

        return super(DecorateMethods, cls).__new__(cls, name, bases, attrs)


def log_to_file(one, two, three, four): # *args, **kwargs):
    # print(args, kwargs)
    print(one, two, three, four)

class LoggingInteractiveBrokersClient(InteractiveBrokersClient,metaclass=DecorateMethods, decorator=log_to_file):
    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        logger: Logger,
        host: str = "127.0.0.1",
        port: int = 7497,
        client_id: int = 1,
        api_log_level: int = logging.ERROR,
        request_timeout_seconds: int | None = None,
    ):
        super().__init__(
            loop=loop,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            logger=logger,
            host=host,
            port=port,
            client_id=client_id,
            api_log_level=api_log_level,
            request_timeout_seconds=request_timeout_seconds,
        )


logger = Logger(name="pytest")


client = LoggingInteractiveBrokersClient(
    loop=asyncio.get_event_loop(),
    msgbus=MessageBus(
        TestIdStubs.trader_id(),
        LiveClock(),
    ),
    cache=TestComponentStubs.cache(),
    clock=LiveClock(),
    logger=logger,
    port=4002,
)
contract = IBContract()
contract.secType="FUT"
contract.exchange="SNFE"
contract.symbol="XT"
contract.tradingClass="XT"
contract.currency="AUD"

asyncio.run(client.request_contract_details(contract=contract))
