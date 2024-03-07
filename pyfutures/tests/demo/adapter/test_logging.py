# Tests if:
# - python module logging (used by Pyfutures IB Client and Historic)
# - nautilus Logger
# can viewed in the same stdout
#
#



import logging
import sys
from nautilus_trader.common.component import Logger
from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel

from pyfutures.adapter.factories import (
    InteractiveBrokersLiveDataClientFactory,
)
from pyfutures.adapter.factories import (
    InteractiveBrokersLiveExecClientFactory,
)
from pyfutures.adapter.config import (
    InteractiveBrokersDataClientConfig,
)
from pyfutures.adapter.config import (
    InteractiveBrokersExecClientConfig,
)


from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.model.identifiers import TraderId
import asyncio

logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)

def test_dual_logging_components():
    clock=LiveClock()
    data_client =  InteractiveBrokersLiveDataClientFactory.create(
        loop=asyncio.get_event_loop(),
        name="str",
        config=InteractiveBrokersDataClientConfig(),
        msgbus = MessageBus(
            trader_id=TraderId("TESTER-000"),
            clock=clock,
        ),
        cache=Cache(),
        clock=clock,
    )
    python_logger = logging.getLogger("test_logging")
    python_logger.setLevel(logging.DEBUG)
    python_logger.info("Here is python logger")


    init_logging(level_stdout=LogLevel.DEBUG)
    nautilus_logger = Logger("test_logging")
    nautilus_logger.info("Here is nautilus logger")

def test_dual_logging():
    """
        should be able to call this with python or pytest
    """
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG)
    python_logger = logging.getLogger("test_logging")
    python_logger.setLevel(logging.DEBUG)
    python_logger.info("Here is python logger")


    init_logging(level_stdout=LogLevel.DEBUG)
    nautilus_logger = Logger("test_logging")
    nautilus_logger.info("Here is nautilus logger")


test_dual_logging_components()
