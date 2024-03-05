import asyncio
import functools


from ibapi.common import BarData

# fmt: off
from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.instruments.base import Instrument

from pyfutures.adapter import IB_VENUE
from pyfutures.adapter.client.client import InteractiveBrokersClient
from pyfutures.adapter.config import InteractiveBrokersDataClientConfig
from pyfutures.adapter.enums import BarSize
from pyfutures.adapter.enums import WhatToShow
from pyfutures.adapter.parsing import dict_to_contract
from pyfutures.adapter.parsing import bar_data_to_nautilus_bar
from pyfutures.adapter.providers import InteractiveBrokersInstrumentProvider


# fmt: on
#

class InteractiveBrokersDataClient(LiveMarketDataClient):
    """
    Provides a data client for the InteractiveBrokers exchange.
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        client: InteractiveBrokersClient,
        msgbus: MessageBus,
        cache: Cache,
        clock: LiveClock,
        instrument_provider: InteractiveBrokersInstrumentProvider,
        ibg_client_id: int,
        config: InteractiveBrokersDataClientConfig,
    ):
        super().__init__(
            loop=loop,
            client_id=ClientId(f"{IB_VENUE.value}-{ibg_client_id:03d}"),
            venue=None,
            instrument_provider=instrument_provider,
            msgbus=msgbus,
            cache=cache,
            clock=clock,
            # config=NautilusConfig(
            #     name=f"{type(self).__name__}-{ibg_client_id:03d}",
            #     client_id=ibg_client_id,
            # ),
        )
        self._client = client
        self._handle_revised_bars = config.handle_revised_bars
        self._use_regular_trading_hours = config.use_regular_trading_hours
        self._market_data_type = config.market_data_type

    @property
    def instrument_provider(self) -> InteractiveBrokersInstrumentProvider:
        return self._instrument_provider  # type: ignore

    async def _connect(self):
        if not self._client.connection.is_connected:
            await self._client.connect()

        # Load instruments based on config
        await self.instrument_provider.initialize()
        for instrument in self._instrument_provider.list_all():
            self._handle_data(instrument)  # add to cache

    def _bar_callback(self, bar_type: BarType, bar: BarData, instrument: Instrument) -> None:
        nautilus_bar = bar_data_to_nautilus_bar(bar_type=bar_type, bar=bar, instrument=instrument)
        self._handle_data(nautilus_bar)



    async def _subscribe_bars(self, bar_type: BarType):
        instrument = self._cache.instrument(bar_type.instrument_id)

        if instrument is None:
            self._log.error(f"Cannot subscribe to {bar_type}, Instrument not found.")
            return


        # parse bar_type.spec to bar_size
        callback = functools.partial(
            self._bar_callback,
            bar_type=bar_type,
            instrument=instrument
        )
        self._client.subscribe_bars(
            contract=dict_to_contract(instrument.info["contract"]),
            what_to_show=WhatToShow.from_price_type(bar_type.spec.price_type),
            bar_size=BarSize.from_bar_spec(bar_type.spec),
            callback=callback
        )
    

    async def _subscribe_quote_ticks(self, bar_type: BarType):
        pass
            

