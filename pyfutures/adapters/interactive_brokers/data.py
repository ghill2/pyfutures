import asyncio

# fmt: off

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.common.component import MessageBus
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pyfutures.adapters.interactive_brokers.parsing import dict_to_contract
from pyfutures.adapters.interactive_brokers.client.client import InteractiveBrokersClient
from pyfutures.adapters.interactive_brokers import IB_VENUE
from pyfutures.adapters.interactive_brokers.config import InteractiveBrokersDataClientConfig
from pyfutures.adapters.interactive_brokers.providers import InteractiveBrokersInstrumentProvider
from nautilus_trader.common.config import NautilusConfig

# fmt: on


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

    async def _subscribe_bars(self, bar_type: BarType):
        instrument = self._cache.instrument(bar_type.instrument_id)

        if instrument is None:
            self._log.error(f"Cannot subscribe to {bar_type}, Instrument not found.")
            return

        await self._client.subscribe_bars(
            request_id=str(bar_type),
            contract=dict_to_contract(instrument.info["contract"]),
            what_to_show=WhatToShow.from_price_type(bar_type.spec.price_type),
        )

    async def _unsubscribe_bars(self, bar_type: BarType) -> None:
        await self._client.unsubscribe_bars(
            request_id=str(bar_type),
            what_to_show=WhatToShow.from_price_type(bar_type.spec.price_type),
        )
