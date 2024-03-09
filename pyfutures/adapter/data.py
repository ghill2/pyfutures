import pandas as pd
import asyncio
import functools
from ibapi.common import BarData

from nautilus_trader.cache.cache import Cache
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import MessageBus
from nautilus_trader.live.data_client import LiveMarketDataClient
from nautilus_trader.model.data import BarType
from nautilus_trader.model.identifiers import ClientId
from nautilus_trader.model.instruments.base import Instrument
from pyfutures.adapter import IB_VENUE
from nautilus_trader.model.data import Bar
from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.adapter.config import InteractiveBrokersDataClientConfig
from pyfutures.adapter.enums import BarSize
from pyfutures.adapter.enums import WhatToShow
from pyfutures.adapter.providers import InteractiveBrokersInstrumentProvider
from nautilus_trader.core.uuid import UUID4
from pyfutures.client.historic import InteractiveBrokersBarClient
from pyfutures.adapter.parsing import AdapterParser

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
        config: InteractiveBrokersDataClientConfig,
    ):
        super().__init__(
            loop=loop,
            client_id=ClientId(f"{IB_VENUE.value}"),
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
        
        self._historic = InteractiveBrokersBarClient(
            client=client,
            use_cache=False,  # TODO add config
        )
        
        self._parser = AdapterParser()

    @property
    def instrument_provider(self) -> InteractiveBrokersInstrumentProvider:
        return self._instrument_provider  # type: ignore
    
    @property
    def client(self) -> InteractiveBrokersClient:
        return self._client
    
    @property
    def cache(self) -> Cache:
        return self._cache
    
    async def _connect(self):
        
        if not self._client.connection.is_connected:
            await self._client.connect()

        # Load instruments based on config
        await self.instrument_provider.initialize()
        for instrument in self._instrument_provider.list_all():
            self._handle_data(instrument)  # add to cache

    async def _subscribe_bars(self, bar_type: BarType):
        
        if not (instrument := self._cache.instrument(bar_type.instrument_id)):
            self._log.error(f"Cannot subscribe to {bar_type}, Instrument not found.")
            return

        # parse bar_type.spec to bar_size
        callback = functools.partial(
            self._bar_callback,
            bar_type=bar_type,
            instrument=instrument
        )
        
        self._client.subscribe_bars(
            contract=self._parser.instrument_id_to_contract(bar_type.instrument_id),
            what_to_show=WhatToShow.from_price_type(bar_type.spec.price_type),
            bar_size=BarSize.from_bar_spec(bar_type.spec),
            callback=callback
        )
    
    def _bar_callback(self, bar_type: BarType, bar: BarData, instrument: Instrument) -> None:
        nautilus_bar: Bar = self._parser.bar_data_to_nautilus_bar(bar_type=bar_type, bar=bar, instrument=instrument)
        self._handle_data(nautilus_bar)
        
    async def _request_bars(
        self,
        bar_type: BarType,
        limit: int,
        correlation_id: UUID4,
        start: pd.Timestamp | None = None,
        end: pd.Timestamp | None = None,
    ) -> None:
        
        if not (instrument := self._cache.instrument(bar_type.instrument_id)):
            self._log.error(
                f"Cannot request {bar_type}, Instrument not found.",
            )
            return
        
        if not bar_type.spec.is_time_aggregated():
            self._log.error(
                f"Cannot request {bar_type}: only time bars are aggregated by InteractiveBrokers.",
            )
            return
        
        bars: list[BarData] = await self._historic.request_bars(
            contract=self._parser.instrument_id_to_contract(bar_type.instrument_id),
            bar_size=BarSize.from_bar_spec(bar_type.spec),
            what_to_show=WhatToShow.from_price_type(bar_type.spec.price_type),
            start_time=start,
            end_time=end or pd.Timestamp.utcnow(),
            limit=None if limit == 0 else limit,
        )
        
        bars: list[Bar] = [
            self._parser.bar_data_to_nautilus_bar(bar_type=bar_type, bar=bar, instrument=instrument)
            for bar in bars
        ]
        
        self._handle_bars(
            bar_type=bar_type,
            bars=bars,
            partial=None,  # bars[0]
            correlation_id=correlation_id,
        )
        
        
        
    
            

