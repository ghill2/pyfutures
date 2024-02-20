import pytest
import asyncio
import time
from decimal import Decimal
from unittest.mock import Mock

import pytest
from ibapi.contract import Contract
from ibapi.contract import Contract as IBContract
from ibapi.contract import ContractDetails as IBContractDetails
from ibapi.order import Order

from nautilus_trader.core.uuid import UUID4
from nautilus_trader.model.identifiers import InstrumentId

from pyfutures.adapters.interactive_brokers.client.client import ClientException
from pyfutures.adapters.interactive_brokers.client.objects import ClientException
from pyfutures.adapters.interactive_brokers.client.objects import IBBar
from pyfutures.adapters.interactive_brokers.client.objects import IBQuoteTick
from pyfutures.adapters.interactive_brokers.client.objects import IBTradeTick
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.enums import Frequency
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from ibapi.common import HistoricalTickBidAsk
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.adapters.interactive_brokers.parsing import instrument_id_to_contract
from pyfutures.adapters.interactive_brokers.parsing import parse_datetime



class TestIBClientSubscribe:
    @pytest.mark.skip(reason="flakey if market not open")
    @pytest.mark.asyncio()
    async def test_subscribe_quote_ticks(self, client):
        callback_mock = Mock()

        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        client.subscribe_quote_ticks(
            name="test",
            contract=contract,
            callback=callback_mock,
        )

        async def wait_for_quote_tick():
            while callback_mock.call_count == 0:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_quote_tick(), 2)

        assert callback_mock.call_count > 0


    @pytest.mark.skip(reason="flakey if market not open")
    @pytest.mark.asyncio()
    async def test_subscribe_bars_realtime(self, client):
        callback_mock = Mock()

        contract = Contract()
        contract.conId = 553444806
        contract.exchange = "ICEEUSOFT"

        client.subscribe_bars(
            name="test",
            contract=contract,
            what_to_show=WhatToShow.BID,
            bar_size=BarSize._5_SECOND,
            callback=callback_mock,
        )

        async def wait_for_bar():
            while callback_mock.call_count == 0:
                await asyncio.sleep(0)

        await asyncio.wait_for(wait_for_bar(), 2)

        assert callback_mock.call_count > 0

    # TODO: do we need this test?
    # @pytest.mark.skip(reason="flakey if market not open")
    # @pytest.mark.asyncio()
    # async def test_subscribe_bars_historical(self, client):
    #     callback_mock = Mock()
    #
    #     client.bar_events += callback_mock
    #
    #     contract = Contract()
    #     contract.conId = 553444806
    #     contract.exchange = "ICEEUSOFT"
    #
    #     client.subscribe_bars(
    #         name="test",
    #         contract=contract,
    #         what_to_show=WhatToShow.BID,
    #         bar_size=BarSize._15_SECOND,
    #         callback=callback_mock,
    #     )
    #
    #     async def wait_for_bar():
    #         while callback_mock.call_count == 0:
    #             await asyncio.sleep(0)
    #
    #     await asyncio.wait_for(wait_for_bar(), 2)
    #
    #     assert callback_mock.call_count > 0
    #
