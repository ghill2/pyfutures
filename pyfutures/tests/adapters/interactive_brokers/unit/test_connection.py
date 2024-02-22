import asyncio

import pytest
from nautilus_trader.model.identifiers import InstrumentId

from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pyfutures.adapters.interactive_brokers.parsing import instrument_id_to_contract


class TestInteractiveBrokersClient:
    # @pytest.mark.asyncio()
    # async def test_handle_disconnect(self, client):

    #     import os
    #     os.system("killall -m java")
    #     await asyncio.sleep(1)

    #     os.system("sh /opt/ibc/twsstartmacos.sh")

    #     await asyncio.sleep(5)

    #     assert False

    @pytest.mark.skip(reason="research")
    @pytest.mark.asyncio()
    async def test_reconnect_after_restart(self, client):
        """
        Does market data continue to continue streaming after restart? Conclusion: NO
        """
        instrument_id = InstrumentId.from_str("PL-PL.NYMEX")
        contract = instrument_id_to_contract(instrument_id)
        front_contract = await client.request_front_contract(contract)

        client.subscribe_bars(
            name=instrument_id.value,
            contract=front_contract,
            what_to_show=WhatToShow.BID,
            bar_size=BarSize._5_SECOND,
            callback=lambda x: print(x),
        )

        while True:
            await asyncio.sleep(0)
