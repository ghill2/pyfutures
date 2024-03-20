import asyncio
import pytest
from pyfutures.client.protocol import IBProtocol
from unittest.mock import Mock
import time


@pytest.mark.asyncio()
async def test_single_buf(event_loop):
    protocol = IBProtocol(loop=event_loop, client=None)
    protocol._decoder = Mock()
    buf = b"\x00\x00\x00563\x001\x00-10\x00DU7606863\x00EquityWithLoanValue\x00868521.81\x00GBP\x00"
    fields = protocol.data_received(buf)

    # (b'63', b'1', b'-10', b'DU7606863', b'EquityWithLoanValue', b'868521.81', b'GBP')


@pytest.mark.asyncio()
async def test_multiple_buf(event_loop):
    """
    Sometimes data_received receives multiple responses
    """
    protocol = IBProtocol(loop=event_loop, client=None)
    protocol._decoder = Mock()
    buf = b"\x00\x00\x00%63\x001\x00-10\x00DU7606863\x00Cushion\x000.993542\x00\x00\x00\x00\x00*63\x001\x00-10\x00DU7606863\x00DayTradesRemaining\x00-1\x00\x00\x00\x00\x00*63\x001\x00-10\x00DU7606863\x00LookAheadNextChange\x000\x00\x00\x00\x00\x00*63\x001\x00-10\x00DU7606863\x00AccruedCash\x00695.46\x00GBP\x00\x00\x00\x00063\x001\x00-10\x00DU7606863\x00AvailableFunds\x00862955.49\x00GBP\x00\x00\x00\x00.63\x001\x00-10\x00DU7606863\x00BuyingPower\x003451821.94\x00GBP\x00\x00\x00\x00563\x001\x00-10\x00DU7606863\x00EquityWithLoanValue\x00868533.39\x00GBP\x00\x00\x00\x00163\x001\x00-10\x00DU7606863\x00ExcessLiquidity\x00864071.07\x00GBP\x00\x00\x00\x00463\x001\x00-10\x00DU7606863\x00FullAvailableFunds\x00862955.49\x00GBP\x00\x00\x00\x00563\x001\x00-10\x00DU7606863\x00FullExcessLiquidity\x00864071.07\x00GBP\x00\x00\x00\x00163\x001\x00-10\x00DU7606863\x00FullInitMarginReq\x005577.91\x00GBP\x00\x00\x00\x00263\x001\x00-10\x00DU7606863\x00FullMaintMarginReq\x004462.33\x00GBP\x00\x00\x00\x00/63\x001\x00-10\x00DU7606863\x00GrossPositionValue\x000.00\x00GBP\x00\x00\x00\x00-63\x001\x00-10\x00DU7606863\x00InitMarginReq\x005577.91\x00GBP\x00\x00\x00\x00963\x001\x00-10\x00DU7606863\x00LookAheadAvailableFunds\x00862955.49\x00GBP\x00\x00\x00\x00:63\x001\x00-10\x00DU7606863\x00LookAheadExcessLiquidity\x00864071.07\x00GBP\x00\x00\x00\x00663\x001\x00-10\x00DU7606863\x00LookAheadInitMarginReq\x005577.91\x00GBP\x00\x00\x00\x00763\x001\x00-10\x00DU7606863\x00LookAheadMaintMarginReq\x004462.33\x00GBP\x00\x00\x00\x00.63\x001\x00-10\x00DU7606863\x00MaintMarginReq\x004462.33\x00GBP\x00\x00\x00\x00063\x001\x00-10\x00DU7606863\x00NetLiquidation\x00869687.77\x00GBP\x00\x00\x00\x00@63\x001\x00-10\x00DU7606863\x00PreviousDayEquityWithLoanValue\x00385217.48\x00GBP\x00\x00\x00\x00%63\x001\x00-10\x00DU7606863\x00SMA\x00387055.34\x00GBP\x00\x00\x00\x00063\x001\x00-10\x00DU7606863\x00TotalCashValue\x00868992.32\x00GBP\x00\x00\x00\x00\t64\x001\x00-10\x00"

    start = time.perf_counter()
    fields = protocol.data_received(buf)
    stop = time.perf_counter()
    print(f"Elapsed time: {stop - start}")
    # check what _decoder.interpret was called by and assert all fields
