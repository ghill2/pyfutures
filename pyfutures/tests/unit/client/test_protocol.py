import asyncio
import pytest
from pyfutures.client.protocol import Protocol
from unittest.mock import Mock
from unittest.mock import call
import time


@pytest.mark.asyncio()
async def test_data_received_single(event_loop):
    protocol = Protocol(loop=event_loop, client=None)
    protocol._decoder = Mock()
    protocol._decoder.interpret = Mock()
    call_args_list = [
        call(
            (
                b"63",
                b"1",
                b"-10",
                b"DU7606863",
                b"EquityWithLoanValue",
                b"868521.81",
                b"GBP",
            )
        )
    ]
    buf = b"\x00\x00\x00563\x001\x00-10\x00DU7606863\x00EquityWithLoanValue\x00868521.81\x00GBP\x00"

    fields = protocol.data_received(buf)

    assert protocol._decoder.interpret.call_args_list == call_args_list


@pytest.mark.asyncio()
async def test_data_received_multiple(event_loop):
    """
    Sometimes data_received receives multiple responses
    """
    protocol = Protocol(
        loop=event_loop,
        connection_lost_callback=Mock(),
        fields_received_callback=Mock(),
    )

    call_args = [
        call((b"63", b"1", b"-10", b"DU7606863", b"Cushion", b"0.993542", b"")),
        call((b"63", b"1", b"-10", b"DU7606863", b"DayTradesRemaining", b"-1", b"")),
        call((b"63", b"1", b"-10", b"DU7606863", b"LookAheadNextChange", b"0", b"")),
        call((b"63", b"1", b"-10", b"DU7606863", b"AccruedCash", b"695.46", b"GBP")),
        call(
            (b"63", b"1", b"-10", b"DU7606863", b"AvailableFunds", b"862955.49", b"GBP")
        ),
        call(
            (b"63", b"1", b"-10", b"DU7606863", b"BuyingPower", b"3451821.94", b"GBP")
        ),
        call(
            (
                b"63",
                b"1",
                b"-10",
                b"DU7606863",
                b"EquityWithLoanValue",
                b"868533.39",
                b"GBP",
            )
        ),
        call(
            (
                b"63",
                b"1",
                b"-10",
                b"DU7606863",
                b"ExcessLiquidity",
                b"864071.07",
                b"GBP",
            )
        ),
        call(
            (
                b"63",
                b"1",
                b"-10",
                b"DU7606863",
                b"FullAvailableFunds",
                b"862955.49",
                b"GBP",
            )
        ),
        call(
            (
                b"63",
                b"1",
                b"-10",
                b"DU7606863",
                b"FullExcessLiquidity",
                b"864071.07",
                b"GBP",
            )
        ),
        call(
            (
                b"63",
                b"1",
                b"-10",
                b"DU7606863",
                b"FullInitMarginReq",
                b"5577.91",
                b"GBP",
            )
        ),
        call(
            (
                b"63",
                b"1",
                b"-10",
                b"DU7606863",
                b"FullMaintMarginReq",
                b"4462.33",
                b"GBP",
            )
        ),
        call(
            (b"63", b"1", b"-10", b"DU7606863", b"GrossPositionValue", b"0.00", b"GBP")
        ),
        call((b"63", b"1", b"-10", b"DU7606863", b"InitMarginReq", b"5577.91", b"GBP")),
        call(
            (
                b"63",
                b"1",
                b"-10",
                b"DU7606863",
                b"LookAheadAvailableFunds",
                b"862955.49",
                b"GBP",
            )
        ),
        call(
            (
                b"63",
                b"1",
                b"-10",
                b"DU7606863",
                b"LookAheadExcessLiquidity",
                b"864071.07",
                b"GBP",
            )
        ),
        call(
            (
                b"63",
                b"1",
                b"-10",
                b"DU7606863",
                b"LookAheadInitMarginReq",
                b"5577.91",
                b"GBP",
            )
        ),
        call(
            (
                b"63",
                b"1",
                b"-10",
                b"DU7606863",
                b"LookAheadMaintMarginReq",
                b"4462.33",
                b"GBP",
            )
        ),
        call(
            (b"63", b"1", b"-10", b"DU7606863", b"MaintMarginReq", b"4462.33", b"GBP")
        ),
        call(
            (b"63", b"1", b"-10", b"DU7606863", b"NetLiquidation", b"869687.77", b"GBP")
        ),
        call(
            (
                b"63",
                b"1",
                b"-10",
                b"DU7606863",
                b"PreviousDayEquityWithLoanValue",
                b"385217.48",
                b"GBP",
            )
        ),
        call((b"63", b"1", b"-10", b"DU7606863", b"SMA", b"387055.34", b"GBP")),
        call(
            (b"63", b"1", b"-10", b"DU7606863", b"TotalCashValue", b"868992.32", b"GBP")
        ),
        call((b"64", b"1", b"-10")),
    ]

    buf = b"\x00\x00\x00%63\x001\x00-10\x00DU7606863\x00Cushion\x000.993542\x00\x00\x00\x00\x00*63\x001\x00-10\x00DU7606863\x00DayTradesRemaining\x00-1\x00\x00\x00\x00\x00*63\x001\x00-10\x00DU7606863\x00LookAheadNextChange\x000\x00\x00\x00\x00\x00*63\x001\x00-10\x00DU7606863\x00AccruedCash\x00695.46\x00GBP\x00\x00\x00\x00063\x001\x00-10\x00DU7606863\x00AvailableFunds\x00862955.49\x00GBP\x00\x00\x00\x00.63\x001\x00-10\x00DU7606863\x00BuyingPower\x003451821.94\x00GBP\x00\x00\x00\x00563\x001\x00-10\x00DU7606863\x00EquityWithLoanValue\x00868533.39\x00GBP\x00\x00\x00\x00163\x001\x00-10\x00DU7606863\x00ExcessLiquidity\x00864071.07\x00GBP\x00\x00\x00\x00463\x001\x00-10\x00DU7606863\x00FullAvailableFunds\x00862955.49\x00GBP\x00\x00\x00\x00563\x001\x00-10\x00DU7606863\x00FullExcessLiquidity\x00864071.07\x00GBP\x00\x00\x00\x00163\x001\x00-10\x00DU7606863\x00FullInitMarginReq\x005577.91\x00GBP\x00\x00\x00\x00263\x001\x00-10\x00DU7606863\x00FullMaintMarginReq\x004462.33\x00GBP\x00\x00\x00\x00/63\x001\x00-10\x00DU7606863\x00GrossPositionValue\x000.00\x00GBP\x00\x00\x00\x00-63\x001\x00-10\x00DU7606863\x00InitMarginReq\x005577.91\x00GBP\x00\x00\x00\x00963\x001\x00-10\x00DU7606863\x00LookAheadAvailableFunds\x00862955.49\x00GBP\x00\x00\x00\x00:63\x001\x00-10\x00DU7606863\x00LookAheadExcessLiquidity\x00864071.07\x00GBP\x00\x00\x00\x00663\x001\x00-10\x00DU7606863\x00LookAheadInitMarginReq\x005577.91\x00GBP\x00\x00\x00\x00763\x001\x00-10\x00DU7606863\x00LookAheadMaintMarginReq\x004462.33\x00GBP\x00\x00\x00\x00.63\x001\x00-10\x00DU7606863\x00MaintMarginReq\x004462.33\x00GBP\x00\x00\x00\x00063\x001\x00-10\x00DU7606863\x00NetLiquidation\x00869687.77\x00GBP\x00\x00\x00\x00@63\x001\x00-10\x00DU7606863\x00PreviousDayEquityWithLoanValue\x00385217.48\x00GBP\x00\x00\x00\x00%63\x001\x00-10\x00DU7606863\x00SMA\x00387055.34\x00GBP\x00\x00\x00\x00063\x001\x00-10\x00DU7606863\x00TotalCashValue\x00868992.32\x00GBP\x00\x00\x00\x00\t64\x001\x00-10\x00"

    start = time.perf_counter()

    protocol.data_received(buf)
    stop = time.perf_counter()
    print(f"Elapsed time: {stop - start}")
    # assert protocol._decoder.interpret.call_args_list == call_args
    # send_mock.call_args_list[0][1] == {
    #     "reqId": -10,
    #     "contract": contract,
    # }
