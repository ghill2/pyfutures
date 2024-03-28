import time
from unittest.mock import Mock
from unittest.mock import call

import pytest

from pyfutures.client.protocol import Protocol


@pytest.mark.asyncio()
async def test_data_received_max_limit(event_loop):
    """
    If the amount of bytes being read by the transport is above a certain amount
    data_received method will receive an incomplete / unparsable buffer
    until the next call to data_received.
    the incomplete buffer should be detected and stored until the next data_received call, then parsed

    For every message, there should be 1 call of fields_received.

    correctly formatted message = bytestring containing null delimited fields with its size prefix / length added
    eg: b"<4 bytes / size_prefix>field\x00field\x00field...\x00"
    eg: b"\x00\x00\x02\xf8field\x00field\x00field\x00"

    Example of what this is testing for::
        data_received call 1 input:
            -- multiple correctly formatted messages to reach max limit
            -- last message = b"\x00\x00\x02\xf8field\x00this field is a str"
        data_received call 2 input:
            -- b"ing that has been split between data_received calls\x00"

    To demo test this: pull contract details for all universe instruments
    """
    details_bytes = b"\x00\x00\x02\xf810\x00-60\x00NG\x00FUT\x0020270827-18:30:00\x000\x00\x00NYMEX\x00USD\x00NGU27\x00NG\x00NG\x00269460134\x000.001\x0010000\x00ACTIVETIM,AD,ADJUST,ALERT,ALGO,ALLOC,AVGCOST,BASKET,BENCHPX,COND,CONDORDER,DAY,DEACT,DEACTDIS,DEACTEOD,GAT,GTC,GTD,GTT,HID,ICE,IOC,LIT,LMT,LTH,MIT,MKT,MTL,NGCOMB,NONALGO,OCA,PEGBENCH,SCALE,SCALERST,SIZECHK,SNAPMID,SNAPMKT,SNAPREL,STP,STPLMT,TRAIL,TRAILLIT,TRAILLMT,TRAILMIT,WHATIF\x00NYMEX,QBALGO\x001\x0036552980\x00Henry Hub Natural Gas\x00\x00202709\x00\x00\x00\x00US/Eastern\x0020240325:1800-20240326:1700;20240326:1800-20240327:1700;20240327:1800-20240328:1700;20240329:CLOSED;20240330:CLOSED;20240331:1800-20240401:1700\x0020240326:0930-20240326:1700;20240327:0930-20240327:1700;20240328:0930-20240328:1700;20240329:CLOSED;20240330:CLOSED;20240331:1800-20240401:1700\x00\x00\x000\x002147483647\x00NG\x00IND\x0098,98\x0020270827\x00\x001\x001\x001\x00"
    first_half_details_bytes = details_bytes[:42]

    first_call_bytes = details_bytes + first_half_details_bytes
    second_call_bytes = details_bytes[42:]

    fields_received_mock = Mock()
    protocol = Protocol(
        loop=event_loop,
        connection_lost_callback=Mock(),
        fields_received_callback=fields_received_mock,
    )

    # Test Case 1
    protocol.data_received(first_call_bytes)
    protocol.data_received(second_call_bytes)

    assert fields_received_mock.call_count
    # the split was on the exchange field index 7, check if they are the same
    assert (
        fields_received_mock.call_args_list[0] == fields_received_mock.call_args_list[1]
    )
    assert protocol._buffer == b""


@pytest.mark.asyncio()
async def test_data_received_hang(event_loop):
    fields_received_mock = Mock()
    protocol = Protocol(
        loop=event_loop,
        connection_lost_callback=Mock(),
        fields_received_callback=fields_received_mock,
    )
    bytes_1 = b"\x00\x00\x02\xde10\x00-10\x00DA\x00FUT\x0020250701-17:10:00\x000\x00\x00CME\x00USD\x00DCM5\x00DC\x00DC\x00639357861\x000.01\x002000\x00ACTIVETIM,AD,ADJUST,ALERT,ALGO,ALLOC,AVGCOST,BASKET,BENCHPX,COND,CONDORDER,DAY,DEACT,DEACTDIS,DEACTEOD,GAT,GTC,GTD,GTT,HID,ICE,IOC,LIT,LMT,LTH,MIT,MKT,MTL,NGCOMB,NONALGO,OCA,PEGBENCH,SCALE,SCALERST,SNAPMID,SNAPMKT,SNAPREL,STP,STPLMT,TRAIL,TRAILLIT,TRAILLMT,TRAILMIT,WHATIF\x00CME\x001\x0036555530\x00MILK CLASS III INDEX\x00\x00202506\x00\x00\x00\x00US/Central\x0020240325:1700-20240326:1600;20240326:1700-20240327:1600;20240327:1700-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x0020240326:0830-20240326:1600;20240327:0830-20240327:1600;20240328:0830-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x00\x00\x000\x002147483647\x00DA\x00IND\x0032\x0020250702\x00\x001\x001\x001\x00\x00\x00\x02\xde10\x00-10\x00DA\x00FUT\x0020250729-17:10:00\x000\x00\x00CME\x00USD\x00DCN5\x00DC\x00DC\x00645904227\x000.01\x002000\x00ACTIVETIM,AD,ADJUST,ALERT,ALGO,ALLOC,AVGCOST,BASKET,BENCHPX,COND,CONDORDER,DAY,DEACT,DEACTDIS,DEACTEOD,GAT,GTC,GTD,GTT,HID,ICE,IOC,LIT,LMT,LTH,MIT,MKT,MTL,NGCOMB,NONALGO,OCA,PEGBENCH,SCALE,SCALERST,SNAPMID,SNAPMKT,SNAPREL,STP,STPLMT,TRAIL,TRAILLIT,TRAILLMT,TRAILMIT,WHATIF\x00CME\x001\x0036555530\x00MILK CLASS III INDEX\x00\x00202507\x00\x00\x00\x00US/Central\x0020240325:1700-20240326:1600;20240326:1700-20240327:1600;20240327:1700-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x0020240326:0830-20240326:1600;20240327:0830-20240327:1600;20240328:0830-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x00\x00\x000\x002147483647\x00DA\x00IND\x0032\x0020250730\x00\x001\x001\x001\x00"
    bytes_2 = b"\x00\x00\x02\xde10\x00-10\x00DA\x00FUT\x0020250903-17:10:00\x000\x00\x00CME\x00USD\x00DCQ5\x00DC\x00DC\x00650949040\x000.01\x002000\x00ACTIVETIM,AD,ADJUST,ALERT,ALGO,ALLOC,AVGCOST,BASKET,BENCHPX,COND,CONDORDER,DAY,DEACT,DEACTDIS,DEACTEOD,GAT,GTC,GTD,GTT,HID,ICE,IOC,LIT,LMT,LTH,MIT,MKT,MTL,NGCOMB,NONALGO,OCA,PEGBENCH,SCALE,SCALERST,SNAPMID,SNAPMKT,SNAPREL,STP,STPLMT,TRAIL,TRAILLIT,TRAILLMT,TRAILMIT,WHATIF\x00CME\x001\x0036555530\x00MILK CLASS III INDEX\x00\x00202508\x00\x00\x00\x00US/Central\x0020240325:1700-20240326:1600;20240326:1700-20240327:1600;20240327:1700-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x0020240326:0830-20240326:1600;20240327:0830-20240327:1600;20240328:0830-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x00\x00\x000\x002147483647\x00DA\x00IND\x0032\x0020250904\x00\x001\x001\x001\x00\x00\x00\x02\xde10\x00-10\x00DA\x00FUT\x0020250930-17:10:00\x000\x00\x00CME\x00USD\x00DCU5\x00DC\x00DC\x00657760778\x000.01\x002000\x00ACTIVETIM,AD,ADJUST,ALERT,ALGO,ALLOC,AVGCOST,BASKET,BENCHPX,COND,CONDORDER,DAY,DEACT,DEACTDIS,DEACTEOD,GAT,GTC,GTD,GTT,HID,ICE,IOC,LIT,LMT,LTH,MIT,MKT,MTL,NGCOMB,NONALGO,OCA,PEGBENCH,SCALE,SCALERST,SNAPMID,SNAPMKT,SNAPREL,STP,STPLMT,TRAIL,TRAILLIT,TRAILLMT,TRAILMIT,WHATIF\x00CME\x001\x0036555530\x00MILK CLASS III INDEX\x00\x00202509\x00\x00\x00\x00US/Central\x0020240325:1700-20240326:1600;20240326:1700-20240327:1600;20240327:1700-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x0020240326:0830-20240326:1600;20240327:0830-20240327:1600;20240328:0830-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x00\x00\x000\x002147483647\x00DA\x00IND\x0032\x0020251001\x00\x001\x001\x001\x00\x00\x00\x02\xde10\x00-10\x00DA\x00FUT\x0020251104-18:10:00\x000\x00\x00CME\x00USD\x00DCV5\x00DC\x00DC\x00663168763\x000.01\x002000\x00ACTIVETIM,AD,ADJUST,ALERT,ALGO,ALLOC,AVGCOST,BASKET,BENCHPX,COND,CONDORDER,DAY,DEACT,DEACTDIS,DEACTEOD,GAT,GTC,GTD,GTT,HID,ICE,IOC,LIT,LMT,LTH,MIT,MKT,MTL,NGCOMB,NONALGO,OCA,PEGBENCH,SCALE,SCALERST,SNAPMID,SNAPMKT,SNAPREL,STP,STPLMT,TRAIL,TRAILLIT,TRAILLMT,TRAILMIT,WHATIF\x00CME\x001\x0036555530\x00MILK CLASS III INDEX\x00\x00202510\x00\x00\x00\x00US/Central\x0020240325:1700-20240326:1600;20240326:1700-20240327:1600;20240327:1700-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x0020240326:0830-20240326:1600;20240327:0830-20240327:1600;20240328:0830-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x00\x00\x000\x002147483647\x00DA\x00IND\x0032\x0020251105\x00\x001\x001\x001\x00\x00\x00\x02\xde10\x00-10\x00DA\x00FUT\x0020251202-18:10:00\x000\x00\x00CME\x00USD\x00DCX5\x00DC\x00DC\x00668447027\x000.01\x002000\x00ACTIVETIM,AD,ADJUST,ALERT,ALGO,ALLOC,AVGCOST,BASKET,BENCHPX,COND,CONDORDER,DAY,DEACT,DEACTDIS,DEACTEOD,GAT,GTC,GTD,GTT,HID,ICE,IOC,LIT,LMT,LTH,MIT,MKT,MTL,NGCOMB,NONALGO,OCA,PEGBENCH,SCALE,SCALERST,SNAPMID,SNAPMKT,SNAPREL,STP,STPLMT,TRAIL,TRAILLIT,TRAILLMT,TRAILMIT,WHATIF\x00CME\x001\x0036555530\x00MILK CLASS III INDEX\x00\x00202511\x00\x00\x00\x00US/Central\x0020240325:1700-20240326:1600;20240326:1700-20240327:1600;20240327:1700-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x0020240326:0830-20240326:1600;20240327:0830-20240327:1600;20240328:0830-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x00\x00\x000\x002147483647\x00DA\x00IND\x0032\x0020251203\x00\x001\x001\x001\x00\x00\x00\x02\xde10\x00-10\x00DA\x00FUT\x0020251230-18:10:00\x000\x00\x00CME\x00USD\x00DCZ5\x00DC\x00DC\x00675617905\x000.01\x002000\x00ACTIVETIM,AD,ADJUST,ALERT,ALGO,ALLOC,AVGCOST,BASKET,BENCHPX,COND,CONDORDER,DAY,DEACT,DEACTDIS,DEACTEOD,GAT,GTC,GTD,GTT,HID,ICE,IOC,LIT,LMT,LTH,MIT,MKT,MTL,NGCOMB,NONALGO,OCA,PEGBENCH,SCALE,SCALERST,SNAPMID,SNAPMKT,SNAPREL,STP,STPLMT,TRAIL,TRAILLIT,TRAILLMT,TRAILMIT,WHATIF\x00CME\x001\x0036555530\x00MILK CLASS III INDEX\x00\x00202512\x00\x00\x00\x00US/Central\x0020240325:1700-20240326:1600;20240326:1700-20240327:1600;20240327:1700-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x0020240326:0830-20240326:1600;20240327:0830-20240327:1600;20240328:0830-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x00\x00\x000\x002147483647\x00DA\x00IND\x0032\x0020251231\x00\x001\x001\x001\x00\x00\x00\x02\xde10\x00-10\x00DA\x00FUT\x0020260203-18:10:00\x000\x00\x00CME\x00USD\x00DCF6\x00DC\x00DC\x00681156880\x000.01\x002000\x00ACTIVETIM,AD,ADJUST,ALERT,ALGO,ALLOC,AVGCOST,BASKET,BENCHPX,COND,CONDORDER,DAY,DEACT,DEACTDIS,DEACTEOD,GAT,GTC,GTD,GTT,HID,ICE,IOC,LIT,LMT,LTH,MIT,MKT,MTL,NGCOMB,NONALGO,OCA,PEGBENCH,SCALE,SCALERST,SNAPMID,SNAPMKT,SNAPREL,STP,STPLMT,TRAIL,TRAILLIT,TRAILLMT,TRAILMIT,WHATIF\x00CME\x001\x0036555530\x00MILK CLASS III INDEX\x00\x00202601\x00\x00\x00\x00US/Central\x0020240325:1700-20240326:1600;20240326:1700-20240327:1600;20240327:1700-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x0020240326:0830-20240326:1600;20240327:0830-20240327:1600;20240328:0830-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x00\x00\x000\x002147483647\x00DA\x00IND\x0032\x0020260204\x00\x001\x001\x001\x00\x00\x00\x02\xde10\x00-10\x00DA\x00FUT\x0020260303-18:10:00\x000\x00\x00CME\x00USD\x00DCG6\x00DC\x00DC\x00687087934\x000.01\x002000\x00ACTIVETIM,AD,ADJUST,ALERT,ALGO,ALLOC,AVGCOST,BASKET,BENCHPX,COND,CONDORDER,DAY,DEACT,DEACTDIS,DEACTEOD,GAT,GTC,GTD,GTT,HID,ICE,IOC,LIT,LMT,LTH,MIT,MKT,MTL,NGCOMB,NONALGO,OCA,PEGBENCH,SCALE,SCALERST,SNAPMID,SNAPMKT,SNAPREL,STP,STPLMT,TRAIL,TRAILLIT,TRAILLMT,TRAILMIT,WHATIF\x00CME\x001\x0036555530\x00MILK CLASS III INDEX\x00\x00202602\x00\x00\x00\x00US/Central\x0020240325:1700-20240326:1600;20240326:1700-20240327:1600;20240327:1700-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x0020240326:0830-20240326:1600;20240327:0830-20240327:1600;20240328:0830-20240328:1355;20240329:CLOSED;20240330:CLOSED;20240331:1700-20240401:1600\x00\x00\x000\x002147483647\x00DA\x00IND\x0032\x0020260304\x00\x001\x001\x001\x00\x00\x00\x00\t52\x001\x00-10\x00"
    protocol.data_received(bytes_1)
    protocol.data_received(bytes_2)


# TO FINISH
@pytest.mark.asyncio()
async def test_data_received_single(event_loop):
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

    fields_received_mock = Mock()
    protocol = Protocol(
        loop=event_loop,
        connection_lost_callback=Mock(),
        fields_received_callback=fields_received_mock,
    )

    buf = b"\x00\x00\x00563\x001\x00-10\x00DU7606863\x00EquityWithLoanValue\x00868521.81\x00GBP\x00"

    fields = protocol.data_received(buf)
    print(protocol.call_args_list)


# TO FINISH
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
