# from pyfutures.adapters.interactive_brokers.client.objects import IBFuturesInstrument
import asyncio
import json

from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pyfutures.adapters.interactive_brokers.historic import InteractiveBrokersHistoric
from pytower import PACKAGE_ROOT
from pathlib import Path

import pandas as pd
import pytest
from ibapi.contract import Contract as IBContract

from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from unittest.mock import Mock
from pyfutures.adapters.interactive_brokers.client.objects import ClientException

from nautilus_trader.common.component import init_logging
from nautilus_trader.common.enums import LogLevel

init_logging(level_stdout=LogLevel.WARNING)


@pytest.mark.asyncio()
async def test_request_last_bar_universe(client):
    
    rows = IBTestProviderStubs.universe_rows()
    
    await client.connect()
    
    await client.request_market_data_type(4)
    
    asyncio.sleep(2)

    missing = []
    for row in rows:
        print(row.instrument_id)
        contract = row.contract_cont
        try:
            last_bar = await client.request_last_bar(
                contract=contract,
                bar_size=BarSize._1_MINUTE,
                what_to_show=WhatToShow.BID_ASK,
            )
            print("last_bar: ", last_bar)
        except ClientException as e:
            print(e)
            missing.append(row)

    for row in missing:
        print(row.trading_class, row.exchange)


@pytest.mark.asyncio()
async def test_request_last_quote_tick_universe(client):
    """
    Find missing subscriptions in the universe
    """
    rows = IBTestProviderStubs.universe_rows()

    missing = []
    await client.connect()
    for row in rows:
        contract = row.contract_cont

        try:
            last = await client.request_last_quote_tick(
                contract=contract,
            )
        except ClientException as e:
            if e.code == 10187:
                missing.append(row)
            else:
                print(e)
                print(row)
                raise

    for row in missing:
        print(row.trading_class, row.exchange)


from pyfutures.adapters.interactive_brokers.client.objects import IBBar
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Duration
from pyfutures.adapters.interactive_brokers.enums import Frequency
from pyfutures.adapters.interactive_brokers.enums import WhatToShow


@pytest.mark.asyncio()
async def test_request_bars_universe(client):
    rows = IBTestProviderStubs.universe_rows()

    historic = InteractiveBrokersHistoric(client=client)
    await client.connect()
    client._client.reqMarketDataType(2)
    await asyncio.sleep(2)
    for row in rows:
        print(f"====== NEW INSTUMENT {row.contract=} =====")
        contract = row.contract_cont

        bars = await historic.request_bars2(
            contract=contract,
            bar_size=BarSize._1_DAY,
            what_to_show=WhatToShow.BID,
        )

        for bar in bars:
            print(bar)
        exit()
        # assert all(isinstance(bar, IBBar) for bar in bars)
        # assert len(bars) > 0


@pytest.mark.asyncio()
async def test_request_start_quote_tick_universe(client):
    rows = IBTestProviderStubs.universe_rows()

    missing = []
    await client.connect()
    for row in rows:
        contract = row.contract_cont

        try:
            first = await client.request_first_quote_tick(
                contract=contract,
            )
        except ClientException as e:
            print(e)
            print(row)
            raise

        if first is None:
            missing.append(row)

    for row in missing:
        print(row.trading_class, row.exchange)


@pytest.mark.asyncio()
async def test_request_price_magnifier(client):
    await client.connect()

    contract = IBContract()
    contract.tradingClass = "NIFTY"
    contract.symbol = "NIFTY50"
    contract.exchange = "NSE"
    contract.includeExpired = False
    contract.secType = "FUT"

    details = await client.request_front_contract_details(contract)
    print(details.priceMagnifier)
    print(details.minTick)


@pytest.mark.asyncio()
async def test_can_just_use_trading_class(client):
    await client.connect()

    universe = IBTestProviderStubs.universe_dataframe()

    data = []
    for row in universe.itertuples():
        print(row)
        contract = IBContract()
        contract.symbol = row.symbol.replace("_", ".")
        contract.exchange = row.exchange.replace("_", ".")
        contract.tradingClass = row.trading_class.replace("_", ".")
        contract.includeExpired = False
        contract.secType = "FUT"

        details = await client.request_front_contract_details(contract)
        data.append(f"{details.minTick:.8f}".rstrip("0"))

    for x in data:
        print(x)

        # print(details.priceMagnifier)
        # print(details.minTick)


async def test_fmeu(client):
    await client.connect()

    contract = IBContract()
    contract.symbol = row.symbol.replace("_", ".")
    contract.exchange = row.exchange.replace("_", ".")
    contract.tradingClass = row.trading_class.replace("_", ".")
    contract.includeExpired = False
    contract.secType = "FUT"


# @pytest.mark.skip(reason="research")
@pytest.mark.asyncio()
async def test_trading_class(client):
    """
    Find which instruments in the universe have more than one trading class
    """
    await client.connect()

    df = IBTestProviderStubs.universe_dataframe()

    data = []
    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.width",
        None,
    ):
        for i in range(len(df)):
            symbol = df.iloc[i].symbol
            exchange = df.iloc[i].exchange
            contract = IBContract()
            contract.secType = "FUT"
            contract.symbol = symbol.replace("_", ".")
            contract.exchange = exchange.replace("_", ".")
            contract.includeExpired = False
            await asyncio.sleep(0.4)
            details_list = await client.request_contract_details(contract)
            details_list = sorted(details_list, key=lambda x: x.contractMonth)
            trading_classes = {x.contract.tradingClass for x in details_list}
            if len(trading_classes) > 1:
                print(len(trading_classes), len(set(trading_classes)))
                print(trading_classes)
                print(contract)
                exit()
            data.append(next(iter(trading_classes)))
        else:
            data.append(df.iloc[i].trading_class)

        df["trading_class"] = data
        print(df)

    path = PACKAGE_ROOT / "instruments/universe2.csv"
    df.to_csv(path, index=False)


@pytest.mark.skip(reason="research")
@pytest.mark.asyncio()
async def test_weekly_contracts(client):
    """
    Find instruments that have special contracts and need special handling.
    """
    # data = {}
    # for details in IBTestProviderStubs.universe_contract_details():
    #     data.setdefault(details.contract.symbol, []).append(details)
    matched = []
    for instrument_id in IBTestProviderStubs.universe_instrument_ids():
        contract = IBContract()
        contract.secType = "FUT"
        contract.symbol = instrument_id.symbol.value.replace("_", ".")
        contract.exchange = instrument_id.venue.value.replace("_", ".")
        contract.includeExpired = False

        details_list = await client.request_contract_details(contract)

        await asyncio.sleep(0.4)

        details_list = sorted(details_list, key=lambda x: x.contractMonth)

        contract_months = [x.contractMonth for x in details_list]
        if len(contract_months) == len(set(contract_months)):
            continue

        matched.append(details_list)

    for details_list in matched:
        for details in details_list:
            print(
                # details.contract.symbol,
                " | ".join(
                    map(
                        str,
                        [
                            details.contract.localSymbol,
                            details.contract.lastTradeDateOrContractMonth,
                            details.contractMonth,
                            details.realExpirationDate,
                            # details.contract.tradingClass,
                            # details.contractMonth,
                            details.contract.primaryExchange,
                            details.contract.secType,
                            details.contract.secIdType,
                            details.contract.secId,
                            details.contract.description,
                            details.contract.issuerId,
                            details.contract.multiplier,
                            details.contract.currency,
                            details.longName,
                            details.underConId,
                            details.marketName,
                            details.stockType,
                        ],
                    ),
                ),
            )


@pytest.mark.skip(reason="research")
@pytest.mark.asyncio()
async def test_universe_price_parameters():
    folder = "/Users/g1/BU/projects/pytower_develop/pytower/tests/adapters/interactive_brokers/responses/universe_info"

    for path in Path(folder).glob("*.json"):
        data = json.loads(path.read_text())

        params = data.get("Price Parameters")
        symbol = data["Symbol"]
        exchange = data["Exchange"]

        if params is None:
            print(f"Missing {symbol}")
        elif len(params) > 1:
            print(symbol, exchange, params)


@pytest.mark.skip(reason="research")
@pytest.mark.asyncio()
async def test_universe_price_magnifiers():
    magnifiers = set()
    symbols = set()
    for details in IBTestProviderStubs.universe_contract_details():
        # details = await instrument_provider.load_async(contract)
        magnifiers.add(details.priceMagnifier)
        if details.priceMagnifier == 100:
            symbols.add(details.contract.symbol)

    print(magnifiers)
    print(symbols)
