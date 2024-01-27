# from pyfutures.adapters.interactive_brokers.client.objects import IBFuturesInstrument
import asyncio
import json
from pytower import PACKAGE_ROOT
from pathlib import Path

import pandas as pd
import pytest
from ibapi.contract import Contract as IBContract

from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from unittest.mock import Mock

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