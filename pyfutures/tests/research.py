import asyncio
from pathlib import Path

from ibapi.contract import Contract as IBContract

from pyfutures.tests.test_kit import IBTestProviderStubs


async def use_rt():
    """
    test data with use_rth returns different data where the product listing has liquid hours
    2024-02-27 00:00:00 > 2024-02-27 06:30:00
    12:30 > 15:30 = Japan
    03:30 > 06:30 = UTC

    00:00 > 06.30 Japan
    15:00 >  21:00
    """
    client: InteractiveBrokersClient = ClientStubs.client(
        request_timeout_seconds=60 * 10,
        override_timeout=False,
        api_log_level=logging.ERROR,
    )

    rows = IBTestProviderStubs.universe_rows(
        filter=["JBLM"],
    )
    row = rows[0]

    historic = InteractiveBrokersBarClient(
        client=client,
        delay=0.5,
        use_cache=False,
        cache_dir=CACHE_DIR,
    )

    await client.connect()
    await client.request_market_data_type(4)

    print(f"Processing {row.uname}: BID...")

    df: pd.DataFrame = await historic.request_bars(
        contract=row.contract_cont,
        bar_size=BarSize._1_MINUTE,
        what_to_show=WhatToShow.BID,
        start_time=pd.Timestamp("2024-01-23", tz="UTC"),  # Tuesday
        end_time=pd.Timestamp("2024-01-29", tz="UTC"),  # Friday
        as_dataframe=True,
        skip_first=False,
    )
    df.sort_values(by="timestamp", inplace=True)
    df.reset_index(inplace=True, drop=True)
    with pd.option_context(
        "display.max_rows",
        None,
        "display.max_columns",
        None,
        "display.width",
        None,
    ):
        df["dayofweek"] = df.timestamp.dt.dayofweek
        print(df)
        
async def find_universe_weekly_contracts(client):
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


async def find_universe_price_parameters():
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


async def find_universe_price_magnifiers():
    magnifiers = set()
    symbols = set()
    for details in IBTestProviderStubs.universe_contract_details():
        # details = await instrument_provider.load_async(contract)
        magnifiers.add(details.priceMagnifier)
        if details.priceMagnifier == 100:
            symbols.add(details.contract.symbol)

    print(magnifiers)
    print(symbols)
    # {1, 100}
