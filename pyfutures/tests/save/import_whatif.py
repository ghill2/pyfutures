import asyncio
from ibapi.order import Order as IBOrder
from pyfutures.tests.test_kit import IBTestProviderStubs
from pyfutures.client.client import InteractiveBrokersClient
from pytower.stats.cache import async_cache_json_daily
import pandas as pd
from pyfutures.tests.test_kit import UNIVERSE_WHATIF_CSV_PATH


async def import_whatif():
    """
    Do not run this with Trader WorkStation
    Otherwise it produces error:
    - Parameters in request conflicts with contract parameters received by contract id: requested expiry 20240515 12:00:00 GB,
    - For ICEEUSOFT, C
    """
    client = InteractiveBrokersClient(loop=asyncio.get_running_loop())
    await client.connect()

    # universe_df = IBTestProviderStubs.universe_dataframe()
    # df = universe_df.iloc[:, :4]
    # df["initMarginChange"] = [None] * len(df)
    # df["commission"] = [None] * len(df)
    # df["commissionCurrency"] = [None] * len(df)

    # iterate universe rows, then just add to dataframe
    #
    df = pd.DataFrame(
        columns=[
            "uname",
            "trading_class",
            "symbol",
            "exchange",
            "initMarginChange",
            "commission",
            "commissionCurrency",
        ]
    )

    for index, universe_row in IBTestProviderStubs.universe_dataframe(
        merge=False
    ).iterrows():
        # if row.exchange != "NYMEX" and row.symbol != "NG":
        # continue
        row = dict(
            uname=universe_row.uname,
            # trading_class=universe_row.trading_class,
            # symbol=universe_row.symbol,
            # exchange=universe_row.exchange,
        )

        try:
            details = await client.request_contract_details(universe_row.contract)

            # solves The contract's last trading time has passed error code 201
            # if the contract is expiring today, a qualified contract will have "" as lastTradeTime
            if details[0].lastTradeTime == "":
                print("========== LAST TRADE TIME NONE =============")
                detail = details[1]
            else:
                detail = details[0]

            whatif = await client.request_whatif(detail)
        except asyncio.TimeoutError as e:
            # Errors on Crypto instrument CME|MBT
            row["initMarginChange"] = None
            row["commission"] = None
            row["commissionCurrency"] = None
            print(e)
        else:
            print(whatif)
            row["initMarginChange"] = whatif["initMarginChange"]
            row["commission"] = whatif["commission"]
            row["commissionCurrency"] = whatif["commissionCurrency"]

        df.loc[index] = row

    print(df)
    df.to_csv(UNIVERSE_WHATIF_CSV_PATH)


asyncio.run(import_whatif())
