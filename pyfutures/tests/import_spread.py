import asyncio
import logging

import pandas as pd
import random
from pyfutures.client.enums import BarSize
from pyfutures.client.enums import WhatToShow
from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.client.historic import InteractiveBrokersBarClient
from pyfutures.tests.test_kit import CACHE_DIR
from pyfutures.tests.test_kit import SPREAD_FOLDER
from pyfutures.tests.test_kit import IBTestProviderStubs
from pyfutures.tests.unit.client.stubs import ClientStubs
from pyfutures.client.enums import Frequency

async def import_spread():
    """
    sample a random 20 second bar within the liquid hours
    """
    
    # filter hours in the liquid sessions
    rows = IBTestProviderStubs.universe_rows(
        filter=["DC"],
    )
    row = rows[0]
    
    end_time = pd.Timestamp.utcnow().floor("1D")
    start_time = end_time - pd.Timedelta(days=128)
    schedule = row.liquid_schedule
    hours = pd.date_range(start_time, end_time, freq="H")
    
    hours = [
        h for h in hours if schedule.is_open(h)
    ]
    
    client: InteractiveBrokersClient = ClientStubs.client(
        request_timeout_seconds=60 * 10,
        override_timeout=False,
        api_log_level=logging.ERROR,
    )
    
    bars = []
    for hour in hours:
        
        bar = None
        while bar is None:
            
            random_second = random.randrange(0, 295, 5)
            _end_time = hour + pd.Timedelta(seconds=random_second)
            assert schedule.is_open(_end_time)
            bars: pd.DataFrame = await client.request_bars(
                contract=row.contract_cont,
                bar_size=BarSize._5_SECOND,
                what_to_show=WhatToShow.BID_ASK,
                end_time=_end_time,
                duration=Duration(10, Frequency.SECOND),
            )
            
            if len(bars) > 0:
                bar = bars[-1]
                
        bars.append(bar)
    print(len(bars))
    
    
    
    
    
    
    
    
async def write_spread(write: bool = False):
    client: InteractiveBrokersClient = ClientStubs.client(
        request_timeout_seconds=60 * 10,
        override_timeout=False,
        api_log_level=logging.ERROR,
    )

    rows = IBTestProviderStubs.universe_rows(
        # filter=["ECO"],
    )
    historic = InteractiveBrokersBarClient(
        client=client,
        delay=0.5,
        use_cache=True,
        cache_dir=CACHE_DIR,
    )

    start_time = (pd.Timestamp.utcnow() - pd.Timedelta(days=128)).floor("1D")

    # historic.cache.purge_errors(asyncio.TimeoutError)

    rows = [r for r in rows if r.uname == "FTI"]
    print(rows)
    assert len(rows) > 0

    await client.connect()
    await client.request_market_data_type(4)
    for i, row in enumerate(rows):
        print(f"Processing {i}/{len(rows)}: {row.uname}: BID...")
        bars: pd.DataFrame = await historic.request_bars(
            contract=row.contract_cont,
            bar_size=BarSize._1_MINUTE,
            what_to_show=WhatToShow.BID,
            start_time=start_time,
            as_dataframe=write,
            skip_first=True,
        )
        if write:
            bars.to_parquet(SPREAD_FOLDER / f"{row.uname}_BID.parquet", index=False)

        print(f"Processing {i}/{len(rows)}: {row.uname}: ASK...")
        bars: pd.DataFrame = await historic.request_bars(
            contract=row.contract_cont,
            bar_size=BarSize._1_MINUTE,
            what_to_show=WhatToShow.ASK,
            start_time=start_time,
            as_dataframe=write,
            skip_first=True,
        )
        if write:
            bars.to_parquet(SPREAD_FOLDER / f"{row.uname}_ASK.parquet", index=False)


if __name__ == "__main__":
    # write_spread(write=True)
    asyncio.get_event_loop().run_until_complete(import_spread())

# @pytest.mark.asyncio()
# async def test_export_spread_daily(client):
#     """
#     Export tick history for every instrument of hte universe
#     Make one of the markets a liquid one like ZN
#     And an illiquid one like Aluminium
#     """
#     rows = IBTestProviderStubs.universe_rows(
#         # filter=["ECO"],
#     )
#     historic = InteractiveBrokersHistoric(client=client, delay=2)
#     # start_time = (pd.Timestamp.utcnow() - pd.Timedelta(days=128)).floor("1D")
#     end_time = (pd.Timestamp.utcnow() - pd.Timedelta(days=1)).floor("1D")
#
#     await client.connect()
#     # await client.request_market_data_type(1)
#     for row in rows[30:]:
#         print(f"Processing {row}")
#         bars = await historic.request_bars(
#             contract=row.contract_cont,
#             bar_size=BarSize._1_DAY,
#             what_to_show=WhatToShow.BID_ASK,
#             end_time=end_time,
#             as_dataframe=True,
#             cache=True,
#         )
#         print(bars)
#         break


# @pytest.mark.asyncio()
# async def test_export_spread(client):
#     """
#     Export tick history for every instrument of hte universe
#     Make one of the markets a liquid one like ZN
#     And an illiquid one like Aluminium
#     """
#     rows = IBTestProviderStubs.universe_rows(
#         # filter=["ECO"],
#     )
#     historic = InteractiveBrokersHistoric(client=client, delay=2)
#     # start_time = (pd.Timestamp.utcnow() - pd.Timedelta(days=128)).floor("1D")
#     end_time = (pd.Timestamp.utcnow() - pd.Timedelta(days=1)).floor("1D")
#
#     await client.connect()
#     await client.request_market_data_type(4)
#     for row in rows[0:]:
#         path = SPREAD_FOLDER / (row.uname + ".parquet")
#         if path.exists():
#             print(f"Skipping {row}")
#             continue
#
#         print(f"Processing {row}")
#         df = await historic.request_bars(
#             contract=row.contract_cont,
#             bar_size=BarSize._1_MINUTE,
#             what_to_show=WhatToShow.BID_ASK,
#             end_time=end_time,
#             as_dataframe=True,
#         )
#         assert not df.empty
#         df = df.rename({"date": "timestamp"}, axis=1)
#         df = df[["timestamp", "open", "high", "low", "close", "volume"]]
#         df.volume = 1.0
#
#         print(f"Exporting {row.uname}")
#
#         path.parent.mkdir(exist_ok=True, parents=True)
#         # df.to_parquet(path, index=False)
#
#         file = ParquetFile(
#             parent=SPREAD_FOLDER,
#             bar_type=BarType.from_str(f"{row.instrument_id}-1-MINUTE-MID-EXTERNAL"),
#             cls=Bar,
#         )
#
#         writer = BarParquetWriter(
#             path=file.path,
#             bar_type=file.bar_type,
#             price_precision=row.price_precision,
#             size_precision=1,
#         )
#
#         writer.write_dataframe(df)
#
#         del df
#         gc.collect()

# @pytest.mark.asyncio()
# async def test_import_spread(client):

#     """
#     Export tick history for every instrument of hte universe
#     Make one of the markets a liquid one like ZN
#     And an illiquid one like Aluminium
#     """

#     rows = IBTestProviderStubs.universe_rows(filter=["ZN"])
#     historic = InteractiveBrokersHistoric(client=client, delay=1)
#     start_time = (pd.Timestamp.utcnow() - pd.Timedelta(days=365)).floor("1D")
#     end_time = (pd.Timestamp.utcnow() - pd.Timedelta(days=1)).floor("1D")

#     await client.connect()

#     for row in rows:

#         print(f"Processing {row}")
#         df = await historic.request_quote_ticks(
#             contract=row.contract_cont,
#             start_time=start_time,
#             end_time=end_time,
#             as_dataframe=True,
#         )

#         print(f"Exporting {row.instrument_id}")

#         path = SPREAD_FOLDER / (row.uname + ".parquet")
#         path.parent.mkdir(exist_ok=True, parents=True)
#         # df.to_parquet(path, index=False)
#         del df
#         gc.collect()

# df = pd.DataFrame()

# for session in sessions.itertuples():
# sessions: pd.DataFrame = row.liquid_schedule.sessions(start_date=start_date)

# async def test_import_spread(client):

#     """
#     so sample one tick every hour in the liquid session
#     """

#     rows = IBTestProviderStubs.universe_rows()

#     await client.connect()

#     for row in rows:

#         contract = await client.request_front_contract(row.contract_cont)

#         times = row.liquid_schedule.to_date_range(
#             start_date=pd.Timestamp.utcnow() - pd.Timedelta(days=365),
#             interval=pd.Timedelta(hours=1),
#         )
#         times = times[::-1]

#         seconds_in_hour = 3600
#         milliseconds_in_hour = 3_600_000

#         spreads = []
#         for i, ts in enumerate(times):

#             random_second = random.randint(0, seconds_in_hour - 2)
#             start_time = ts + pd.Timedelta(seconds=random_second)
#             end_time = ts + pd.Timedelta(hours=1)

#             quotes = await client.request_quote_ticks(
#                 name=str(UUID4()),
#                 contract=contract,
#                 start_time=start_time,
#                 end_time=end_time,
#                 count=1,
#             )
#             if len(quotes) == 0:
#                 continue

#             quote = quotes[0]

#             spread = quote.ask_price - quote.bid_price
#             spreads.append(spread)
#             print(f"{i}/{len(times)} {row.instrument_id} {start_time} {spread}")
#             time.sleep(0.5)  # 2000 requests, 0.2sec, 6.6 minutes

#         average_spread = float(pd.Series(spreads).mean())

#         print(f"Exporting {row.instrument_id}")

#         path = SPREAD_FOLDER / (row.uname + ".txt")
#         path.parent.mkdir(exist_ok=True, parents=True)
#         with open(path, 'w') as f:
#             f.write(str(average_spread))

#         path = SPREAD_FOLDER / (row.uname + ".parquet")
#         df = pd.DataFrame(spreads)
#         df.to_parquet(path, index=False)
