import asyncio
import logging

import pandas as pd

from pyfutures.client.client import InteractiveBrokersClient
from pyfutures.client.enums import BarSize
from pyfutures.client.enums import WhatToShow
from pyfutures.client.historic import InteractiveBrokersBarClient
from pyfutures.tests.test_kit import CACHE_DIR
from pyfutures.tests.test_kit import SPREAD_FOLDER
from pyfutures.tests.test_kit import IBTestProviderStubs
from pyfutures.tests.unit.client.stubs import ClientStubs


async def use_rt():
    """
    test data in this range is returned without RTH argument
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


async def write_spread_first_hour():
    end_time = pd.Timestamp.utcnow().floor(pd.Timedelta(days=1)) - pd.Timedelta(days=1)
    start_time = end_time - pd.Timedelta(days=128)
    print(start_time, end_time)
    client: InteractiveBrokersClient = ClientStubs.client(
        request_timeout_seconds=60 * 10,
        override_timeout=False,
        api_log_level=logging.ERROR,
    )

    historic = InteractiveBrokersBarClient(
        client=client,
        delay=0.5,
        use_cache=True,
        cache_dir=CACHE_DIR,
    )

    rows = IBTestProviderStubs.universe_rows(
        # filter=["ECO"],
    )
    row = rows[0]

    schedule = row.liquid_schedule

    open_times = schedule.to_open_range(
        start_date=start_time,
        end_date=end_time,
    )

    await client.connect()
    await client.request_market_data_type(4)

    for open_time in open_times[::-1]:
        bars: pd.DataFrame = await historic.request_bars(
            contract=row.contract_cont,
            bar_size=BarSize._5_SECOND,
            what_to_show=WhatToShow.BID,
            start_time=open_time,
            end_time=open_time + pd.Timedelta(hours=1),
            use_cache=True,
            as_dataframe=True,
        )
        print(bars)
        exit()


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

    rows = [r for r in rows if r.uname == "FTI"]
    assert len(rows) > 0

    await client.connect()
    await client.request_market_data_type(4)
    for i, row in enumerate(rows):
        # US Futures bundle. any us tickers

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
    asyncio.get_event_loop().run_until_complete(write_spread_first_hour())


# async def import_spread_using_bars():
#     """
#     sample a random 20 second bar within the liquid hours
#     elapsed 20 seconds:
#     """

#     # filter hours in the liquid sessions
#     rows = IBTestProviderStubs.universe_rows(
#         # filter=["DC"],
#     )

#     end_time = pd.Timestamp.utcnow().floor("1D")
#     start_time = end_time - pd.Timedelta(days=128)

#     client: InteractiveBrokersClient = ClientStubs.client(
#         request_timeout_seconds=60 * 10,
#         override_timeout=False,
#         api_log_level=logging.ERROR,
#     )
#     await client.connect()
#     await client.request_market_data_type(4)
#     for row in rows:

#         path = SPREAD_FOLDER / f"{row.uname}_spreads.parquet"
#         if path.exists():
#             print(f"Skipping {path}")
#             continue

#         schedule = row.liquid_schedule
#         hours = pd.date_range(start_time, end_time, freq="H")

#         hours = [
#             h for h in hours if schedule.is_open(h)
#         ]

#         average_spreads = []

#         for i, hour in enumerate(hours):

#             bar = None
#             attempts = 0
#             while bar is None:

#                 if attempts > 50:
#                     raise RuntimeError("Attempts maximum exceeded")

#                 random_second = random.randrange(0, 295, 5)
#                 _end_time = hour + pd.Timedelta(seconds=random_second)
#                 assert schedule.is_open(_end_time)

#                 print(f"Requesting {i}/{len(hours)}: {_end_time}. Attempt: {attempts}")

#                 try:
#                     start = time.perf_counter()
#                     bars_bid: pd.DataFrame = await client.request_bars(
#                         contract=row.contract_cont,
#                         bar_size=BarSize._5_SECOND,
#                         what_to_show=WhatToShow.BID,
#                         end_time=_end_time,
#                         duration=Duration(30, Frequency.SECOND),
#                     )
#                     if len(bars_bid) == 0:
#                         print(f"Bid bars missing in time range, retrying...")
#                         attempts += 1
#                         continue
#                     bars_ask: pd.DataFrame = await client.request_bars(
#                         contract=row.contract_cont,
#                         bar_size=BarSize._5_SECOND,
#                         what_to_show=WhatToShow.ASK,
#                         end_time=_end_time,
#                         duration=Duration(30, Frequency.SECOND),
#                     )
#                     if len(bars_ask) == 0:
#                         print(f"Ask bars missing in time range, retrying...")
#                         attempts += 1
#                         continue
#                     stop = time.perf_counter()
#                     print(f"Elapsed time: {stop - start:.2f}")
#                 except ClientException as e:
#                     print(str(e))
#                     attempts += 1
#                     continue
#                 except asyncio.TimeoutError as e:
#                     print(str(e.__class__.__name__))
#                     attempts += 1
#                     continue

#                 df: pd.DataFrame = _merge_dataframe(
#                     bars_bid=bars_bid,
#                     bars_ask=bars_ask,
#                 )
#                 print(df)

#                 if df.empty:
#                     print(f"Empty after merge. Retrying...")
#                     attempts += 1
#                     continue

#                 df = df[(df.bid != 0.0) & (df.ask != 0.0)]
#                 df = df[df.bid != df.ask]
#                 if df.empty:
#                     print(f"Filtering 0 and equal values resultedin empty dataframe. Retrying...")
#                     attempts += 1
#                     continue

#                 """
#                 BID_ASK
#                     open: Time average bid
#                     high: Max Ask
#                     low: Min Bid
#                     close: Time average ask
#                     volume: N/A
#                 """
#                 value = df.iloc[-1].ask - df.iloc[-1].bid
#                 average_spreads.append(value)
#                 print(f"Average spread: {value} for {_end_time}")
#                 await asyncio.sleep(0.5)


#         pd.Series(average_spreads).to_frame().to_parquet(path, index=False)

# async def import_spread_using_quotes():
#     """
#     sample a random 20 second bar within the liquid hours
#     elapsed 20 seconds:
#     """

#     # filter hours in the liquid sessions
#     rows = IBTestProviderStubs.universe_rows(
#         # filter=["DC"],
#     )

#     end_time = pd.Timestamp.utcnow().floor("1D")
#     start_time = end_time - pd.Timedelta(days=128)

#     client: InteractiveBrokersClient = ClientStubs.client(
#         request_timeout_seconds=60 * 10,
#         override_timeout=False,
#         api_log_level=logging.ERROR,
#     )
#     await client.connect()
#     await client.request_market_data_type(4)
#     for row in rows:

#         path = SPREAD_FOLDER / f"{row.uname}_spreads.parquet"
#         if path.exists():
#             print(f"Skipping {path}")
#             continue

#         schedule = row.liquid_schedule
#         hours = pd.date_range(start_time, end_time, freq="H")

#         hours = [
#             h for h in hours if schedule.is_open(h)
#         ]

#         average_spreads = []

#         for i, hour in enumerate(hours):

#             bar = None
#             attempts = 0
#             while bar is None:

#                 if attempts > 50:
#                     raise RuntimeError("Attempts maximum exceeded")

#                 random_second = random.randrange(0, 295, 5)
#                 _end_time = hour + pd.Timedelta(seconds=random_second)
#                 assert schedule.is_open(_end_time)

#                 print(f"Requesting {i}/{len(hours)}: {_end_time}. Attempt: {attempts}")

#                 try:
#                     start = time.perf_counter()
#                     bars_bid: pd.DataFrame = await client.request_bars(
#                         contract=row.contract_cont,
#                         bar_size=BarSize._5_SECOND,
#                         what_to_show=WhatToShow.BID,
#                         end_time=_end_time,
#                         duration=Duration(30, Frequency.SECOND),
#                     )
#                     if len(bars_bid) == 0:
#                         print(f"Bid bars missing in time range, retrying...")
#                         attempts += 1
#                         continue
#                     stop = time.perf_counter()
#                     print(f"Elapsed time: {stop - start:.2f}")
#                 except ClientException as e:
#                     print(str(e))
#                     attempts += 1
#                     continue
#                 except asyncio.TimeoutError as e:
#                     print(str(e.__class__.__name__))
#                     attempts += 1
#                     continue

#                 quote = quotes[-1]
#                 value = df.iloc[-1].ask - df.iloc[-1].bid
#                 average_spreads.append(value)
#                 print(f"Average spread: {value} for {_end_time}")
#                 await asyncio.sleep(0.5)


#         pd.Series(average_spreads).to_frame().to_parquet(path, index=False)

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
