import pytest
from pyfutures.adapters.interactive_brokers.historic import InteractiveBrokersHistoric
import gc
import random
import time
import pandas as pd

from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.tests.adapters.interactive_brokers.test_kit import SPREAD_FOLDER
from pyfutures.adapters.interactive_brokers.enums import BarSize
from pyfutures.adapters.interactive_brokers.enums import Frequency
from pyfutures.adapters.interactive_brokers.enums import WhatToShow
from pyfutures.adapters.interactive_brokers.enums import Duration
        

@pytest.mark.asyncio()
async def test_import_spread(client):
    
    """
    Export tick history for every instrument of hte universe
    Make one of the markets a liquid one like ZN
    And an illiquid one like Aluminium
    """
    
    rows = IBTestProviderStubs.universe_rows(filter=["ZN"])
    historic = InteractiveBrokersHistoric(client=client, delay=1)
    start_time = (pd.Timestamp.utcnow() - pd.Timedelta(days=128)).floor("1D")
    end_time = (pd.Timestamp.utcnow() - pd.Timedelta(days=1)).floor("1D")
    
    await client.connect()
    
    for row in rows:
        
        print(f"Processing {row}")
        df = await historic.request_bars2(
            contract=row.contract_cont,
            bar_size=BarSize._1_MINUTE,
            what_to_show=WhatToShow.BID_ASK,
            start_time=start_time,
            end_time=end_time,
            as_dataframe=True,
        )
        print(df)
        print(f"Exporting {row.uname}")
        
        path = SPREAD_FOLDER / (row.uname + ".parquet")
        path.parent.mkdir(exist_ok=True, parents=True)
        df.to_parquet(path, index=False)
        del df
        gc.collect()

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
        
    
    
    
    
    
        
    
    