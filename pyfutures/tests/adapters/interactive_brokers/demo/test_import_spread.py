import pytest
from pyfutures.adapters.interactive_brokers.historic import InteractiveBrokersHistoric
import random
import time
import pandas as pd

from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.tests.adapters.interactive_brokers.test_kit import SPREAD_FOLDER

@pytest.mark.asyncio()
async def test_import_spread(client):
    
    """
    so sample one tick every hour in the liquid session
    Make one of the markets a liquid one like ZN
    And an illiquid one like Aluminium
    """
    
    rows = IBTestProviderStubs.universe_rows(filter=["ZN"])
    
    start_date = (pd.Timestamp.now() - pd.Timedelta(days=365)).floor("1D")
    
    await client.connect()
    
    historic = InteractiveBrokersHistoric(client=client, delay=2)
    
    for row in rows:
    
        contract = row.contract_cont
        sessions: pd.DataFrame = row.liquid_schedule.sessions(start_date=start_date)
        
        df = pd.DataFrame()
        
        for session in sessions.itertuples():
            
            ndf = await historic.request_quote_ticks(
                contract=contract,
                start_time=session.start,
                end_time=session.end,
                as_dataframe=True,
            )
            print(session.start, session.end, len(ndf))
            
            if len(ndf) == 0:
                continue
            
            df = pd.concat([df, ndf])
            
        print(f"Exporting {row.instrument_id}")
        
        path = SPREAD_FOLDER / (row.uname + ".txt")
        path.parent.mkdir(exist_ok=True, parents=True)
        df.to_csv(path, index=False)
            
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
        
    
    
    
    
    
        
    
    