import pandas as pd
import random
import joblib
from pyfutures.tests.test_kit import IBTestProviderStubs
from pyfutures.tests.test_kit import SPREAD_FOLDER

"""
once per hour should be enough
"""

def get_spread_value(row):
    path = SPREAD_FOLDER / f"{row.trading_class}.parquet"
        
    df = pd.read_parquet(path)
    
    # print(len(df))
    # TODO: why is this the same length after filtering by liquid hours
    mask = df.timestamp.apply(row.liquid_schedule.is_open)
    df = df[mask]
    
    # print(len(df))
    # Group by date and hour
    grouped = df.groupby(pd.Grouper(key='timestamp', freq='H'))
    
    """
    BID_ASK:
        open: Time average bid
        high: Max Ask
        Low: Min Bid
        Close: Time average ask
        volume: N/A
    """
    
    values = []
    for group_key, df in grouped:
        if df.empty:
            continue
        
        if row.trading_class == "ZC":
            print(df)
            exit()
        
        random_row = df.sample().iloc[0]
        average_spread = random_row.close - random_row.open
        values.append(average_spread)
    
    average_spread: float = pd.Series(values).mean()
    i = 7
    if average_spread == 0.0:
        print(f"{row.trading_class}: {average_spread:.{i}f} failed")
    else:
        print(f"{row.trading_class}: {average_spread:.{i}f}")
        
if __name__ == "__main__":
    
    rows = IBTestProviderStubs.universe_rows(
        # filter=["ECO"],
    )
    
    results = joblib.Parallel(n_jobs=-1, backend="loky") \
        (joblib.delayed(get_spread_value)(row) for row in rows)
    
    # for i, row in enumerate(rows):
        # 0     2023-11-01 18:19:00+00:00  1698862740  380.75  380.75  380.00  380.75     -1  -100        -1
        
            
        
    
        # load bars as dataframe
    
        # filter bars in liquid hours
        
        # return dataframe for each hour
        
        # return a random row in the dataframe for each dataframe
        
        # average all random rows
        
        