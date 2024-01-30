from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.adapters.interactive_brokers.parsing import row_to_contract
from nautilus_trader.model.identifiers import InstrumentId
from pyfutures.adapters.interactive_brokers.client.objects import ClientException
import pandas as pd
import pytest
from pathlib import Path
from ibapi.contract import Contract
from ibapi.contract import ContractDetails


@pytest.mark.asyncio()
async def test_timezones():
    
    universe = IBTestProviderStubs.universe_dataframe()
    for row in universe.itertuples():
        print()
        
        
        
@pytest.mark.asyncio()
async def test_request_front_contract_universe(client):
    """
    print out instrument in the universe where the front contract fails to be requested
    """
    await client.connect()
    
    universe = IBTestProviderStubs.universe_dataframe()
    timezones = []
    
    for row in universe.itertuples():
        
        contract = row_to_contract(row)
        
        try:
            details = await client.request_front_contract_details(contract)
            
            assert type(details) is ContractDetails
            timezones.append(details.timeZoneId)
            print(row.trading_class, details.timeZoneId)
            
        except ClientException as e:
            if e.code == 200:
                timezones.append("None")
                print(row.trading_class, details.timeZoneId)
            else:
                raise e
        
@pytest.mark.asyncio()
async def test_request_front_contract_universe_fix(client):
    """
    print out instrument in the universe where the front contract fails to be requested
    """
    await client.connect()
        
    contract = Contract()
    contract.symbol = "XT"
    contract.tradingClass = "XT"
    contract.exchange = "SNFE"
    contract.secType = "FUT"
    contract.includeExpired = False
    
    try:
        contract = await client.request_front_contract(contract)
        assert type(contract) is Contract
    except ClientException as e:
        if e.code == 200:
            print(f"{row.trading_class}")
        else:
            raise e

def test_historic_schedules_with_sessions_out_of_day():
    """
    find instruments that have sessions where the start and end date is NOT within the same day
    """
    
    schedules_dir = Path("/Users/g1/BU/projects/pytower_develop/pyfutures/pyfutures/schedules")
    universe = IBTestProviderStubs.universe_dataframe()
    
    for row in universe.itertuples():
        
        if row.trading_class == "COIL_Z":
            continue
        
        path = schedules_dir / f"{row.trading_class}.parquet"
        if path.exists():
            df = pd.read_parquet(path)
            if not (df.start.dt.date == df.end.dt.date).all():
                print(row.trading_class)
                # print(df)
        

def find_minimum_day_of_month_within_range(
    start: pd.Timestamp | str,
    end: pd.Timestamp | str,
    dayname: str, # Friday, Tuesday etc
    dayofmonth: int, # 1, 2, 3, 4 etc
):
    
    # find minimum x day of each month within a date range
    import pandas as pd
    df = pd.DataFrame({'date': pd.date_range(start=start, end=end)})
    days = df[df['date'].dt.day_name() == dayname]

    data = {}
    for date in days.date:
        
        key = f"{date.year}{date.month}"
        if data.get(key) is None:
            data[key] = []
            
        data[key].append(date)
        
    filtered = []
    for key, values in data.items():
        filtered.append(values[dayofmonth - 1])
    
    return pd.Series(filtered).dt.day.min()

def test_find_problem_files():
    """
    find trading_classes where the files do have the hold cycle in every year
    """
    universe = IBTestProviderStubs.universe_dataframe()
    data_folder = Path("/Users/g1/Desktop/portara data george/DAY")
    for row in universe.itertuples():
        
        data_dir = (data_folder / row.data_symbol)
        paths = list(data_dir.glob("*.txt")) \
                    + list(data_dir.glob("*.b01")) \
                    + list(data_dir.glob("*.bd"))
                    
        paths = list(sorted(paths))
        assert len(paths) > 0
        
        start_month = ContractMonth(row.data_start)
        end_month = ContractMonth(row.data_end)
        required_months = []
        
        cycle = RollCycle.from_str(row.hold_cycle)
        
        while True:
            required_months.append(start_month)
            start_month = cycle.next_month(start_month)
            if start_month >= end_month:
                break
        
        stems = [
            x.stem[-5:] for x in paths
        ]
        for month in required_months:
            if month.value in stems:
                continue
            print(row.trading_class, month.value)
            
if __name__ == "__main__":
    test_historic_schedules_with_sessions_out_of_day()