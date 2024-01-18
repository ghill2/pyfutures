
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.continuous.wrangler import ContinuousPriceWrangler
from nautilus_trader.core.datetime import unix_nanos_to_dt
from pyfutures.continuous.chain import FuturesChain
from pyfutures.continuous.config import FuturesChainConfig
from pyfutures.continuous.cycle import RollCycle
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.data import ContinuousData
from pytower import PACKAGE_ROOT
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.core.datetime import dt_to_unix_nanos
import pandas as pd
from pathlib import Path
from pyfutures.continuous.contract_month import ContractMonth
from nautilus_trader.cache.cache import Cache
from pyfutures.continuous.price import ContinuousPrice
from nautilus_trader.common.clock import TestClock
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.common.component import MessageBus
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from nautilus_trader.common.logging import Logger
from nautilus_trader.config import DataEngineConfig
from nautilus_trader.data.engine import DataEngine
from pytower.data.writer import ContinuousPriceParquetWriter
from pytower.data.files import ParquetFile
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.data import BarType
from pathlib import Path
import pandas as pd
import pytest
import numpy as np
import pytest
import joblib
from nautilus_trader.model.data import Bar
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity

"""
TODO: test if contract expires it take next forward price
TODO: test if forward contract expires before current contract expiry date
"""
        
def process_row(
    trading_class: str,
    symbol: str,
    hold_cycle: str,
    priced_cycle: str,
    roll_offset: str,
    approximate_expiry_offset: int,
    carry_offset: int,
    start: str,
    missing_months: list[str] | None,
):
    
    # load all the data for the data symbol
    instrument_id = InstrumentId.from_str(f"{trading_class}_{symbol}.IB")
    bar_type = BarType.from_str(f"{instrument_id}-1-MINUTE-MID-EXTERNAL")
    
    file = ParquetFile(
        parent=Path("/Users/g1/Desktop/continuous/data/genericdata_continuous_price"),
        bar_type=bar_type,
        cls=ContinuousPrice,
    )
    
    if file.path.exists():
        print(f"Skipping {trading_class}")
        return
    
    print(f"Processing {trading_class}")
    
    start_month = ContractMonth(start)
    
    # ignore_failed = list(map(ContractMonth, ignore_failed))
    wrangler = ContinuousPriceWrangler(
        bar_type=bar_type,
        start_month=start_month,
        config=FuturesChainConfig(
            instrument_id=instrument_id,
            hold_cycle=hold_cycle,
            priced_cycle=priced_cycle,
            roll_offset=roll_offset,
            approximate_expiry_offset=approximate_expiry_offset,
            carry_offset=carry_offset,
            skip_months=missing_months,
        ),
    )
    
    
    keyword = f"{trading_class}_{symbol}=*.IB*.parquet"
    paths = list(sorted(Path("/Users/g1/Desktop/output").glob(keyword)))
    assert len(paths) > 0
    
    # filter the paths by the hold cycle and start year
    bars = []
    for path in paths:
        bars.extend(ParquetFile.from_path(path).read_objects())
    assert len(bars) > 0
    
    # add extra bars
    if trading_class == "FESB":
        "FESB 2007U - 20070615, 1621, 480.00, 482,00, 479.30, 482.00, 1, 20"
        
        bars.append(
            Bar(
                BarType.from_str("FESB_SX7E=2007U.IB-1-MINUTE-MID-EXTERNAL"),
                open=Price.from_str("480.00"),
                high=Price.from_str("482.00"),
                low=Price.from_str("479.30"),
                close=Price.from_str("482.00"),
                volume=Quantity.from_str("20.0"),
                ts_init=dt_to_unix_nanos(pd.Timestamp("2007-06-14 16:48:00", tz="UTC")),
                ts_event=dt_to_unix_nanos(pd.Timestamp("2007-06-14 16:48:00", tz="UTC")),
            )
        )
    # data[ContractMonth("2007U")] = [bar] + data[ContractMonth("2007U")]
    elif trading_class == "FESU":
        """
        20191221, time 282.40, 283.80, 280.90, 283.80, 1, 13
        20191227, time, 280.00, 280.60, 273.40, 275.20, 1, 10
        2018-12-21 17:53:00
        """
        bars.extend([
            Bar(
                BarType.from_str("FESU_ESU=2019H.IB-1-MINUTE-MID-EXTERNAL"),
                open=Price.from_str("282.40"),
                high=Price.from_str("283.80"),
                low=Price.from_str("280.90"),
                close=Price.from_str("283.80"),
                volume=Quantity.from_str("13.0"),
                ts_init=dt_to_unix_nanos(pd.Timestamp("2018-12-21 17:53:00", tz="UTC")),
                ts_event=dt_to_unix_nanos(pd.Timestamp("2018-12-21 17:53:00", tz="UTC")),
            ),
            Bar(
                BarType.from_str("FESU_ESU=2019H.IB-1-MINUTE-MID-EXTERNAL"),
                open=Price.from_str("280.00"),
                high=Price.from_str("280.60"),
                low=Price.from_str("273.40"),
                close=Price.from_str("275.20"),
                volume=Quantity.from_str("10.0"),
                ts_init=dt_to_unix_nanos(pd.Timestamp("2018-12-27 17:53:00", tz="UTC")),
                ts_event=dt_to_unix_nanos(pd.Timestamp("2018-12-27 17:53:00", tz="UTC")),
            ),
        ])
    
    bars = list(sorted(bars, key= lambda x: x.ts_init))
    prices = wrangler.process_bars(bars)
    print(len(prices))
    writer = ContinuousPriceParquetWriter(path=str(file.path))
    
    print(f"Writing prices... {str(file.path)}")
    writer.write_objects(data=prices)
        
    # start month is start of data
    # start month for debugging
    # idx = paths.index([x for x in paths if "2021Z" in x.stem][0])
    # paths = paths[idx:]
    # start_month = ContractMonth(paths[0].stem.split("=")[1].split(".")[0])
    
    #########################

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

def find_problem_files():
    """
    find trading_classes where the files do have the hold cycle in every year
    """
    universe = IBTestProviderStubs.universe_dataframe()
    data_folder = Path("/Users/g1/Desktop/all UTC")
    for row in universe.itertuples():
        
        data_dir = (data_folder / row.data_symbol)
        paths = list(sorted(list(data_dir.glob("*.txt")) + list(data_dir.glob("*.b01"))))
        assert len(paths) > 0
        
        start_month = ContractMonth(row.data_start)
        end_month = ContractMonth(row.data_end)
        required_months = []
        
        cycle = RollCycle(row.hold_cycle)
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
    universe = IBTestProviderStubs.universe_dataframe()
    
    # for row in universe.itertuples():
    #     if row.trading_class == "EBM":
    #         process_row(
    #             str(row.trading_class),
    #             str(row.symbol),
    #             str(row.hold_cycle),
    #             str(row.priced_cycle),
    #             int(row.roll_offset),
    #             int(row.expiry_offset),
    #             int(row.carry_offset),
    #             str(row.start),
    #             row.missing_months.replace(" ", "").split(",") if type(row.missing_months) is not float else [],
    #             # list(map(lambda x: x.strip(), row.ignore_failed.split(","))),
    #         )
    # exit()
    
    
    func_gen = (
        joblib.delayed(process_row)(
            str(row.trading_class),
            str(row.symbol),
            str(row.hold_cycle),
            str(row.priced_cycle),
            int(row.roll_offset),
            int(row.expiry_offset),
            int(row.carry_offset),
            str(row.start),
            row.missing_months.replace(" ", "").split(",") if type(row.missing_months) is not float else [],
        )
        for row in universe.itertuples()
        # if row.trading_class != "EBM_Z"
        # if row.trading_class == "I"
    )
    results = joblib.Parallel(n_jobs=10, backend="loky")(func_gen)

    # # need carry price bars too
    # month = ContractMonth(path.stem.split("=")[1].split(".")[0])
    # if month in (wrangler.chain.hold_cycle) or (month in wrangler.chain.priced_cycle):
    
    # min_ = find_minimum_day_of_month_within_range(
    #     start=ContractMonth("1999H").timestamp_utc,
    #     end=ContractMonth("2025H").timestamp_utc,
    #     dayname="Thursday",
    #     dayofmonth=2,
    # )