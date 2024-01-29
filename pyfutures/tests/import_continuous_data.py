
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.continuous.wrangler import ContinuousPriceWrangler
from nautilus_trader.core.datetime import unix_nanos_to_dt
from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.config import ContractChainConfig
from pyfutures.continuous.cycle import RollCycle
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.data import ContinuousData
from pytower import PACKAGE_ROOT
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.core.datetime import dt_to_unix_nanos
import pandas as pd
from nautilus_trader.model.instruments.futures_contract import FuturesContract
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
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Quantity

CONTRACT_DATA_FOLDER = Path("/Users/g1/Desktop/output")
OUT_FOLDER = Path("/Users/g1/Desktop/multiple/data/genericdata_continuous_price")

def add_missing_daily_bars(trading_class: str, bars: list[Bar]) -> list[Bar]:

    # add extra bars
    if trading_class == "FESU":
        
        data = (
            ("2018Z", "2018-12-17", 290.6),
            ("2018Z", "2018-12-18", 286.4),
            ("2018Z", "2018-12-19", 289.7),
            ("2018Z", "2018-12-20", 287.3),
            ("2018Z", "2018-12-21", 284.7),
            
            ("2019H", "2018-12-14", 287.5),
            ("2019H", "2018-12-17", 287.6),
            ("2019H", "2018-12-18", 283.4),
            ("2019H", "2018-12-19", 286.7),
            ("2019H", "2018-12-20", 284.3),
            ("2019H", "2018-12-21", 283.8),
            ("2019H", "2018-12-27", 275.2),
        )
        for item in data:
            contract, timestamp, close = item
                
            timestamp_ns = dt_to_unix_nanos(pd.Timestamp(timestamp, tz="UTC"))
            bars.append(
                Bar(
                    BarType.from_str(f"FESU_ESU={contract}.IB-1-DAY-MID-EXTERNAL"),
                    open=Price.from_str(str(close)),
                    high=Price.from_str(str(close)),
                    low=Price.from_str(str(close)),
                    close=Price.from_str(str(close)),
                    volume=Quantity.from_str("20.0"),
                    ts_init=timestamp_ns,
                    ts_event=timestamp_ns,
                ),
            )
    elif trading_class == "NIFTY":
        data = """
        20071227,6120.0,6120.0,6120.0,6120.0,5000,100000
        20071228,6119.5,6119.5,6119.5,6119.5,5000,100000
        20071231,6155.0,6155.0,6155.0,6155.0,5000,100000
        20080101,6156.5,6156.5,6156.5,6156.5,5000,100000
        20080102,6223.0,6223.0,6223.0,6223.0,5000,100000
        20080103,6178.0,6178.0,6178.0,6178.0,5000,100000
        20080104,6145.0,6289.0,6145.0,6255.0,4596,106508
        20080107,6139.5,6288.0,6139.5,6288.0,4720,113831
        20080108,6279.0,6320.0,6195.0,6269.0,6614,120547
        20080109,6250.5,6318.0,6200.0,6260.0,2668,129891
        20080110,6265.0,6312.0,6112.5,6162.0,3052,131973
        20080111,6149.0,6235.0,6095.0,6222.0,5011,133297
        20080114,6200.0,6225.0,6160.0,6225.0,2059,139088
        20080115,6250.0,6250.0,6040.0,6058.0,4067,156727
        20080116,6000.0,6030.0,5800.0,5947.0,16582,187309
        20080117,5861.0,6035.0,5810.0,5922.0,5594,181865
        20080118,5860.0,5918.5,5680.0,5720.0,11137,189895
        20080121,5600.0,5615.0,4850.0,5198.0,19482,204858
        20080122,4900.5,5098.0,4419.5,4920.0,21660,196269
        20080123,5000.0,5340.0,4940.0,5164.0,27372,197447
        20080124,5041.0,5370.0,4951.0,5001.0,14343,188951
        20080125,5025.0,5404.0,4951.0,5404.0,9689,180177
        20080128,5240.0,5277.0,5040.0,5251.5,8483,170850
        20080129,5250.0,5360.0,5200.0,5277.0,6827,145856
        20080130,5280.0,5380.0,5100.0,5167.0,6940,92295
        20080131,5150.0,5351.0,5056.0,5138.0,0,0
        """
        
        for line in data.strip().splitlines():
            items = line.strip().split(",")
            timestamp_ns = dt_to_unix_nanos(
                pd.to_datetime(
                    items[0],
                    format="%Y%m%d",
                    utc=True,
                )
            )
            bars.append(
                Bar(
                    BarType.from_str("NIFTY_NIFTY50=2008F.IB-1-DAY-MID-EXTERNAL"),
                    open=Price.from_str(items[1]),
                    high=Price.from_str(items[2]),
                    low=Price.from_str(items[3]),
                    close=Price.from_str(items[4]),
                    volume=Quantity.from_str(items[6]),
                    ts_init=timestamp_ns,
                    ts_event=timestamp_ns,
                ),
            )
    return bars

def process_row(
    trading_class: str,
    symbol: str,
    hold_cycle: str,
    priced_cycle: str,
    roll_offset: str,
    approximate_expiry_offset: int,
    carry_offset: int,
    start: str,
    quote_currency: str,
    min_tick: float,
    price_magnifier: int,
    multiplier: float,
    missing_months: list[str] | None,
):
    # path = Path("/Users/g1/Desktop/DC_DA=2010Q.IB-1-DAY-MID-EXTERNAL-BAR-2010.parquet")
    
    # load all the data for the data symbol
    
    instrument_id = InstrumentId.from_str(f"{trading_class}_{symbol}.IB")
    daily_bar_type = BarType.from_str(f"{instrument_id}-1-DAY-MID-EXTERNAL")
    minute_bar_type = BarType.from_str(f"{instrument_id}-1-MINUTE-MID-EXTERNAL")
    
    daily_file = ParquetFile(
        parent=OUT_FOLDER,
        bar_type=daily_bar_type,
        cls=ContinuousPrice,
    )
    
    minute_file = ParquetFile(
        parent=OUT_FOLDER,
        bar_type=minute_bar_type,
        cls=ContinuousPrice,
    )
    
    # if daily_file.path.exists() and minute_file.path.exists():
    #     print(f"Skipping {trading_class}")
    #     return
    
    print(f"Processing {trading_class}")
    
    start_month = ContractMonth(start)
    
    missing_months = list(map(ContractMonth, missing_months))
    hold_cycle = RollCycle.from_str(hold_cycle, skip_months=missing_months)
    priced_cycle = RollCycle(priced_cycle)
    
    price_precision = IBTestProviderStubs.price_precision(
        min_tick=min_tick,
        price_magnifier=price_magnifier,
    )
    price_increment = IBTestProviderStubs.price_increment(
        min_tick=min_tick,
        price_magnifier=price_magnifier,
    )
    
    wrangler = ContinuousPriceWrangler(
        daily_bar_type=daily_bar_type,
        minute_bar_type=minute_bar_type,
        start_month=start_month,
        config=ContractChainConfig(
            instrument_id=instrument_id,
            hold_cycle=hold_cycle,
            priced_cycle=priced_cycle,
            roll_offset=roll_offset,
            approximate_expiry_offset=approximate_expiry_offset,
            carry_offset=carry_offset,
            skip_months=missing_months,
        ),
        base=FuturesContract(
            instrument_id=instrument_id,
            raw_symbol=instrument_id.symbol,
            asset_class=AssetClass.ENERGY,
            currency=Currency.from_str(quote_currency),
            price_precision=price_precision,
            price_increment=price_increment,
            multiplier=Quantity.from_str(str(multiplier)),
            lot_size=Quantity.from_int(1),
            underlying="",
            activation_ns=0,
            expiration_ns=0,
            ts_event=0,
            ts_init=0,
        )
    )
    
    keyword = f"{trading_class}_{symbol}*.IB*.parquet"
    
    paths = list(sorted(CONTRACT_DATA_FOLDER.glob(keyword)))
    assert len(paths) > 0
    
    print(f"{len(paths)} paths")
    
    bars = []
    for path in paths:
        bars_ = ParquetFile.from_path(path).read_objects(path)
        assert len(bars_) > 0
        bars.extend(bars_)
    
    print(f"{len(bars)} bars")
    
    bars = add_missing_daily_bars(trading_class, bars)
    bars = list(sorted(
                bars,
                key=lambda x: (x.ts_init, x.bar_type.instrument_id.symbol.value[-1],
            ))
    wrangler.process_bars(bars)
    
    daily_prices = wrangler.daily_prices
    minute_prices = wrangler.minute_prices
    
    # print(len(prices))
    writer = ContinuousPriceParquetWriter(path=str(daily_file.path))
    print(f"Writing daily prices... {len(daily_prices)} items {str(daily_file.path)}")
    writer.write_objects(data=daily_prices)
    
    writer = ContinuousPriceParquetWriter(path=str(minute_file.path))
    print(f"Writing minute prices... {len(minute_prices)} items {str(minute_file.path)}")
    writer.write_objects(data=minute_prices)
        
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

def test_find_problem_files():
    """
    find trading_classes where the files do have the hold cycle in every year
    """
    universe = IBTestProviderStubs.universe_dataframe()
    data_folder = Path("/Users/g1/Desktop/portara data george/DAY")
    for row in universe.itertuples():
        print(row.trading_class)
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
    universe = IBTestProviderStubs.universe_dataframe()
    # for row in universe.itertuples():
    #     if row.trading_class == "EBM":
    #         process_row(
    #             row.trading_class,
    #             row.symbol,
    #             row.hold_cycle,
    #             row.priced_cycle,
    #             row.roll_offset,
    #             row.expiry_offset,
    #             row.carry_offset,
    #             row.start,
    #             row.quote_currency.split("(")[1].split(")")[0],
    #             row.min_tick,
    #             row.price_magnifier,
    #             row.multiplier,
    #             row.missing_months.replace(" ", "").split(",") if type(row.missing_months) is not float else [],
    #         )
    #         exit()
    rows = universe.itertuples()
    # rows = [
    #     row for row in universe.itertuples()
    #     # if row.trading_class not in ("YIW", "EBM")
    # ]
    # for row in rows:
    #     print(row.trading_class)
    func_gen = (
        joblib.delayed(process_row)(
            row.trading_class,
            row.symbol,
            row.hold_cycle,
            row.priced_cycle,
            row.roll_offset,
            row.expiry_offset,
            row.carry_offset,
            row.start,
            row.quote_currency.split("(")[1].split(")")[0],
            row.min_tick,
            row.price_magnifier,
            row.multiplier,
            row.missing_months.replace(" ", "").split(",") if type(row.missing_months) is not float else [],
        )
        for row in rows
    )
    results = joblib.Parallel(n_jobs=20, backend="loky")(func_gen)

    # # need carry price bars too
    # month = ContractMonth(path.stem.split("=")[1].split(".")[0])
    # if month in (wrangler.chain.hold_cycle) or (month in wrangler.chain.priced_cycle):
    
    # min_ = find_minimum_day_of_month_within_range(
    #     start=ContractMonth("1999H").timestamp_utc,
    #     end=ContractMonth("2025H").timestamp_utc,
    #     dayname="Thursday",
    #     dayofmonth=2,
    # )
    
    
    # def add_minute_missing_bars(trading_class: str, bars: list[Bar]) -> list[Bar]:
    # # minute missing bars
    
    # # add extra bars
    # if trading_class == "FESB":
    #     "FESB 2007U - 20070615, 1621, 480.00, 482,00, 479.30, 482.00, 1, 20"
        
    #     bars.append(
    #         Bar(
    #             BarType.from_str("FESB_SX7E=2007U.IB-1-MINUTE-MID-EXTERNAL"),
    #             open=Price.from_str("480.00"),
    #             high=Price.from_str("482.00"),
    #             low=Price.from_str("479.30"),
    #             close=Price.from_str("482.00"),
    #             volume=Quantity.from_str("20.0"),
    #             ts_init=dt_to_unix_nanos(pd.Timestamp("2007-06-14 16:48:00", tz="UTC")),
    #             ts_event=dt_to_unix_nanos(pd.Timestamp("2007-06-14 16:48:00", tz="UTC")),
    #         )
    #     )
        
    # # data[ContractMonth("2007U")] = [bar] + data[ContractMonth("2007U")]
    # elif trading_class == "FESU":
    #     """
    #     20191221, time 282.40, 283.80, 280.90, 283.80, 1, 13
    #     20191227, time, 280.00, 280.60, 273.40, 275.20, 1, 10
    #     2018-12-21 17:53:00
    #     """
    #     bars.extend([
    #         Bar(
    #             BarType.from_str("FESU_ESU=2019H.IB-1-MINUTE-MID-EXTERNAL"),
    #             open=Price.from_str("282.40"),
    #             high=Price.from_str("283.80"),
    #             low=Price.from_str("280.90"),
    #             close=Price.from_str("283.80"),
    #             volume=Quantity.from_str("13.0"),
    #             ts_init=dt_to_unix_nanos(pd.Timestamp("2018-12-21 17:53:00", tz="UTC")),
    #             ts_event=dt_to_unix_nanos(pd.Timestamp("2018-12-21 17:53:00", tz="UTC")),
    #         ),
    #         Bar(
    #             BarType.from_str("FESU_ESU=2019H.IB-1-MINUTE-MID-EXTERNAL"),
    #             open=Price.from_str("280.00"),
    #             high=Price.from_str("280.60"),
    #             low=Price.from_str("273.40"),
    #             close=Price.from_str("275.20"),
    #             volume=Quantity.from_str("10.0"),
    #             ts_init=dt_to_unix_nanos(pd.Timestamp("2018-12-27 17:53:00", tz="UTC")),
    #             ts_event=dt_to_unix_nanos(pd.Timestamp("2018-12-27 17:53:00", tz="UTC")),
    #         ),
    #     ])
    # return bars