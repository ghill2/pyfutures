
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs
from pyfutures.continuous.wrangler import MultiplePriceWrangler
from nautilus_trader.core.datetime import unix_nanos_to_dt
from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.config import ContractChainConfig
from pytower.core.datetime import unix_nanos_to_dt_vectorized
from pyfutures.continuous.cycle import RollCycle
from pyfutures.continuous.contract_month import ContractMonth
from pytower.data.files import bars_from_rust
from pyfutures.continuous.data import ContinuousData
from pytower import PACKAGE_ROOT
from nautilus_trader.model.data import BarSpecification
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.enums import PriceType
from nautilus_trader.portfolio.portfolio import Portfolio
from nautilus_trader.core.datetime import dt_to_unix_nanos
import pandas as pd
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from pathlib import Path
from pyfutures.continuous.contract_month import ContractMonth
from nautilus_trader.cache.cache import Cache
from pyfutures.continuous.price import MultiplePrice
from nautilus_trader.common.clock import TestClock
from nautilus_trader.common.enums import LogLevel
from nautilus_trader.common.component import MessageBus
from nautilus_trader.test_kit.stubs.identifiers import TestIdStubs
from nautilus_trader.common.logging import Logger
from nautilus_trader.config import DataEngineConfig
from nautilus_trader.data.engine import DataEngine
from pytower.data.writer import MultiplePriceParquetWriter
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
from pytower.core.datetime import dt_to_unix_nanos_vectorized
from pytower.data.writer import BarParquetWriter
from nautilus_trader.persistence.wranglers import BarDataWrangler
        
CONTRACT_DATA_FOLDER = Path("/Users/g1/Desktop/per_contract")
OUT_FOLDER = Path("/Users/g1/Desktop/multiple/data/genericdata_continuous_price")
MONTH_LIST = ["F", "G", "H", "J", "K", "M", "N", "Q", "U", "V", "X", "Z"]

def import_missing_ebm_months():
    
    # load minute EBM bars from years 2013X, 2014F, 2014X, 2015F
    row = IBTestProviderStubs.universe_dataframe(filter=["EBM"]).to_dict(orient="records")[0]
    months = ("2013X", "2014F", "2014X", "2015F")
    bars_list = []
    for month in months:
        
        bar_type = BarType.from_str(f"EBM_EBM={month}.IB-1-MINUTE-MID-EXTERNAL")
        path = CONTRACT_DATA_FOLDER / f"{bar_type}-BAR-{month[:4]}.parquet"
        assert path.exists()
        
        df = ParquetFile.from_path(path).read()
        
        df.index = unix_nanos_to_dt_vectorized(df.ts_event)
        
        df.drop(["ts_event", "ts_init"], inplace=True, axis=1)
        freq = BarSpecification(1, BarAggregation.MINUTE, PriceType.MID).timedelta
        
        ohlc_dict = {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": "first",
        }
        
        df = df.resample(freq, closed="left", label="left").apply(ohlc_dict).dropna()
        df.index = df.index.floor("D")
        df.reset_index(inplace=True)
        
        df.ts_event = dt_to_unix_nanos_vectorized(df.ts_event)
        df.ts_init = df.ts_event.copy()
        df = df[
            ["open", "high", "low", "close", "volume", "ts_event", "ts_init"]
        ]
        
        df = bars_from_rust(df)
        
        bar_type = BarType.from_str(f"EBM_EBM={month}.IB-1-DAY-MID-EXTERNAL")
        wrangler = BarDataWrangler(
            bar_type=bar_type,
            instrument=row.base,
        )
        bars = wrangler.process(data=df)
        bars_list.extend(bars)
    return bars_list

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
            
    elif trading_class == "EBM":
        bars.extend(
            import_missing_ebm_months()
        )
        
    return bars

def process_row(row: dict) -> None:
    
    instrument_id = row.base.id
    daily_bar_type = BarType.from_str(f"{instrument_id}-1-DAY-MID-EXTERNAL")
    minute_bar_type = BarType.from_str(f"{instrument_id}-1-MINUTE-MID-EXTERNAL")
    
    daily_file = ParquetFile(parent=OUT_FOLDER, bar_type=daily_bar_type, cls=MultiplePrice)
    minute_file = ParquetFile(parent=OUT_FOLDER, bar_type=minute_bar_type, cls=MultiplePrice)
    
    # if daily_file.path.exists() and minute_file.path.exists():
    #     print(f"Skipping {trading_class}")
    #     return
    
    print(f"Processing {row.trading_class}")
    
    
    
    bars_list = []
    
    # read daily bars
    print("Reading daily bars...")
    keyword = f"{instrument_id.symbol}*-1-DAY-MID-EXTERNAL*.parquet"
    paths = list(sorted(CONTRACT_DATA_FOLDER.glob(keyword)))
    assert len(paths) > 0
    print(f"{len(paths)} paths")
    
    for path in paths:
        print(path)
        file = ParquetFile.from_path(path)
        df = file.read()
        assert len(df) > 0
        df = bars_from_rust(df)
        
        # add settlement time to daily bars
        df.index = (df.index.tz_localize(None) + row.settlement_time + pd.Timedelta(seconds=30))
        df.index = df.index.tz_localize(row.timezone)
        df.index = df.index.tz_convert("UTC")
        
        wrangler = BarDataWrangler(
            bar_type=file.bar_type,
            instrument=row.base,
        )
        bars_list.extend(
            wrangler.process(
                data=df,
            )
        )
    assert len(bars_list) > 0
    
    # read minute bars
    print("Reading minute bars...")
    keyword = f"{instrument_id.symbol}*-1-MINUTE-MID-EXTERNAL*.parquet"
    paths = list(sorted(CONTRACT_DATA_FOLDER.glob(keyword)))
    assert len(paths) > 0
    print(f"{len(paths)} paths")
    
    for path in paths:
        bars = ParquetFile.from_path(path).read_objects()
        if str(bars[0].bar_type) == "ECO_ECO.IB-1-DAY-MID-EXTERNAL":
            print(path)
            exit()
        print(len(bars))
        assert len(bars) > 0
        bars_list.extend(bars)
    
    print(f"{len(bars_list)} bars")
    
    bars_list = add_missing_daily_bars(row.trading_class, bars_list)
    bars_list = list(sorted(
                    bars_list,
                    key=lambda x: (
                        x.ts_init,
                        MONTH_LIST.index(x.bar_type.instrument_id.symbol.value[-1]) * -1,
                    )
            ))
    
    print(f"{len(bars_list)} bars")
    wrangler = MultiplePriceWrangler(
        daily_bar_type=daily_bar_type,
        minute_bar_type=minute_bar_type,
        start_month=row.start,
        config=row.config,
        base=row.base,
    )
    wrangler.process_bars(bars_list)
    
    daily_prices = wrangler.daily_prices
    minute_prices = wrangler.minute_prices
    
    # print(len(prices))
    writer = MultiplePriceParquetWriter(path=str(daily_file.path))
    print(f"Writing daily prices... {len(daily_prices)} items {str(daily_file.path)}")
    writer.write_objects(data=daily_prices)
    
    writer = MultiplePriceParquetWriter(path=str(minute_file.path))
    print(f"Writing minute prices... {len(minute_prices)} items {str(minute_file.path)}")
    writer.write_objects(data=minute_prices)
    
    # save csv too
    table = MultiplePriceParquetWriter.to_table(data=daily_prices)
    path = daily_file.path.with_suffix(".csv")
    df = table.to_pandas()
    df["timestamp"] = df.ts_event.apply(unix_nanos_to_dt)
    df.to_csv(path, index=False)
    
    table = MultiplePriceParquetWriter.to_table(data=minute_prices)
    path = minute_file.path.with_suffix(".csv")
    df = table.to_pandas()
    df["timestamp"] = df.ts_event.apply(unix_nanos_to_dt)
    df.to_csv(path, index=False)
        
if __name__ == "__main__":
    
    rows = IBTestProviderStubs.universe_rows(
        filter=["ECO"],
    )
    
    results = joblib.Parallel(n_jobs=20, backend="loky")(
        joblib.delayed(process_row)(row) for row in rows
    )

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