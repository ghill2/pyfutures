
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs

from nautilus_trader.core.datetime import unix_nanos_to_dt
from pyfutures.continuous.chain import FuturesChain
from pyfutures.continuous.config import FuturesChainConfig
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.data import ContinuousData
from pytower import PACKAGE_ROOT
from nautilus_trader.portfolio.portfolio import Portfolio
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

attempting_list = [
    
]

success_list = [
    "DC",
    "EBM",
    "ECO",
    "EMA",
    "KE",
    "RS",
    "XK",
    "XW",
    "ZC",
    "ZL",
    "ZM",
    "ZO",
    "ZR",
    "ZS",
    "GF",
    "HE",
    "LE",
    "C",
    "W",
    "CC",
    "CT",
    "KC",
    "OJ",
    "SB",
    "JBL",
    "JBLM",
    "JB",
    "XT",
    "FGBX",
    "FBTP",
    "FGBL",
    "FOAT",
    "R",
    "FGBM",
    "FGBS",
    "ZB",
    "CGB",
    "TN",
    "UB",
    "ZN",
    "ZF",
    "ZT",
    "ATW",
    "ECF",
    "NGF",
    "TFM",
    "HH",
    "NG",
    "QG",
    "BZ",
    "COIL",
    "GOIL",
    "HO",
    "MCL",
    "QM",
    "RB",
    "06",
    "05",
    "MHI",
    "TPXM",
    "MCH",
    "MFS",
    "MME",
    "SGP",
    "225MC",
    "225M",
    "FTI",
    "MFA",
    "FEDV",
    "FXXP",
    "FSXE",
    "FESX",
    "Y2",
    "MICRO",
    "FTMIB",
    "MINI",
    "MIX",
    "FSMX",
    "FESB",
    "FSTA",
    "FSTS",
    "FSTI",
    "FSTY",
    "EMD",
    "MYM",
    "MNQ",
    "M2K",
    "MES",
    "SXF",
    "SXM",
    "RX",
    "XAP",
    "XAE",
    "XAF",
    "XAV",
    "XAI",
    "XAU",
    "RP",
    "RY",
    "6L",
    "6M",
    "6A",
    "6C",
    "6S",
    "DX",
    "E7",
    "6E",
    "6B",
    "6J",
    "M6A",
    "M6E",
    "6N",
    "FEF",
    "TF",
    "ALI",
    "HG",
    "HRC",
    "MHG",
    "PA",
    "GC",
    "MBT",
    "MGC",
    "PL",
    "SI",
    "SIL",
    "YIW",
    "FEU3",
    "I",
    "SO3",
    "SR3",
    "FVS",
    "VX",
    "VXM",
]

failed_list = [
    "ZW",  # 1987Z, contract expired
    "167", # 2017M, contract expired
    "RC",  # 2008X, contract expired
    "TWN",  # 2020N, contract expired
    "CN", # XINA50, contract expired
    "CL",  # 1987Z, contract expired
    "FESU",  # 2008H, contract expired, roll date is 2008-03-16 for 2008H but no data in next contract 2008M until 2008-03-20
    "FSTO",  # 2008H, contract expired
    "FSTH",  # 2008H, contract expired
    "FSTE",  # 2008H, contract expired
    "FSTU",  # 2008H, contract expired
    "FSTL",  # 2008U, contract expired
    "FSMS",  # 1994M, contract expired
    "FSMI",  # 1994M, contract expired
    "Z",  # 1988H, contract expired
    "FDXM",  # 1992Z, contract expired
    "FDXS",  # 1992Z, contract expired
    "FDAX",  # 1992Z, contract expired
    "FCE",  # 1990M, contract expired
    "MFC",  # 1990M, contract expired
    "6Z",  # 2008M, contract expired
]
def get_start_month_and_year():
    
    data_folder = Path("/Users/g1/Downloads/portara data/all UTC")
    
    universe = IBTestProviderStubs.universe_dataframe()
    
    for row in universe.itertuples():
        
        data_dir = (data_folder / row.data_symbol)
        
        assert data_dir.exists()
        
        files = list(sorted(list(data_dir.rglob("*.txt")) + list(data_dir.rglob("*.b01"))))
        start_month = files[0].stem[-1]
        start_year = files[0].stem[-5:-1]
        
        end_month = files[-1].stem[-1]
        end_year = files[-1].stem[-5:-1]
        print(end_year)

def process_row(
    trading_class: str,
    symbol: str,
    exchange: str,
    hold_cycle: str,
    priced_cycle: str,
    roll_offset: str,
    approximate_expiry_offset: int,
    carry_offset: int,
):
    
    
    # load all the data for the data symbol
    data_folder = Path("/Users/g1/Desktop/output")
    
    keyword = f"{trading_class}_{symbol}=*.IB*.parquet"
    paths = list(sorted(data_folder.glob(keyword)))
    assert len(paths) > 0
    
    # start = Path("/Users/g1/Desktop/output/HO-HO=2023V.NYMEX-1-MINUTE-MID-EXTERNAL-BAR-2023.parquet")
    # paths = paths[paths.index(start):]
    
    bars = []
    
    for path in paths:
        file = ParquetFile.from_path(path)
        bars.extend(file.read_objects())
    bars = list(sorted(bars, key=lambda x: x.ts_event))
    
    timestamps = [x.ts_event for x in bars]
    assert list(sorted(timestamps)) == timestamps
    
    instrument_id = InstrumentId.from_str(f"{trading_class}_{symbol}.IB")
    bar_type = BarType.from_str(f"{instrument_id}-1-MINUTE-MID-EXTERNAL")
    
    chain = FuturesChain(
        config=FuturesChainConfig(
            instrument_id=instrument_id,
            hold_cycle=hold_cycle,
            priced_cycle=priced_cycle,
            roll_offset=roll_offset,
            approximate_expiry_offset=approximate_expiry_offset,
            carry_offset=carry_offset,
        ),
    )
    
    # # user defined start month for debugging
    # start_year = int(start.stem.split("=")[1].split(".")[0][:4])
    # start_letter_month = start.stem.split("=")[1].split(".")[0][-1]
    # start_month = ContractMonth.from_year_letter_month(year=start_year, letter_month=start_letter_month)
    
    # start month is start of data
    start_year = int(paths[0].stem.split("=")[1].split(".")[0][:4])
    start_letter_month = paths[0].stem.split("=")[1].split(".")[0][-1]
    start_month = ContractMonth.from_year_letter_month(
                        year=start_year,
                        letter_month=start_letter_month,
                    )
    
    prices = []
    data = ContinuousData(
        bar_type=bar_type,
        chain=chain,
        start_month=start_month,
        # end_month=end_month,
        handler=prices.append,
    )
    
    #########################
    # component setup
    
    clock = TestClock()
    logger = Logger(
        clock=TestClock(),
        level_stdout=LogLevel.INFO,
        bypass=True,
    )

    msgbus = MessageBus(
        trader_id=TestIdStubs.trader_id(),
        clock=clock,
        logger=logger,
    )

    cache = Cache(
        logger=logger,
    )

    data_engine = DataEngine(
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        logger=logger,
        config=DataEngineConfig(debug=True),
    )

    portfolio = Portfolio(
        msgbus,
        cache,
        clock,
        logger,
    )
    
    data.register_base(
        portfolio=portfolio,
        msgbus=msgbus,
        cache=cache,
        clock=clock,
        logger=logger,
    )

    data.start()
    data_engine.start()
    
    #########################
    
    data.on_start()
    
    end_month = ContractMonth("2024F")
    for bar in bars:
        
        # stop when the data module rolls to year 2024
        if len(prices) > 0 and prices[-1].current_month >= end_month:
            prices.pop(-1)
            assert (len(prices)) > 0, f"prices length is 0 {trading_class}"
            assert prices[-1].current_month.year < 2024, f"prices[-1].current_month.year < 2024 {trading_class}"
            break  # done sampling
        
        cache.add_bar(bar)
        # try:
        data.on_bar(bar)
        # except ValueError as e:
        #     return repr(e)
    
    
    file = ParquetFile(
        parent=Path("/Users/g1/Desktop/continuous/data/genericdata_continuous_price"),
        bar_type=bar_type,
        cls=ContinuousPrice,
    )
    writer = ContinuousPriceParquetWriter(path=file.path)
    writer.write_objects(data=prices)
    
if __name__ == "__main__":
    
    
    
    universe = IBTestProviderStubs.universe_dataframe()
    
    func_gen = (
        joblib.delayed(process_row)(
            str(row.trading_class),
            str(row.symbol),
            str(row.exchange),
            str(row.hold_cycle),
            str(row.priced_cycle),
            int(row.roll_offset),
            int(row.expiry_offset),
            int(row.carry_offset),
        )
        for row in universe.itertuples()
        # if row.trading_class in failed_list
    )
        
    results = joblib.Parallel(n_jobs=-1, backend="loky")(func_gen)