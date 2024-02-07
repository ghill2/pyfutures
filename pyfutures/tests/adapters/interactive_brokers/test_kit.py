import json
import pathlib
from datetime import time
# from pyfutures.adapters.interactive_brokers.client.objects import IBFuturesInstrument
# from pyfutures.adapters.interactive_brokers.client.objects import IBFuturesContract
from datetime import datetime
from collections import namedtuple
import pandas as pd
from pathlib import Path
from ibapi.contract import Contract as IBContract
from ibapi.contract import ContractDetails as IBContractDetails
from pytz import timezone
from pyfutures.adapters.interactive_brokers.parsing import create_contract

from nautilus_trader.model.identifiers import InstrumentId
from pyfutures import PACKAGE_ROOT
from pyfutures.adapters.interactive_brokers.parsing import dict_to_contract_details
from nautilus_trader.model.enums import BarAggregation
from pyfutures.data.files import ParquetFile
from pyfutures.continuous.chain import ContractChain
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from nautilus_trader.model.objects import Price
from pyfutures.continuous.contract_month import ContractMonth
from nautilus_trader.model.objects import Currency
from pyfutures.continuous.config import ContractChainConfig
from pyfutures.continuous.schedule import MarketSchedule
from nautilus_trader.model.enums import AssetClass
from pyfutures.continuous.cycle import RollCycle
from nautilus_trader.model.enums import InstrumentClass
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Quantity
from nautilus_trader.model.functions import bar_aggregation_to_str

TEST_PATH = pathlib.Path(PACKAGE_ROOT / "tests/adapters/interactive_brokers/")
RESPONSES_PATH = pathlib.Path(TEST_PATH / "responses")
STREAMING_PATH = pathlib.Path(TEST_PATH / "streaming")
PER_CONTRACT_FOLDER = Path("/Users/g1/Desktop/per_contract")
CONTRACT_PATH = pathlib.Path(RESPONSES_PATH / "contracts")
MULTIPLE_PRICES_FOLDER = Path("/Users/g1/Desktop/multiple/data/genericdata_continuous_price")
ADJUSTED_PRICES_FOLDER = Path("/Users/g1/Desktop/adjusted")
MERGED_FOLDER = Path("/Users/g1/Desktop/merged")
CONTRACT_DETAILS_PATH = RESPONSES_PATH / "import_contracts_details"
UNIVERSE_CSV_PATH = PACKAGE_ROOT / "universe.csv"
UNIVERSE_END = pd.Timestamp("2030-01-01", tz="UTC")

class Session:
    def __init__(
        self,
        name: int,
        chains: list[ContractChain],
        start_time: datetime.time,
        end_time: datetime.time,
    ):
        self.name = name
        self.chains = chains
        self.start_time = start_time
        self.end_time = end_time

    def contracts(self, timestamp: pd.Timestamp = None) -> list[IBContract]:
        contracts = []
        if timestamp is None:
            timestamp = pd.Timestamp.utcnow()
        for chain in self.chains:
            contracts.append(chain.current_contract(timestamp))
        return contracts

class IBTestProviderStubs:
    
    @staticmethod
    def universe_dataframe(filter: list | None = None, skip: list | None = None) -> pd.DataFrame:
        file = UNIVERSE_CSV_PATH
        assert file.exists()

        dtype = {
            "uname": str,
            "tradingClass": str,
            "symbol": str,
            "exchange": str,
            "ex_symbol": str,
            "data_symbol": str,
            "timezone": str,
            "price_precision": pd.Float64Dtype(),
            "data_start_minute": str,
            "data_start_day": str,
            "start": str,
            "skip_months": str,
            "data_completes": pd.BooleanDtype(),
            "price_magnifier":	pd.Int64Dtype(),
            "min_tick": pd.Float64Dtype(),
            "priced_cycle": str,
            "expiry_offset": pd.Int64Dtype(),
            "roll_offset": pd.Int64Dtype(),
            "carry_offset": pd.Int64Dtype(),
            "market_hours_local": str,
            "liquid_hours_local": str,
            "weekly_hours": str,
            "settlement_time": str,
            "hours_last_edited": str,
            "session": pd.Int64Dtype(),
            "description": str,
            "sector": str,
            "sub_sector": str,
            "region": str,
            "ib_url": str,
            "ex_url": str,
            "open": str,
            "close": str,
            "comments": str,
        }
        df = pd.read_csv(file, dtype=dtype)
        
        # temporary remove instruments that are failing to roll
        # df = df[(df.trading_class != "EBM") & (df.trading_class != "YIW")]
        ignored = [
            # "EBM",
            "YIW",
        ]
        df = df[~df.trading_class.isin(ignored)]
        
        if filter is not None:
            df = df[df.trading_class.isin(filter)]
            
        if skip is not None:
            df = df[~df.trading_class.isin(skip)]
        
        
        # skip rows with no data symbol
        df = df[df.data_symbol.notna()]
        
        # check for missing values
        assert df.exchange.notna().all()
        assert df.symbol.notna().all()
        assert df.trading_class.notna().all()
        assert df.min_tick.notna().all()
        assert df.price_magnifier.notna().all()
        assert df.quote_currency.notna().all()
        
        df["start"] = df.start.apply(ContractMonth)
        df["data_start_day"] = df.data_start_day.apply(ContractMonth)
        df["data_start_minute"] = df.data_start_minute.apply(ContractMonth)
        df["timezone"] = df.timezone.apply(timezone)
        
        df["settlement_time"] = df.settlement_time.apply(
                                    lambda x: pd.Timedelta(
                                        hours=int(x.split(":")[0]),
                                        minutes=int(x.split(":")[1]),
                                    )
                                )
                
        configs = []
        bases = []
        contracts = []
        instrument_ids = []
        liquid_schedules = []
        for row in df.itertuples():
            
            instrument_id = InstrumentId.from_str(f"{row.trading_class}_{row.symbol}.IB")
            instrument_ids.append(instrument_id)
            
            # parse config
            missing_months = row.missing_months.replace(" ", "").split(",") \
                                if not isinstance(row.missing_months, float) \
                                else []
            missing_months = list(map(ContractMonth, missing_months))
            
            configs.append(
                ContractChainConfig(
                    instrument_id=instrument_id,
                    hold_cycle=RollCycle.from_str(row.hold_cycle, skip_months=missing_months),
                    priced_cycle=RollCycle(row.priced_cycle),
                    roll_offset=row.roll_offset,
                    approximate_expiry_offset=row.expiry_offset,
                    carry_offset=row.carry_offset,
                    skip_months=missing_months,
                    start_month=row.start,
                )
            )
            
            # parse base contract
            price_precision = len(f"{(row.min_tick * row.price_magnifier):.8f}".rstrip("0").split(".")[1])
            
            bases.append(
                FuturesContract(
                    instrument_id=instrument_id,
                    raw_symbol=instrument_id.symbol,
                    asset_class=AssetClass.COMMODITY,
                    currency=Currency.from_str(row.quote_currency.split("(")[1].split(")")[0]),
                    price_precision=price_precision,
                    price_increment=Price(row.min_tick * row.price_magnifier, price_precision),
                    multiplier=Quantity.from_str(str(row.multiplier)),
                    lot_size=Quantity.from_int(1),
                    underlying="",
                    activation_ns=0,
                    expiration_ns=0,
                    ts_event=0,
                    ts_init=0,
                )
            )
            
            # parse contract
            contracts.append(
                create_contract(
                    trading_class=row.trading_class,
                    symbol=row.symbol,
                    venue=row.exchange,
                )
            )
            
            # parse liquid schedule
            # 08:30-13:20
            for value in row.liquid_hours_local.replace(" ", "").split(","):
                start, end = tuple(value.split("-"))
                start_hour, start_minutes = tuple(start.split(":"))
                end_hour, end_minutes = tuple(end.split(":"))
            data = pd.DataFrame(columns=["dayofweek", "open", "close"])
            for dayofweek in range(5):
                # create dataframe
                data.loc[len(data)] = {
                    "dayofweek": dayofweek,
                    "open": time(int(start_hour), int(start_minutes)),
                    "close": time(int(end_hour), int(end_minutes)),
                }
                    
            liquid_schedules.append(
                MarketSchedule(
                    name=str(instrument_id),
                    data=data,
                    timezone=row.timezone,
                ),
            )
        
        df["instrument_id"] = instrument_ids
        df["config"] = configs
        df["base"] = bases
        df["contract"] = contracts
        df["liquid_schedule"] = liquid_schedules
        
        # parse settlement time
        remove = [
            "comments", "open", "close", "ex_url", "ib_url", "ex_symbol",
            "description", "session", "hours_last_edited", "data_start", "data_end",
            "data_completes", "minute_transition", "price_magnifier", "min_tick", "multiplier",
            "missing_months", "hold_cycle", "priced_cycle", "roll_offset", "expiry_offset", "carry_offset",
        ]
        df = df[[x for x in df.columns if x not in remove]]
        
        return df
    
    @staticmethod
    def universe_rows(
        filter: list | None = None,
        skip: list | None = None,
    ) -> list[dict]:
        universe = IBTestProviderStubs.universe_dataframe(
            filter=filter,
            skip=skip,
        )
        
        rows = universe.to_dict(orient="records")
        assert len(rows) > 0
        
        Row = namedtuple("Row", list(rows[0].keys()))
        rows = [
            Row(**row) for row in rows
        ]
        return rows
    
    # def mes_instrument(self) -> FuturesContract:
        
        
    @staticmethod
    def bar_files(
        trading_class: str,
        symbol: str,
        aggregation: BarAggregation,
        month: str | None = None,
    ) -> list[ParquetFile]:
        aggregation = bar_aggregation_to_str(aggregation)
        month = month or "*"
        glob_str = f"{trading_class}_{symbol}={month}.IB-1-{aggregation}-MID*.parquet"
        print(glob_str)
        paths = list(PER_CONTRACT_FOLDER.glob(glob_str))
        paths = list(sorted(paths))
        files = list(map(ParquetFile.from_path, paths))
        assert len(files) > 0
        return files
    
    @staticmethod
    def adjusted_file(
        trading_class: str,
        symbol: str,
        aggregation: BarAggregation,
    ) -> ParquetFile:
        aggregation = bar_aggregation_to_str(aggregation)
        glob_str = f"{trading_class}_{symbol}.IB-1-{aggregation}-MID*.parquet"
        print(glob_str)
        paths = list(ADJUSTED_PRICES_FOLDER.glob(glob_str))
        paths = list(sorted(paths))
        files = list(map(ParquetFile.from_path, paths))
        assert len(files) == 1
        return files[0]
        
    @classmethod
    def universe_future_chains(cls) -> list[ContractChain]:
        chains = []
        universe = cls.universe_dataframe()
        
        # universe.open = universe.open.apply(lambda x: datetime.strptime(x, "%H:%M").time())
        # universe.close = universe.close.apply(lambda x: datetime.strptime(x, "%H:%M").time())
        
        for row in universe.itertuples():
            instrument_id = f"{row.tradingClass}-{row.symbol}.{row.exchange}"
            chains.append(
                FuturesChain(
                    config=FuturesChainConfig(
                        instrument_id=instrument_id,
                        hold_cycle=row.hold_cycle,
                        priced_cycle=row.priced_cycle,
                        roll_offset=row.roll_offset,
                        expiry_offset=row.expiry_offset,
                        carry_offset=row.carry_offset,
                    ),
                ),
            )
        return chains
    
    # @classmethod
    # def sessions(cls, names: int | None = None) -> list[Session]:
    #     universe = cls.universe_dataframe()

    #     sessions = []

    #     grouped = list(universe.groupby("session"))
    #     for session, df in grouped:
    #         chains = []
    #         for row in df.itertuples():
    #             instrument_id = f"{row.tradingClass}-{row.symbol}.{row.exchange}"
    #             chains.append(
    #                 FuturesChain(
    #                     config=FuturesChainConfig(
    #                         instrument_id=instrument_id,
    #                         hold_cycle=row.hold_cycle,
    #                         priced_cycle=row.priced_cycle,
    #                         roll_offset=row.roll_offset,
    #                         expiry_offset=row.expiry_offset,
    #                         carry_offset=row.carry_offset,
    #                     ),
    #                 ),
    #             )

    #         sessions.append(
    #             Session(
    #                 name=session,
    #                 chains=chains,
    #                 start_time=df.open.max(),
    #                 end_time=df.close.min(),
    #             ),
    #         )

    #     if names is not None:
    #         sessions = [x for x in sessions if x.name in names]
    #     return sessions

    # @classmethod
    # def universe_instrument_ids(cls) -> set[InstrumentId]:
    #     instrument_ids = []
    #     for chain in IBTestProviderStubs.universe_future_chains():
    #         for instrument_id in chain.instrument_ids(
    #             start=pd.Timestamp.utcnow(),
    #             end=UNIVERSE_END,
    #         ):
    #             instrument_ids.append(instrument_id)
    #     assert len(instrument_ids) == len(set(instrument_ids))
    #     return set(instrument_ids)

    # @staticmethod
    # def universe_contract_details() -> list[IBContractDetails]:
    #     """
    #     Return the unexpired contract details for all FutureChains in the universe of
    #     instruments.
    #     """
    #     folder = CONTRACT_DETAILS_PATH
    #     assert folder.exists()
    #     return [
    #         dict_to_contract_details(json.loads(path.read_text()))
    #         for path in sorted(folder.glob("*.json"))
    #     ]
        
    # @staticmethod
    # def price_precision(
    #     min_tick: float,
    #     price_magnifier: int,
    # ) -> int:
    #     min_tick = min_tick * price_magnifier
    #     price_precision = len(f"{min_tick:.8f}".rstrip("0").split(".")[1])
    #     return price_precision
    
    # @staticmethod
    # def price_increment(
    #     min_tick: float,
    #     price_magnifier: int,
    # ) -> Price:
    #     min_tick = min_tick * price_magnifier
    #     price_precision = len(f"{min_tick:.8f}".rstrip("0").split(".")[1])
    #     return Price(min_tick, price_precision)