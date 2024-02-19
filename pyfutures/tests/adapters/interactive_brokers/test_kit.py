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
import pytz
from pyfutures.adapters.interactive_brokers.parsing import create_contract

from nautilus_trader.model.identifiers import InstrumentId
from pyfutures import PACKAGE_ROOT
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
import re

from nautilus_trader.common.component import MessageBus
from nautilus_trader.common.component import LiveClock
from nautilus_trader.common.component import Logger
from nautilus_trader.cache.cache import Cache

TEST_PATH = pathlib.Path(PACKAGE_ROOT / "tests/adapters/interactive_brokers/")
RESPONSES_PATH = pathlib.Path(TEST_PATH / "responses")
STREAMING_PATH = pathlib.Path(TEST_PATH / "streaming")
PER_CONTRACT_FOLDER = Path("/Users/g1/Desktop/per_contract")
CONTRACT_PATH = pathlib.Path(RESPONSES_PATH / "contracts")
MULTIPLE_PRICES_FOLDER = Path("/Users/g1/Desktop/catalog/data/customdata_multiple_bar")
ADJUSTED_PRICES_FOLDER = Path("/Users/g1/Desktop/adjusted")
MERGED_FOLDER = Path("/Users/g1/Desktop/merged")
CONTRACT_DETAILS_PATH = RESPONSES_PATH / "import_contracts_details"
UNIVERSE_XLSX_PATH = PACKAGE_ROOT / "universe.xlsx"
FX_RATES_FOLDER = PACKAGE_ROOT / "fx_rates"
TRADERMADE_FOLDER = PACKAGE_ROOT / "tradermade"
SPREAD_FOLDER = Path("/Users/g1/Desktop/spread")
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
        file = UNIVERSE_XLSX_PATH
        assert file.exists()

        dtype = {
            "uname": str,
            "trading_class": str,
            "symbol": str,
            "exchange": str,
            "ex_symbol": str,
            "data_symbol": str,
            "quote_currency": str,
            "fee_fixed": str,
            "fee_fixed_currency": str,
            "fee_fixed_percent": str, # pd.BooleanDtype(),
            "fee_exchange": str,
            "fee_exchange_currency": str,
            "fee_regulatory": str,
            "fee_regulatory_currency": str,
            "fee_clearing": str,
            "fee_clearing_currency": str,
            "fee_clearing_percent": str, # pd.BooleanDtype()
            "timezone": str,
            "price_precision": pd.Float64Dtype(),
            "data_start_minute": str,
            "data_start_day": str,
            "start": str,
            "skip_months": str,
            "data_completes": str, # pd.BooleanDtype()
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
        # converters override dtypes
        df = pd.read_excel(file, dtype=dtype, engine="openpyxl")
        def parse_bool(s):
            if s in ["TRUE", "True"]:
                return True
            elif s == ["FALSE", "False"]:
                return False
            elif s == "nan":
                return None
        # openpyxl casting is broken for booleans
        # https://github.com/pandas-dev/pandas/issues/45903
        df["fee_fixed_percent"] = df["fee_fixed_percent"].apply(parse_bool)
        df["fee_clearing_percent"] = df["fee_clearing_percent"].apply(parse_bool)
        df["data_completes"] = df["data_completes"].apply(parse_bool)

        # df["trading_class"] = df["trading_class"].apply(str)
        
        # print(df.dtypes)
        # print(df.loc[28, "tradingClass"])
        # exit()
        
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
        df["timezone"] = df.timezone.apply(pytz.timezone)
        
        
        df["settlement_time"] = df.settlement_time.apply(
                                    lambda x: pd.Timedelta(
                                        hours=int(x.split(":")[0]),
                                        minutes=int(x.split(":")[1]),
                                    )
                                )
        
        
        df["quote_currency"] = df.quote_currency.apply(
            lambda x: Currency.from_str(re.search(r"\((.*?)\)", x).group(1)),
        )
        
        df["contract"] = df.apply(
            lambda row: create_contract(
                trading_class=row.trading_class,
                symbol=row.symbol,
                venue=row.exchange,
                sec_type="FUT",
                currency=row.quote_currency,
            ),
            axis=1
        )
        
        df["contract_cont"] = df.apply(
            lambda row: create_contract(
                trading_class=row.trading_class,
                symbol=row.symbol,
                venue=row.exchange,
                sec_type="CONTFUT",
                currency=row.quote_currency,
            ),
            axis=1,
        )
        
        df["instrument_id"] = df.apply(
            lambda row: InstrumentId.from_str(f"{row.trading_class}_{row.symbol}.IB"),
            axis=1,
        )
        df["quote_home_instrument_id"] = df.quote_currency.apply(
            lambda x: InstrumentId.from_str(f"{x}GBP.SIM"),
        )
        
        df["missing_months"] = df.missing_months.apply(
            lambda x: list(map(ContractMonth, x.replace(" ", "").split(","))) \
            if not isinstance(x, float) \
            else []
        )
        
        df["price_precision"] = df.apply(
            lambda row: len(f"{(row.min_tick * row.price_magnifier):.8f}".rstrip("0").split(".")[1]),
            axis=1,
        )
        
        df["config"] = df.apply(
            lambda row: ContractChainConfig(
                instrument_id=row.instrument_id,
                hold_cycle=RollCycle.from_str(row.hold_cycle, skip_months=row.missing_months),
                priced_cycle=RollCycle(row.priced_cycle),
                roll_offset=row.roll_offset,
                approximate_expiry_offset=row.expiry_offset,
                carry_offset=row.carry_offset,
                skip_months=row.missing_months,
                start_month=row.start,
            ),
            axis=1,
        )
        
        df["base_instrument"] = df.apply(
            lambda row: FuturesContract(
                instrument_id=row.instrument_id,
                raw_symbol=row.instrument_id.symbol,
                asset_class=AssetClass.COMMODITY,
                currency=row.quote_currency,
                price_precision=row.price_precision,
                price_increment=Price(row.min_tick * row.price_magnifier, row.price_precision),
                multiplier=Quantity.from_str(str(row.multiplier)),
                lot_size=Quantity.from_int(1),
                underlying="",
                activation_ns=0,
                expiration_ns=0,
                ts_event=0,
                ts_init=0,
            ),
            axis=1,
        )
        
        df["liquid_schedule"] = df.apply(
            lambda row: MarketSchedule.from_daily_str(
                name=f"{row.instrument_id}_liquid",
                timezone=row.timezone,
                value=row.liquid_hours_local,
            ),
            axis=1,
        )
        df["market_schedule"] = df.apply(
            lambda row: MarketSchedule.from_daily_str(
                name=f"{row.instrument_id}_market",
                timezone=row.timezone,
                value=row.market_hours_local,
            ),
            axis=1,
        )
            
        fees = []
        for row in df.itertuples():

            parsed_fees = []
            for fee_type, fee_value, fee_currency, is_percent in [
                ("fixed", row.fee_fixed, row.fee_fixed_currency, row.fee_fixed_percent),
                ("exchange", row.fee_exchange, row.fee_exchange_currency, False),
                ("regulatory", row.fee_regulatory, row.fee_regulatory_currency, False),
                ("clearing", row.fee_clearing, row.fee_clearing_currency, row.fee_clearing_percent)]:
                fee_value = float(fee_value)
                if is_percent:
                    assert fee_currency == row.contract.currency, f"{row.contract.exchange}-{row.contract.symbol}: if fees are as a percentage, the fee currency should equal the quote currency"
                    assert fee_value != 0, f"{row.contract.exchange}-{row.contract.symbol}: percent fee columns are 0, there is an error in the data"

                parsed_fees.append(
                    (fee_type, fee_value, fee_currency, is_percent)
                )
            fees.append(parsed_fees)
        
        df["fees"] = fees

        # parse settlement time
        remove = [
            "comments", "open", "close", "ex_url", "ex_symbol",
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
        
        UniverseRow = namedtuple("Row", list(rows[0].keys()))
        rows = [
            UniverseRow(**row) for row in rows
        ]
        return rows
    
    # def mes_instrument(self) -> FuturesContract:
        
        
    @classmethod
    def bar_files(
        cls,
        trading_class: str,
        symbol: str,
        aggregation: BarAggregation | None = None,
        month: str | None = None,
    ) -> list[ParquetFile]:
        
        aggregation = bar_aggregation_to_str(aggregation) if aggregation is not None else "*"
        month = month or "*"
        glob_str=f"{trading_class}_{symbol}={month}.IB-1-{aggregation}-MID*.parquet"
        return cls._get_files(parent=PER_CONTRACT_FOLDER, glob=glob_str)
    
    @classmethod
    def multiple_files(
        cls,
        trading_class: str,
        symbol: str,
        aggregation: BarAggregation,
    ) -> ParquetFile:
        aggregation = bar_aggregation_to_str(aggregation)
        glob_str = f"{trading_class}_{symbol}.IB-1-{aggregation}-MID*.parquet"
        files = cls._get_files(parent=MULTIPLE_PRICES_FOLDER, glob=glob_str)
        return files
    
    @classmethod
    def adjusted_files(
        cls,
        trading_class: str,
        symbol: str,
        aggregation: BarAggregation,
    ) -> ParquetFile:
        
        aggregation = bar_aggregation_to_str(aggregation)
        glob_str = f"{trading_class}_{symbol}.IB-1-{aggregation}-MID*.parquet"
        files = cls._get_files(parent=ADJUSTED_PRICES_FOLDER, glob=glob_str)
        return files
    
    @staticmethod
    def _get_files(
        parent: Path,
        glob: str,
    ) -> ParquetFile:
        paths = list(parent.glob(glob))
        paths = list(sorted(paths))
        files = list(map(ParquetFile.from_path, paths))
        if len(files) == 0:
            raise RuntimeError(f"Missing files for {glob}")
        
        return files
        
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
