import pathlib
import re
from collections import namedtuple
from datetime import datetime
from pathlib import Path

import pandas as pd
import pytz
from ibapi.contract import Contract as IBContract
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.enums import PriceType
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.functions import bar_aggregation_to_str
from nautilus_trader.model.functions import price_type_to_str
from nautilus_trader.core.nautilus_pyo3 import DataBackendSession
from nautilus_trader.model.data import capsule_to_list
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import Bar
from nautilus_trader.model.identifiers import Venue
from nautilus_trader.model.instruments.base import Instrument
from nautilus_trader.core.data import Data
from pytower.strategies.master import sort_key
from pyfutures.continuous.config import RollConfig
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Price
from nautilus_trader.model.objects import Quantity
from nautilus_trader.test_kit.providers import TestInstrumentProvider
from nautilus_trader.persistence.catalog.parquet import ParquetDataCatalog

from pyfutures import PACKAGE_ROOT
from pyfutures.adapters.interactive_brokers.parsing import create_contract
from pyfutures.continuous.chain import ContractChain
from pyfutures.continuous.config import ContractChainConfig
from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.continuous.cycle import RollCycle
from pyfutures.continuous.schedule import MarketSchedule
from pyfutures.data.files import ParquetFile
from dataclasses import dataclass
from dataclasses import fields

TEST_PATH = pathlib.Path(PACKAGE_ROOT / "tests/adapters/interactive_brokers/")
RESPONSES_PATH = pathlib.Path(TEST_PATH / "responses")
STREAMING_PATH = pathlib.Path(TEST_PATH / "streaming")
PER_CONTRACT_FOLDER = Path.home() / "Desktop" / "per_contract"
CONTRACT_PATH = pathlib.Path(RESPONSES_PATH / "contracts")
MULTIPLE_PRICES_FOLDER = (
    Path.home() / "Desktop" / "catalog" / "data" / "custom_multiple_bar"
)
CATALOG_FOLDER = Path.home() / "Desktop" / "catalog"
CATALOG = ParquetDataCatalog(path=CATALOG_FOLDER)  # , show_query_paths=True
ADJUSTED_PRICES_FOLDER = Path.home() / "Desktop" / "adjusted"
MERGED_FOLDER = Path.home() / "Desktop" / "merged"
CONTRACT_DETAILS_PATH = RESPONSES_PATH / "import_contracts_details"
UNIVERSE_XLSX_PATH = PACKAGE_ROOT / "universe.xlsx"
FX_RATES_FOLDER = PACKAGE_ROOT / "fx_rates"
TRADERMADE_FOLDER = PACKAGE_ROOT / "tradermade"
SPREAD_FOLDER = Path.home() / "Desktop" / "spread"
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


@dataclass
class UniverseRow:
    uname: str
    trading_class: str
    symbol: str
    exchange: str
    data_symbol: str
    sector: str
    sub_sector: str
    region: str
    quote_currency: Currency
    timezone: pytz.timezone
    settlement_time: pd.Timedelta
    market_schedule: MarketSchedule
    liquid_schedule: MarketSchedule
    start_month: ContractMonth
    chain_config: ContractChainConfig
    quote_home_instrument: Instrument
    instrument: FuturesContract
    contract: IBContract
    contract_cont: IBContract
    fee_fixed: float
    fee_fixed_currency: float
    fee_fixed_percent: float
    fee_exchange: float
    fee_exchange_currency: float
    fee_regulatory: float
    fee_regulatory_currency: float
    fee_clearing: float
    fee_clearing_currency: float
    fee_clearing_percent: float

    def instrument_for_month(self, month: ContractMonth) -> FuturesContract:
        instrument_id = self.instrument_id_for_month(
            month=month,
        )
        return FuturesContract(
            instrument_id=instrument_id,
            raw_symbol=self.instrument.id.symbol,
            asset_class=self.instrument.asset_class,
            currency=self.instrument.quote_currency,
            price_precision=self.instrument.price_precision,
            price_increment=self.instrument.price_increment,
            multiplier=self.instrument.multiplier,
            lot_size=self.instrument.lot_size,
            underlying=self.instrument.underlying,
            activation_ns=0,
            expiration_ns=0,
            ts_event=0,
            ts_init=0,
        )

    def instrument_id_for_month(self, month: ContractMonth) -> InstrumentId:
        return InstrumentId(
            symbol=Symbol(self.instrument.id.symbol.value + "=" + month.value),
            venue=self.instrument.id.venue,
        )

    def bar_type(self, aggregation: BarAggregation) -> BarType:
        aggregation = bar_aggregation_to_str(aggregation)
        return BarType.from_str(f"{self.instrument.id}-1-{aggregation}-MID-EXTERNAL")

    def bar_type_for_month(
        self,
        month: ContractMonth,
        aggregation: BarAggregation,
        price_type: PriceType,
    ) -> BarType:
        instrument_id = self.instrument_id_for_month(
            month=month,
        )
        aggregation = bar_aggregation_to_str(aggregation)
        price_type = price_type_to_str(price_type)
        return BarType.from_str(f"{instrument_id}-1-{aggregation}-{price_type}-EXTERNAL")

    def bar_files(
        self,
        aggregation: BarAggregation | None = None,
        month: str | None = None,
    ) -> list[ParquetFile]:
        aggregation = (
            bar_aggregation_to_str(aggregation) if aggregation is not None else "*"
        )
        month = month or "*"
        glob_str = f"{self.trading_class}={self.symbol}=FUT={month}-1-{aggregation}-MID*.parquet"
        return self._get_files(parent=PER_CONTRACT_FOLDER, glob=glob_str)
    
    @property
    def contract_bars(self) -> list[Bar]:
        """
        MID point bars only to process the chain
        """
        bars: list[Bar] = CATALOG.query(
            data_cls=Bar,
            instrument_ids=[self.instrument.id.symbol.value],
        )
        bars = [
            b for b in bars
            if b.bar_type.spec.price_type == PriceType.MID
        ]
        bars = sorted(bars, key=sort_key)
        assert len(bars) > 0
        return bars
    
    @staticmethod
    def _get_files(
        parent: Path,
        glob: str,
    ) -> ParquetFile:
        paths = list(parent.glob(glob))
        paths = sorted(paths)
        files = list(map(ParquetFile.from_path, paths))
        if len(files) == 0:
            raise RuntimeError(f"Missing files for {glob}")
        return files
    
    @property
    def backend_session(self) -> DataBackendSession:
        # create data
        session: DataBackendSession = CATALOG.backend_session(
            data_cls=QuoteTick,
            instrument_ids=[self.quote_home_instrument.id.value],  # fx rates
        )
        session: DataBackendSession = CATALOG.backend_session(
            data_cls=Bar,
            instrument_ids=[self.instrument.id.symbol.value],  # contract bars
            session=session,
        )
        return session
    
    @property
    def data(self) -> list[Data]:
        result = self.backend_session.to_query_result()
        data = []
        for chunk in result:
            data.extend(capsule_to_list(chunk))
            
        timestamps = {
            x.ts_init for x in data
        }
        for timestamp in timestamps:
            data.append(
                QuoteTick(
                    instrument_id=InstrumentId.from_str("EXECUTION.SIM"),
                    bid_price=Price.from_int(1),
                    ask_price=Price.from_int(1),
                    bid_size=Quantity.from_int(1),
                    ask_size=Quantity.from_int(1),
                    ts_init=timestamp,
                    ts_event=timestamp,
                )
            )
            
        data = sorted(data, key=sort_key)
        return data
    
    @property
    def instruments(self) -> list[Instrument]:
        instruments = CATALOG.instruments(
            instrument_type=FuturesContract,
            instrument_ids=[self.instrument.id.symbol.value],
            
        )
        instruments.append(self.quote_home_instrument)
        instruments.append(self.instrument)
        from decimal import Decimal
        from nautilus_trader.model.enums import AssetClass
        from nautilus_trader.model.enums import InstrumentClass
        instruments.append(
            Instrument(
                instrument_id=InstrumentId.from_str("EXECUTION.SIM"),
                raw_symbol=Symbol("EXECUTION"),
                asset_class=AssetClass.COMMODITY,
                instrument_class=InstrumentClass.SPOT,
                quote_currency=Currency.from_str("GBP"),
                is_inverse=False,
                price_precision=1,
                size_precision=0,
                size_increment=Quantity.from_int(1),
                multiplier=Quantity.from_int(1),
                margin_init=Decimal("1"),
                margin_maint=Decimal("1"),
                maker_fee=Decimal("1"),
                taker_fee=Decimal("1"),
                ts_event=0,
                ts_init=0,
            ),
        )
        return instruments
        
class IBTestProviderStubs:
    @staticmethod
    def universe_dataframe(
        filter: list | None = None, skip: list | None = None
    ) -> pd.DataFrame:
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
            "fee_fixed_percent": str,  # pd.BooleanDtype(),
            "fee_exchange": str,
            "fee_exchange_currency": str,
            "fee_regulatory": str,
            "fee_regulatory_currency": str,
            "fee_clearing": str,
            "fee_clearing_currency": str,
            "fee_clearing_percent": str,  # pd.BooleanDtype()
            "timezone": str,
            "price_precision": pd.Float64Dtype(),
            "data_start_minute": str,
            "data_start_day": str,
            "start_month": str,
            "skip_months": str,
            "data_completes": str,  # pd.BooleanDtype()
            "price_magnifier": pd.Int64Dtype(),
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

        df["start_month"] = df.start.apply(ContractMonth)
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
            lambda x: Currency.from_str(re.search(r"\((.*?)\)", x).group(1))
        )

        df["instrument_id"] = df.apply(
            lambda row: InstrumentId.from_str(
                f"{row.trading_class}={row.symbol}=FUT.SIM"
            ),
            axis=1,
        )
        df["instrument_id_live"] = df.apply(
            lambda row: InstrumentId.from_str(
                f"{row.trading_class}={row.symbol}=FUT.{row.exchange}"
            ),
            axis=1,
        )

        df["quote_home_instrument"] = df.quote_currency.apply(
            lambda x: TestInstrumentProvider.default_fx_ccy(
                symbol=f"{x}GBP", venue=Venue("SIM")
            )
        )

        df["contract"] = df.apply(
            lambda row: create_contract(
                trading_class=row.trading_class,
                symbol=row.symbol,
                venue=row.exchange,
                sec_type="FUT",
                currency=str(row.quote_currency),
            ),
            axis=1,
        )

        df["contract_cont"] = df.apply(
            lambda row: create_contract(
                trading_class=row.trading_class,
                symbol=row.symbol,
                venue=row.exchange,
                sec_type="CONTFUT",
                currency=str(row.quote_currency),
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

        df["missing_months"] = df.missing_months.apply(
            lambda x: list(
                map(
                    ContractMonth,
                    x.replace(" ", "").split(",") if not isinstance(x, float) else [],
                )
            )
        )
        
        df["roll_config"] = df.apply(
            lambda row: RollConfig(
                instrument_id=row.instrument_id,
                hold_cycle=RollCycle.from_str(row.hold_cycle, skip_months=row.missing_months),
                priced_cycle=RollCycle(row.priced_cycle),
                roll_offset=row.roll_offset,
                approximate_expiry_offset=row.expiry_offset,
                carry_offset=row.carry_offset,
                skip_months=row.missing_months,
            ),
            axis=1,
        )
        
        df["chain_config"] = df.apply(
            lambda row: ContractChainConfig(
                bar_type=BarType.from_str(f"{row.instrument_id}-1-DAY-MID-EXTERNAL"),
                roll_config=row.roll_config,
                start_month=row.start_month,
            ),
            axis=1,
        )

        df["price_precision"] = df.apply(
            lambda row: len(
                f"{(row.min_tick * row.price_magnifier):.8f}".rstrip("0").split(".")[1]
            ),
            axis=1,
        )

        df["instrument"] = df.apply(
            lambda row: FuturesContract(
                instrument_id=row.instrument_id,
                raw_symbol=row.instrument_id.symbol,
                asset_class=AssetClass.COMMODITY,
                currency=row.quote_currency,
                price_precision=row.price_precision,
                price_increment=Price(
                    row.min_tick * row.price_magnifier, row.price_precision
                ),
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

        keep = [f.name for f in fields(UniverseRow)]
        df = df[[x for x in df.columns if x in keep]]

        return df

    @staticmethod
    def universe_rows(
        filter: list | None = None,
        skip: list | None = None,
    ) -> list[dict]:
        df = IBTestProviderStubs.universe_dataframe(
            filter=filter,
            skip=skip,
        )
        rows = [UniverseRow(**d) for d in df.to_dict(orient="records")]
        assert len(rows) > 0

        return rows

    # @classmethod
    # def multiple_files(
    #     cls,
    #     trading_class: str,
    #     symbol: str,
    #     aggregation: BarAggregation,
    # ) -> ParquetFile:
    #     aggregation = bar_aggregation_to_str(aggregation)
    #     glob_str = f"{trading_class}={symbol}=FUT*{aggregation}-MID*.parquet"
    #     files = cls._get_files(parent=MULTIPLE_PRICES_FOLDER, glob=glob_str)
    #     return files

    # @classmethod
    # def adjusted_files(
    #     cls,
    #     trading_class: str,
    #     symbol: str,
    #     aggregation: BarAggregation,
    # ) -> ParquetFile:
    #     aggregation = bar_aggregation_to_str(aggregation)
    #     glob_str = f"{trading_class}={symbol}=FUT*{aggregation}-MID*.parquet"
    #     files = cls._get_files(parent=ADJUSTED_PRICES_FOLDER, glob=glob_str)
    #     return files