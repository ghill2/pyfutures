import json
import pathlib

# from pyfutures.adapters.interactive_brokers.client.objects import IBFuturesInstrument
# from pyfutures.adapters.interactive_brokers.client.objects import IBFuturesContract
from datetime import datetime

import pandas as pd
from ibapi.contract import Contract as IBContract
from ibapi.contract import ContractDetails as IBContractDetails

from nautilus_trader.model.identifiers import InstrumentId
from pyfutures import PACKAGE_ROOT
from pyfutures.adapters.interactive_brokers.parsing import dict_to_contract_details
from pyfutures.continuous.chain import ContractChain
from nautilus_trader.model.instruments.futures_contract import FuturesContract
from nautilus_trader.model.objects import Price
from pyfutures.continuous.contract_month import ContractMonth
from nautilus_trader.model.objects import Currency
from pyfutures.continuous.config import ContractChainConfig
from pyfutures.continuous.cycle import RollCycle
from nautilus_trader.model.enums import AssetClass
from nautilus_trader.model.objects import Currency
from nautilus_trader.model.objects import Quantity

TEST_PATH = pathlib.Path(PACKAGE_ROOT / "tests/adapters/interactive_brokers/")
RESPONSES_PATH = pathlib.Path(TEST_PATH / "responses")
STREAMING_PATH = pathlib.Path(TEST_PATH / "streaming")
CONTRACT_PATH = pathlib.Path(RESPONSES_PATH / "contracts")

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
    def universe_dataframe() -> pd.DataFrame:
        file = UNIVERSE_CSV_PATH
        assert file.exists()

        dtype = {
            "tradingClass": str,
            "symbol": str,
            "exchange": str,
            "ex_symbol": str,
            "data_symbol": str,
            "timezone": str,
            "price_precision": pd.Float64Dtype(),
            "data_start": str,
            "data_end": str,
            "start": str,
            "end": str,
            "skip_months": str,
            "data_completes": pd.BooleanDtype(),
            "minute_transition": pd.BooleanDtype(),
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
            "comments": str,
            "open": str,
            "close": str,
        }
        df = pd.read_csv(file, dtype=dtype)
        
        # temporary remove instruments that are failing to roll
        df = df[(df.trading_class != "EBM") & (df.trading_class != "YIW")]
        
        # skip rows with no data symbol
        df = df[df.data_symbol.notna()]
        
        # check for missing values
        assert not df.exchange.isna().any()
        assert not df.symbol.isna().any()
        assert not df.trading_class.isna().any()
        assert not df.min_tick.isna().any()
        assert not df.price_magnifier.isna().any()
        assert not df.quote_currency.isna().any()

        
        
        # parse quote currency
        df.quote_currency = df.quote_currency.apply(
                                lambda x: Currency.from_str(x.split("(")[1].split(")")[0])
                            )
        
        # parse price precision
        df["price_precision"] = df.apply(
            lambda row: len(f"{(row.min_tick):.8f}".rstrip("0").split(".")[1]),
            axis=1,
        )
        
        # parse price increment
        df["price_increment"] = df.apply(
            lambda row: Price(row.min_tick, row.price_precision),
            axis=1,
        )
        
        # for row in df.itertuples():
            # if row.trading_class == "KE":
            # if row.price_increment <= 0.0 and row.price_magnifier == 100:
            #     print(row.trading_class, row.price_magnifier, row.price_precision, row.price_increment)
        # exit()
            
        # parse instrument_id
        df["instrument_id"] = df.apply(
            lambda row: InstrumentId.from_str(f"{row.trading_class}_{row.symbol}.IB"),
            axis=1,
        )
        
        # parse missing months
        data = []
        for missing_months in df.missing_months:
            missing_months = missing_months.replace(" ", "").split(",") if type(missing_months) is not float else []
            data.append(list(map(ContractMonth, missing_months)))
        df.missing_months = pd.Series(data)
        
        df["config"] = df.apply(
            lambda row: ContractChainConfig(
                instrument_id=row.instrument_id,
                hold_cycle=RollCycle.from_str(row.hold_cycle),
                priced_cycle=RollCycle(row.priced_cycle),
                roll_offset=row.roll_offset,
                approximate_expiry_offset=row.expiry_offset,
                carry_offset=row.carry_offset,
                skip_months=row.missing_months,
            ),
            axis=1,
        )
        
        for row in df.itertuples():
            FuturesContract(
                instrument_id=row.instrument_id,
                raw_symbol=row.instrument_id.symbol,
                asset_class=AssetClass.ENERGY,
                currency=row.quote_currency,
                price_precision=row.price_precision,
                price_increment=row.price_increment,
                multiplier=Quantity.from_str(str(row.multiplier)),
                lot_size=Quantity.from_int(1),
                underlying="",
                activation_ns=0,
                expiration_ns=0,
                ts_event=0,
                ts_init=0,
            )
        
        df["base"] = df.apply(
            lambda row: FuturesContract(
                instrument_id=row.instrument_id,
                raw_symbol=row.instrument_id.symbol,
                asset_class=AssetClass.ENERGY,
                currency=row.quote_currency,
                price_precision=row.price_precision,
                price_increment=row.price_increment,
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
        
        # parse settlement time
        remove = [
            "comments", "open", "close", "ex_url", "ib_url", "sector", "sub_sector", "ex_symbol",
            "description", "region", "session", "hours_last_edited", "data_start", "data_end",
            "data_completes", "minute_transition", "price_magnifier", "min_tick", "multiplier",
            "missing_months", "hold_cycle", "priced_cycle", "roll_offset", "expiry_offset", "carry_offset",
            "instrument_id", "price_increment", "price_precision", "quote_currency",
        ]
        df = df[[x for x in df.columns if x not in remove]]
        
        return df

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
    
    @staticmethod
    def price_precision(
        min_tick: float,
        price_magnifier: int,
    ) -> int:
        min_tick = min_tick * price_magnifier
        price_precision = len(f"{min_tick:.8f}".rstrip("0").split(".")[1])
        return price_precision
    
    @staticmethod
    def price_increment(
        min_tick: float,
        price_magnifier: int,
    ) -> Price:
        min_tick = min_tick * price_magnifier
        price_precision = len(f"{min_tick:.8f}".rstrip("0").split(".")[1])
        return Price(min_tick, price_precision)
    
    @classmethod
    def universe_continuous_data(cls) -> list:
        pass
        
    @classmethod
    def sessions(cls, names: int | None = None) -> list[Session]:
        universe = cls.universe_dataframe()

        sessions = []

        grouped = list(universe.groupby("session"))
        for session, df in grouped:
            chains = []
            for row in df.itertuples():
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

            sessions.append(
                Session(
                    name=session,
                    chains=chains,
                    start_time=df.open.max(),
                    end_time=df.close.min(),
                ),
            )

        if names is not None:
            sessions = [x for x in sessions if x.name in names]
        return sessions

    @classmethod
    def universe_instrument_ids(cls) -> set[InstrumentId]:
        instrument_ids = []
        for chain in IBTestProviderStubs.universe_future_chains():
            for instrument_id in chain.instrument_ids(
                start=pd.Timestamp.utcnow(),
                end=UNIVERSE_END,
            ):
                instrument_ids.append(instrument_id)
        assert len(instrument_ids) == len(set(instrument_ids))
        return set(instrument_ids)

    @staticmethod
    def universe_contract_details() -> list[IBContractDetails]:
        """
        Return the unexpired contract details for all FutureChains in the universe of
        instruments.
        """
        folder = CONTRACT_DETAILS_PATH
        assert folder.exists()
        return [
            dict_to_contract_details(json.loads(path.read_text()))
            for path in sorted(folder.glob("*.json"))
        ]