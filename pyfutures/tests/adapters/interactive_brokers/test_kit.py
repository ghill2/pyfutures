import json
import pathlib

# from pyfutures.adapters.interactive_brokers.client.objects import IBFuturesInstrument
# from pyfutures.adapters.interactive_brokers.client.objects import IBFuturesContract
from datetime import datetime

import pandas as pd
from ibapi.contract import Contract as IBContract
from ibapi.contract import ContractDetails as IBContractDetails

from nautilus_trader.model.identifiers import InstrumentId
from pytower import PACKAGE_ROOT
from pyfutures.adapters.interactive_brokers.parsing import dict_to_contract_details
from nautilus_trader.model.continuous.chain import FuturesChain
from nautilus_trader.model.continuous.config import FuturesChainConfig


TEST_PATH = pathlib.Path(PACKAGE_ROOT / "tests/adapters/interactive_brokers/")
RESPONSES_PATH = pathlib.Path(TEST_PATH / "responses")
STREAMING_PATH = pathlib.Path(TEST_PATH / "streaming")
CONTRACT_PATH = pathlib.Path(RESPONSES_PATH / "contracts")

CONTRACT_DETAILS_PATH = RESPONSES_PATH / "import_contracts_details"
UNIVERSE_CSV_PATH = PACKAGE_ROOT / "instruments/universe.csv"
UNIVERSE_END = pd.Timestamp("2030-01-01", tz="UTC")


class Session:
    def __init__(
        self,
        name: int,
        chains: list[FuturesChain],
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
            "hold_cycle": str,
            "priced_cycle": str,
            "expiry_offset": pd.Int64Dtype(),
            "roll_offset": pd.Int64Dtype(),
            "carry_offset": pd.Int64Dtype(),
            "open": str,
            "close": str,
            "session": pd.Int64Dtype(),
            "description": str,
        }
        df = pd.read_csv(file, dtype=dtype)
        df.open = df.open.apply(lambda x: datetime.strptime(x, "%H:%M").time())
        df.close = df.close.apply(lambda x: datetime.strptime(x, "%H:%M").time())
        assert not df.exchange.isna().any()
        assert not df.symbol.isna().any()
        assert not df.tradingClass.isna().any()
        return df

    @classmethod
    def universe_future_chains(cls) -> list[FuturesChain]:
        chains = []
        universe = cls.universe_dataframe()
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