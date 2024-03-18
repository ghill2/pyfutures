from pathlib import Path

import joblib
import pandas as pd
from nautilus_trader.core.datetime import unix_nanos_to_dt

from nautilus_trader.continuous.chain import ContractChain
from pyfutures.continuous.chain2 import TestContractProvider
from nautilus_trader.continuous.contract_month import ContractMonth
from pyfutures.tests.test_kit import IBTestProviderStubs


OUT_FOLDER = Path("/Users/g1/Desktop/calendars")


def process_row(
    row: dict,
):
    instrument_provider = TestContractProvider(
        approximate_expiry_offset=row.config.approximate_expiry_offset,
        base=row.base,
    )

    chain = ContractChain(
        config=row.config,
        instrument_provider=instrument_provider,
    )

    df = pd.DataFrame(columns=["month", "approximate_expiry_date", "roll_date"])

    end_month = ContractMonth("2023F")
    contract = chain.current_contract(row.start)
    while contract.info["month"] <= end_month:
        df.loc[len(df)] = (
            contract.info["month"].value,
            unix_nanos_to_dt(contract.expiration_ns).strftime("%Y-%m-%d"),
            chain.roll_date_utc(contract).strftime("%Y-%m-%d"),
        )

        contract = chain.forward_contract(contract)

    path = OUT_FOLDER / f"{row.trading_class}_roll_calendar.csv"
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


if __name__ == "__main__":
    rows = IBTestProviderStubs.universe_rows(
        filter=["ECO"],
    )

    func_gen = (joblib.delayed(process_row)(row) for row in rows)
    results = joblib.Parallel(n_jobs=20, backend="loky")(func_gen)
