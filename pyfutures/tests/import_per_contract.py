from pathlib import Path

import joblib
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol

from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.data.files import ParquetFile
from pyfutures.data.portara import PortaraData
from pyfutures.data.writer import BarParquetWriter
from pyfutures.data.writer import QuoteTickParquetWriter
from pyfutures.tests.adapters.interactive_brokers.test_kit import PER_CONTRACT_FOLDER
from pyfutures.tests.adapters.interactive_brokers.test_kit import CATALOG
from pyfutures.tests.adapters.interactive_brokers.test_kit import IBTestProviderStubs

BASENAME_TEMPLATE = lambda x: x.instrument_id.value + "-{i}"

def process_minute_and_day(path: Path, row: dict) -> None:
    
    contract_month = ContractMonth(path.stem[-5:])
    aggregation = path.parent.parent.stem

    bar_type = BarType.from_str(f"{instrument_id}-1-{aggregation}-MID-EXTERNAL")

    df = PortaraData.read_dataframe(path)
    
    wrangler = BarDataWrangler(
        instrument=row.base_instrument,
        
    )
    CATALOG.write_chunk(
        data=bars,
        data_cls=Bar,
        basename_template=file.instrument_id.value + "-{i}",
    )
    
    file = ParquetFile(
        parent=PER_CONTRACT_FOLDER,
        bar_type=bar_type,
        cls=Bar,
    )
    
    
    writer = BarParquetWriter(
        path=file.path,
        bar_type=bar_type,
        price_precision=row.price_precision,
        size_precision=1,
    )

    file.path.parent.mkdir(exist_ok=True, parents=True)
    print(f"Writing {bar_type} {file}...")

    writer.write_dataframe(df)


def process_hour(file: ParquetFile, row: tuple) -> None:
    assert file.path.exists()

    # MINUTE -> HOUR
    df = file.read(
        to_aggregation=(1, BarAggregation.HOUR),
    )

    bar_type = BarType.from_str(str(file.bar_type).replace("MINUTE", "HOUR"))

    file = ParquetFile(
        parent=PER_CONTRACT_FOLDER,
        bar_type=bar_type,
        cls=Bar,
    )
    file.path.parent.mkdir(exist_ok=True, parents=True)
    writer = BarParquetWriter(
        path=file.path,
        bar_type=bar_type,
        price_precision=row.price_precision,
        size_precision=1,
    )

    print(f"Writing {bar_type} {file}...")
    writer.write_dataframe(df)


def process_as_ticks(file: ParquetFile, row: tuple) -> None:
    """
    Export the bar parquet files as QuoteTick objects
    """
    df = file.read(
        bar_to_quote=True,
    )

    file = ParquetFile(
        parent=PER_CONTRACT_FOLDER,
        bar_type=file.bar_type,
        cls=QuoteTick,
    )

    file.path.parent.mkdir(exist_ok=True, parents=True)

    writer = QuoteTickParquetWriter(
        path=file.path,
        instrument_id=row.instrument_id,
        price_precision=row.price_precision,
        size_precision=1,
    )

    writer.write_dataframe(df)


rows = IBTestProviderStubs.universe_rows(
    filter=["ECO"],
)


def import_minute_and_hour():
    # import MINUTE and DAY
    for row in rows:
        files_d1 = PortaraData.get_paths(row.data_symbol, BarAggregation.DAY)
        files_m1 = PortaraData.get_paths(row.data_symbol, BarAggregation.MINUTE)
        paths = sorted(set(files_d1 + files_m1))
        for path in paths:
            yield joblib.delayed(process_minute_and_day)(path, row)


def minute_to_hour():
    # convert MINUTE > HOUR
    for row in rows:
        files = IBTestProviderStubs.bar_files(
            trading_class=row.trading_class,
            symbol=row.symbol,
            aggregation=BarAggregation.MINUTE,
        )
        for file in files:
            yield joblib.delayed(process_hour)(file, row)


def bars_to_quote_tick():
    # convert all to QuoteTick
    for row in rows:
        files = IBTestProviderStubs.bar_files(
            trading_class=row.trading_class,
            symbol=row.symbol,
        )
        for file in files:
            yield joblib.delayed(process_as_ticks)(file, row)


if __name__ == "__main__":
    joblib.Parallel(n_jobs=-1, backend="loky")(import_minute_and_hour())
    # joblib.Parallel(n_jobs=-1, backend="loky")(minute_to_hour())
    # joblib.Parallel(n_jobs=-1, backend="loky")(func_gen_minute_to_quote_tick())


# @pytest.mark.asyncio()
# async def test_load_precisions(client):

#     await client.connect()


# make sure each file has a related data symbol marked in the universe csv

# missing = []
# for data_symbol in universe.data_symbol.dropna():
#     print(DATA_FOLDER / data_symbol)
#     if not (DATA_FOLDER / data_symbol).exists():
#         missing.append(data_symbol)
# if len(missing) > 0:
#     for data_symbol in missing:
#         print(f"Missing data for {data_symbol}")
#     exit()

# # check files
# paths = list(DATA_FOLDER.rglob("*.txt")) + list(DATA_FOLDER.rglob("*.bd"))
# no_month = []
# for path in paths:

#     # check file has a letter month check
#     letter_month = path.stem[-1]
#     if letter_month not in MONTH_LIST:
#         no_month.append(path)
# if len(no_month) > 0:
#     for stem in no_month:
#         print(f"{stem}")
#     exit()

# # check file has data symbol
# rows = universe[universe['data_symbol'] == file.parent.stem]
# if rows.empty:
#     raise ValueError(f"{file}")
