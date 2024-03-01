from pathlib import Path

import joblib
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from nautilus_trader.model.data import QuoteTick
import pandas as pd
from nautilus_trader.model.enums import BarAggregation
from nautilus_trader.model.identifiers import InstrumentId
from nautilus_trader.model.identifiers import Symbol
from nautilus_trader.persistence.wranglers import BarDataWrangler
from nautilus_trader.persistence.wranglers import QuoteTickDataWrangler
from nautilus_trader.model.functions import bar_aggregation_from_str
from nautilus_trader.model.enums import PriceType

from pyfutures.continuous.contract_month import ContractMonth
from pyfutures.data.files import ParquetFile
from pyfutures.data.portara import PortaraData
from pyfutures.data.files import bars_to_dataframe
from pyfutures.data.files import bar_dataframe_to_quote_dataframe
from pyfutures.data.writer import BarParquetWriter
from pyfutures.data.writer import QuoteTickParquetWriter
from pyfutures.tests.test_kit import PER_CONTRACT_FOLDER
from pyfutures.tests.test_kit import CATALOG
from pyfutures.tests.test_kit import IBTestProviderStubs

def process_daily_bars(row: dict, path: Path) -> None:
    
    month = ContractMonth(path.stem[-5:])
    aggregation: BarAggregation = bar_aggregation_from_str(path.parent.parent.stem)

    df = PortaraData.read_dataframe(path)
    
    # apply settlement time
    df.index = df.index.tz_localize(None) + row.settlement_time + pd.Timedelta(seconds=30)
    df.index = df.index.tz_localize(row.timezone)
    df.index = df.index.tz_convert("UTC")
            
    instrument = row.instrument_for_month(month)
    
    bar_type_bid = row.bar_type_for_month(month, aggregation, PriceType.BID)
    bar_type_mid = row.bar_type_for_month(month, aggregation, PriceType.MID)
    bar_type_ask = row.bar_type_for_month(month, aggregation, PriceType.ASK)
    
    for bar_type in (bar_type_bid, bar_type_mid, bar_type_ask):
        wrangler = BarDataWrangler(
            instrument=instrument,
            bar_type=bar_type,
            
        )
        bars = wrangler.process(df)
        bars = list(sorted(bars, key=lambda x: x.ts_init))
        
        CATALOG.write_chunk(
            data=bars,
            data_cls=Bar,
            basename_template=str(bar_type) + "-{i}",
        )
        print(f"Written {bar_type!r}....")

def process_instruments(row: dict, month: ContractMonth) -> None:
    
    instrument = row.instrument_for_month(month)
    
    CATALOG.write_data(
        data=[instrument],
        basename_template=instrument.id.value + "-{i}",
    )
    print(f"Written {instrument.id!r}....")

def process_as_ticks(row: tuple, path: Path) -> None:
    """
    Export execution ticks
    """
    month = ContractMonth(path.stem[-5:])
    instrument = row.instrument_for_month(month)
    
    bars = CATALOG.query(
        data_cls=Bar,
        bar_types=[
            BarType.from_str(f"{instrument.id.value}")
        ],
        # instrument_ids=[
        #     instrument.id.value,
        # ]
    )
    
    df = bars_to_dataframe(bars)
    df = bar_dataframe_to_quote_dataframe(df)
    
    wrangler = QuoteTickDataWrangler(
        instrument=instrument,
    )
    quotes = wrangler.process(df)
    CATALOG.write_data(
        data=quotes,
        basename_template=instrument.id.value + "-{i}",
    )
        
rows = IBTestProviderStubs.universe_rows(
    filter=["ECO", "DC", "MES"],
)


def import_daily_bars():
    for row in rows:
        paths = PortaraData.get_paths(row.data_symbol, BarAggregation.DAY)
        # files_m1 = PortaraData.get_paths(row.data_symbol, BarAggregation.MINUTE)
        # paths = sorted(set(files_d1))
        for path in paths:
            yield joblib.delayed(process_daily_bars)(row, path)

def import_instruments():
    for row in rows:
        paths = PortaraData.get_paths(row.data_symbol, BarAggregation.DAY)
        # files_m1 = PortaraData.get_paths(row.data_symbol, BarAggregation.MINUTE)
        # paths = sorted(set(files_d1 + files_m1))
        
        months = {
            ContractMonth(path.stem[-5:])
            for path in paths
        }
        for month in months:
            yield joblib.delayed(process_instruments)(row, month)


            
if __name__ == "__main__":
    joblib.Parallel(n_jobs=-1, backend="loky")(import_instruments())
    joblib.Parallel(n_jobs=-1, backend="loky")(import_daily_bars())
    # joblib.Parallel(n_jobs=-1, backend="loky")(import_daily_quotes())


# def import_daily_quotes():
#     for row in rows:
#         paths = PortaraData.get_paths(row.data_symbol, BarAggregation.DAY)
#         # files_m1 = PortaraData.get_paths(row.data_symbol, BarAggregation.MINUTE)
#         # paths = sorted(set(files_d1))
#         for path in paths:
#             yield joblib.delayed(process_as_ticks)(row, path)

# def process_hour(row: tuple) -> None:
#     assert file.path.exists()
    
#     # load fx prices
#     CATALOG.query(
#         data_cls=QuoteTick,
#         instrument_ids=[row.instrument.id],
#         # filter_expr=Expression._field('bar_type') == str(bar_type),
#         # filter_expr=pyarrow._compute.Expression(f"field('bar_type') == '{bar_type}'"),
#     )
    
#     # MINUTE -> HOUR
#     df = file.read(
#         to_aggregation=(1, BarAggregation.HOUR),
#     )

#     bar_type = BarType.from_str(str(file.bar_type).replace("MINUTE", "HOUR"))

#     file = ParquetFile(
#         parent=PER_CONTRACT_FOLDER,
#         bar_type=bar_type,
#         cls=Bar,
#     )
    
#     file.path.parent.mkdir(exist_ok=True, parents=True)
#     writer = BarParquetWriter(
#         path=file.path,
#         bar_type=bar_type,
#         price_precision=row.price_precision,
#         size_precision=1,
#     )

#     print(f"Writing {bar_type} {file}...")
#     writer.write_dataframe(df)

# def minute_to_hour():
#     # convert MINUTE > HOUR
#     for row in rows:
#         files = IBTestProviderStubs.bar_files(
#             trading_class=row.trading_class,
#             symbol=row.symbol,
#             aggregation=BarAggregation.MINUTE,
#         )
#         for file in files:
#             yield joblib.delayed(process_hour)(file, row)


# def bars_to_quote_tick():
#     # convert all to QuoteTick
#     for row in rows:
#         files = IBTestProviderStubs.bar_files(
#             trading_class=row.trading_class,
#             symbol=row.symbol,
#         )
#         for file in files:
#             yield joblib.delayed(process_as_ticks)(file, row)

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
