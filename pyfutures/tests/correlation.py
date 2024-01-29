import os
from pathlib import Path

import pandas as pd

from nautilus_trader.model.data.bar import Bar
from nautilus_trader.model.data.bar import BarSpecification
from nautilus_trader.model.data.bar import BarType
from nautilus_trader.model.data.bar_aggregation import BarAggregation
from nautilus_trader.model.enums_c import aggregation_source_from_str
from nautilus_trader.model.enums_c import price_type_from_str
from nautilus_trader.model.identifiers import InstrumentId
from pytower import PACKAGE_ROOT
from pytower.data.files import YearlyParquetFile
from pytower.data.files import bars_rust_to_normal
from pytower.data.ib.globals import IB_DATA_DIR
from pytower.instruments.pysys.provider import MissingPysysData
from pytower.instruments.pysys.provider import MissingPysysInstrument
from pytower.instruments.pysys.provider import PysysProvider


class Correlation:
    def __init__(self):

        repo_dir = Path(os.path.expanduser("~")) / "dev/app/trading/pysystemtrade"
        self.pysys_provider = PysysProvider(repo_dir=repo_dir)
    
    
    def universe(self):
        """
        Gets all.
        """
        path = PACKAGE_ROOT / "instruments/universe.csv"
        df_universe = pd.read_csv(path)
        all_ids = []
        all_sector = []
        all_region = []
        all_sub_sector = []
        all_series_prices = []
        all_series_returns = []
        all_m_series = []

        missing_pysys_tickers = []
        missing_ib_tickers = []
        # for row in UniverseProvider().instrument_ids():
        for row in df_universe.iterrows():
            exchange = row[1]["Exchange"]
            ex_symbol = row[1]["EX Symbol"]
            ib_symbol = row[1]["IB Symbol"]
            if exchange == "LMAX":
                continue
            instrument_id_str = f"{ex_symbol}.{exchange}"
            if self._should_load_pysys(exchange=exchange, ib_symbol=ib_symbol):
                try:
                    df = self.pysys_provider._adjusted_hourly_to_daily(
                        exchange=exchange,
                        ib_symbol=ib_symbol,
                    )
                except (MissingPysysInstrument, MissingPysysData) as e:
                    print(e)
                    missing_pysys_tickers.append((exchange, ex_symbol))
                    continue
                else:
                    price_col_name = "price"
            else:
                bar_spec = BarSpecification(
                    step=1,
                    aggregation=BarAggregation.DAY,
                    price_type=price_type_from_str("LAST"),
                )
                bar_type = BarType(
                    instrument_id=InstrumentId.from_str(instrument_id_str),
                    bar_spec=bar_spec,
                    aggregation_source=aggregation_source_from_str("EXTERNAL"),
                )
                file = YearlyParquetFile(
                    parent=Path(IB_DATA_DIR) / "CONTFUT",
                    bar_type=bar_type,
                    cls=Bar,
                    year=0,
                )
                if not file.path.exists():
                    missing_ib_tickers.append((exchange, ex_symbol))
                    continue
                df = bars_rust_to_normal(file.read())
                df.volume = (df.volume / 1e9).astype(float)
                df = df.set_index("timestamp")
                price_col_name = "close"
            
            series_prices = df[price_col_name]
            series_prices = series_prices.rename(instrument_id_str)
            all_series_prices.append(series_prices)

            series_returns = df[price_col_name].diff()
            series_returns = series_returns.rename(instrument_id_str)
            all_series_returns.append(series_returns)

            sector = row[1]["Sector"]
            region = row[1]["Region"]
            sub_sector = row[1]["Sub Sector"]
            all_sector.append(sector)
            all_region.append(region)
            all_sub_sector.append(sub_sector)
            all_ids.append(instrument_id_str)
            m_series = pd.Series([sector, region, sub_sector], name=instrument_id_str)
            all_m_series.append(m_series)

        df_prices = pd.concat(all_series_prices, axis=1)
        df_returns = pd.concat(all_series_returns, axis=1)
        df_corr = df_returns.corr()

        df_meta = pd.concat(all_m_series, axis=1)
        print(df_meta)
        df_corr_meta = pd.concat([df_meta, df_corr], ignore_index=True)
        df_corr_meta.insert(loc=0, column="Sub Sector", value=["", "", "", *all_sub_sector])
        df_corr_meta.insert(loc=0, column="Region", value=["", "", "", *all_region])
        df_corr_meta.insert(loc=0, column="Sector", value=["", "", "", *all_sector])
        df_corr_meta.insert(loc=0, column="InstrumentId", value=["", "", "", *all_ids])

        print(df_corr_meta)
        print(missing_pysys_tickers)
        print(missing_ib_tickers)

        outpath = "/Users/f1/tmp/universe_correlations.xlsx"
        df_prices.index = df_prices.index.astype(str)
        df_returns.index = df_returns.index.astype(str)
        with pd.ExcelWriter(
            outpath,
            engine="openpyxl",
            mode="w",
        ) as writer:
            df_prices.to_excel(writer, sheet_name="source_prices", index=True)
            df_returns.to_excel(writer, sheet_name="source_returns", index=True)
            df_corr_meta.to_excel(writer, sheet_name="correlation", index=False)

