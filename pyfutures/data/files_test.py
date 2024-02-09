from pathlib import Path
from pyfutures import PACKAGE_ROOT
from nautilus_trader.model.data import Bar
from nautilus_trader.model.data import BarType
from pyfutures.data.files import ParquetFile


# from pytower.data.files import YearlyParquetFile


class TestParquetFile:

    def test_from_path(self):
        
        filename = "/parent/ES.CME-1-DAY-BID-EXTERNAL-BAR-2019.parquet"
        file = ParquetFile.from_path(filename)

        assert file.parent == Path("/parent")
        assert file.bar_type == BarType.from_str("ES.CME-1-DAY-BID-EXTERNAL")
        assert file.cls == Bar
        assert file.year == 2019

    def test_path(self):
        
        filename = "/parent/ES.CME-1-DAY-BID-EXTERNAL-BAR-2019.parquet"
        assert ParquetFile.from_path(filename).path == Path(filename)

        file = ParquetFile(
            parent="/parent",
            bar_type=BarType.from_str("ES.CME-1-DAY-BID-EXTERNAL"),
            cls=Bar,
            year=2019,
        )
        assert file.path == Path(filename)

    def test_read(self):
        
        path = Path(PACKAGE_ROOT / "tests/data/test_files/MES_MES=2021Z.IB-1-DAY-MID-EXTERNAL-BAR-0.parquet")
        file = ParquetFile.from_path(path)
        df = file.read(bar_to_quote=True)
    
# class TestContractParquetFile:
#     def setup(self):
#         self.filename = "/parent/CME-MES-2023Z.IB-1-DAY-BID-EXTERNAL-BAR.parquet"

#     def test_from_path(self):
#         file = ContractParquetFile.from_path(self.filename)

#         assert file.parent == Path("/parent")
#         assert file.bar_type == BarType.from_str("CME-MES-2023Z.IB-1-DAY-BID-EXTERNAL")
#         assert file.cls == Bar

#     def test_path(self):
#         assert ContractParquetFile.from_path(self.filename).path == Path(self.filename)

#         file = ContractParquetFile(
#                     parent="/parent",
#                     bar_type=BarType.from_str("CME-MES-2023Z.IB-1-DAY-BID-EXTERNAL"),
#                     cls=Bar,
#         )
#         assert file.path == Path(self.filename)

# class TestContractFile:
#     def setup(self):
#         self.filename = "/parent/ES-CME-CONTRACT-BAR-1-DAY-BID-20231215-495512552.parquet"

#     def test_from_path(self):
#         file = ContractFile.from_path(self.filename)

#         assert file.parent == Path("/parent")
#         assert file.bar_type == BarType.from_str("ES.CME-1-DAY-BID-EXTERNAL")
#         assert file.cls == Bar
#         assert file.expiry_date == pd.Timestamp("2023-12-15")
#         assert file.contract_id == 495512552

#     def test_path(self):
#         assert ContractFile.from_path(self.filename).path == Path(self.filename)


# class TestForexParquetFile:
#     def test_parquet_file_from_path(self):
#         file = ForexFile.from_path("EURUSD-DUKA-FX-QUOTETICK-1-TICK-ASK-2019.parquet")
#         assert file.bar_type == BarType.from_str("EUR/USD.DUKA-1-TICK-ASK-EXTERNAL")

#     def test_parquet_file_path(self):
#         filename = "EURUSD-DUKA-FX-QUOTETICK-1-TICK-ASK-2019.parquet"
#         assert ForexFile.from_path(filename).path == Path(filename)

# def test_write_quote_tick_objects(self, tmpdir):

# def test_on_read(self):
#     """tests if ParquetFile._process_before_write() correctly"""
#     """filters unecessary columns, converts to stored format and resets index"""

#     # Arrange
#     name_map = {
#         "bid": "Bid",
#         "ask": "Ask",
#         "bid_size": "BidVolume",
#         "ask_size": "AskVolume",
#         "timestamp": "Time (UTC)",
#     }
#     df = pd.DataFrame.from_dict(
#         {
#             "AskVolume": [0.5],
#             "Bid": [7.43481],
#             "Ask": [7.43485],
#             "BidVolume": [42.0],
#             "Time (UTC)": ["2004-10-25 00:33:07.007000+00:00"],
#             "Another-Column-That-Should-Be-Filtered": [0],
#         },
#     )

#     file = ParquetFile.from_path("EURDKK-DUKA-FX-QUOTETICK-1-TICK-BID-2004.parquet")

#     # Act
#     df = file._on_read(df, name_map=name_map, raise_error=True)

#     # Assert
#     expected = pd.DataFrame.from_dict(
#         {
#             "timestamp": pd.Timestamp("2004-10-25 00:33:07.007000+00:00", tz="UTC"),
#             "bid": [7.43481],
#             "ask": [7.43485],
#             "bid_size": [42.0],
#             "ask_size": [0.5],
#         }
#     ).set_index("timestamp")

#     pd.testing.assert_frame_equal(df, expected)

#     assert list(df.reset_index().columns)[0] == "timestamp"  # check timestamp is column 0

# def test_read_safe_cast(self):
#     """
#     TODO: test that the input dataframe gets cast to expected format safely
#     """

# def test_read(self, tmpdir):

#     # Arrange
#     filename = "EURDKK-DUKA-FX-QUOTETICK-1-TICK-BID-2004.parquet"
#     df = pd.DataFrame.from_dict(
#         {
#             "AskVolume": [0.5],
#             "Bid": [7.43481],
#             "Ask": [7.43485],
#             "BidVolume": [42.0],
#             "Time (UTC)": ["2004-10-25 00:33:07.007000+00:00"],
#         }
#     )
#     file = ParquetFile.from_path(Path(tmpdir) / filename)

#     df.to_parquet(Path(tmpdir) / filename)

#     # Act
#     df = file.read()

#     # Assert
#     expected = pd.DataFrame.from_dict(
#         {
#             "timestamp": [pd.Timestamp("2004-10-25 00:33:07.007000+00:00", tz="UTC")],
#             "bid": [7.43481],
#             "ask": [7.43485],
#             "bid_size": [42.0],
#             "ask_size": [0.5],
#         }
#     ).set_index("timestamp")

#     pd.testing.assert_frame_equal(df, expected)

# def test_read_objects(self, tmpdir):
#     # TODO
#     pass

# def test_write_bar_objects(self, tmpdir):
#     # Arrange
#     bars = [TestDataStubs.bar_5decimal()]

#     file = ParquetFile.from_path(Path(tmpdir) / "EURDKK-DUKA-FX-BAR-1-HOUR-BID-2004.parquet")

#     # Act
#     file.write_objects(bars)

#     # Assert
#     assert file.path.exists()

#     expected = pd.DataFrame().from_dict(
#         {
#             "timestamp": [0],
#             "open": [1.00002],
#             "high": [1.00004],
#             "low": [1.00001],
#             "close": [1.00003],
#             "volume": [1_000_000.0],
#         }
#     )
#     pd.testing.assert_frame_equal(pd.read_parquet(file.path), expected)


if __name__ == "__main__":
    TestFiles().test_parquet_file_path()

    # assert quotes == read_quotes

    # def test_roundtrip(self, tmpdir):
    #     df = ParquetFile.from_path(
    #         TEST_DATA_DIR / "RAW" / "EURDKK-DUKA-FX-QUOTETICK-1-TICK-BID-2004.parquet"
    #     ).read(raise_error=True)

    #     self.assert_normal_format(df)

    #     dest = ParquetFile.from_path(tmpdir / "EURDKK-DUKA-FX-QUOTETICK-1-TICK-BID-2004.parquet")
    #     dest.write_dataframe(df, path=dest.path)

    # assert content of QuoteTick objects afterwards

    # def test_process_on_write_quotes(self, tmpdir):
    #     """filters unecessary columns, converts to stored format and resets index"""

    #     df = pd.DataFrame.from_dict({
    #             "timestamp": [pd.Timestamp("2004-10-25 00:33:07.007000+00:00", tz="UTC")],
    #             "bid": [7.43481],
    #             "ask": [7.43485],
    #             "bid_size": [7.43485],
    #             "ask_size": [0.5],
    #         })

    #     file = ParquetFile.from_path("EURDKK-DUKA-FX-QUOTETICK-1-TICK-BID-2004.parquet")

    #     df = file._on_write(df)

    # def assert_rust_format(self, df: pd.DataFrame):
    #     # Expects a bid ask ask_size bid_size ts_event ts_init with RangedIndex
    #     """
    #     let bid_values = cols[0].as_any().downcast_ref::<Int64Array>().unwrap();
    #     let ask_values = cols[1].as_any().downcast_ref::<Int64Array>().unwrap();
    #     let ask_size_values = cols[2].as_any().downcast_ref::<UInt64Array>().unwrap();
    #     let bid_size_values = cols[3].as_any().downcast_ref::<UInt64Array>().unwrap();
    #     let ts_event_values = cols[4].as_any().downcast_ref::<UInt64Array>().unwrap();
    #     let ts_init_values = cols[5].as_any().downcast_ref::<UInt64Array>().unwrap();
    #     """
    #     assert df.bid.dtype == np.int64  # Int64Array
    #     assert df.ask.dtype == np.int64  # Int64Array
    #     assert df.ask_size.dtype == np.uint64  # UInt64Array
    #     assert df.bid_size.dtype == np.uint64  # UInt64Array
    #     assert df.ts_event.dtype == np.uint64  # UInt64Array
    #     assert df.ts_init.dtype == np.uint64  # UInt64Array

    # def assert_normal_format(self, df: pd.DataFrame):
    #     # Expects a bid ask Optional[bid_size] Optional[ask_size] with timestamp index
    #     assert is_datetime64tz_dtype(df.index.dtype)
    #     assert df.ask.dtype == float
    #     assert df.bid.dtype == float
    #     assert df.ask_size.dtype == float
    #     assert df.bid_size.dtype == float
    #     # assert the columns were renamed, filtered and are in the correct order
    #     assert list(df.columns) == ["bid", "ask", "bid_size", "ask_size"]

    #     return df

    # assert pdtype.is_datetime64_dtype(df.index)


#
#     def test_file_path_to_parts(self):
#         file_path = "/Volumes/CORSAIR/CSV/QuoteTick/AUDSGD-DUKA-1-TICK-BID-EXTERNAL-2017.csv"
#         assert path_to_parts(file_path) == (
#             Path("/Volumes/CORSAIR/CSV"),
#             QuoteTick,
#             BarType.from_str("AUD/SGD.DUKA-1-TICK-BID-EXTERNAL"),
#             2017,
#         )
#
#     def test_parts_to_file_path(self):
#         file_path = "/Volumes/CORSAIR/CSV/QuoteTick/AUDSGD-DUKA-1-TICK-BID-EXTERNAL-2017.csv"
#         assert (
#             file_path
#             == parts_to_path(
#                 root=Path("/Volumes/CORSAIR/CSV"),
#                 cls=QuoteTick,
#                 bar_type=BarType.from_str("AUD/SGD.DUKA-1-TICK-BID-EXTERNAL"),
#                 year=2017,
#             )
#             + ".csv"
#         )
#
#     def test_csv_pickle(self):
#         path = pickle.loads(
#             pickle.dumps(
#                 CSVFile("/Volumes/CORSAIR/CSV/AUDCAD-DUKA-1-TICK-BID-EXTERNAL-2010.csv"),
#             )
#         )
#         assert path.bar_type == BarType.from_str("AUD/CAD.DUKA-1-TICK-BID-EXTERNAL")
#         assert path.year == 2010
#         assert path.instrument_id == InstrumentId.from_str("AUD/CAD.DUKA")
#
#
# def test_to_persistence():
#     len_ = 100
#     start_nanos = dt_to_unix_nanos(pd.Timestamp("2020-01-01"))
#     end_nanos = dt_to_unix_nanos(pd.Timestamp("2021-01-01"))
#     data = dict(
#         timestamp=pd.Series(sorted(random.sample(range(start_nanos, end_nanos), len_))).map(
#             unix_nanos_to_dt
#         ),
#         ask=sorted(np.random.uniform(1.46264, 1.50264, len_)),
#         bid=sorted(np.random.uniform(1.46264, 1.50264, len_)),
#     )
#     df = pd.DataFrame.from_dict(data)
#     df.set_index("timestamp", inplace=True)
#     df = to_stored_format(df)
#
#
# def test_from_persistence():
#     len_ = 100
#     start_nanos = dt_to_unix_nanos(pd.Timestamp("2020-01-01"))
#     end_nanos = dt_to_unix_nanos(pd.Timestamp("2021-01-01"))
#     data = dict(
#         bid=(pd.Series(sorted(np.random.uniform(1.46264, 1.50264, len_))) * FIXED_SCALAR).astype(
#             np.int64
#         ),
#         ask=(pd.Series(sorted(np.random.uniform(1.46264, 1.50264, len_))) * FIXED_SCALAR).astype(
#             np.int64
#         ),
#         ask_size=pd.Series(random.sample(range(start_nanos, end_nanos), len_)).astype(np.uint64),
#         bid_size=pd.Series(random.sample(range(start_nanos, end_nanos), len_)).astype(np.uint64),
#         ts_event=pd.Series(sorted(random.sample(range(start_nanos, end_nanos), len_)))
#         .map(unix_nanos_to_dt)
#         .astype(np.uint64),
#         ts_init=pd.Series(sorted(random.sample(range(start_nanos, end_nanos), len_)))
#         .map(unix_nanos_to_dt)
#         .astype(np.uint64),
#     )
#     df = pd.DataFrame.from_dict(data)
#     df = to_unstored_format(df)
#     # print(df)
#
