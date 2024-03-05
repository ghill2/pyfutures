# from pytower.data.format import Format
# from pytower.data.formats import _DUKA_BAR_FORMAT
# from pytower.data.formats import _DUKA_TICK_FORMAT

# from pytower.data.schema import Schema
# import pyarrow as pa
# import pytest

# from pytower.data.files import ParquetFile
# import pandas as pd

# from pytower import TEST_DATA_DIR

# import os


# class TestFormat:
#     def test_schema_process(self):
#         df = pd.DataFrame(
#             {
#                 # the actual columns that exist within a RAW DUKA FX PARQUET file.
#                 # in the wrong order, process() should correct them
#                 "AskVolume": 0.5,
#                 "Bid": 7.43481,
#                 "Ask": 7.43485,
#                 "BidVolume": 42.0,
#                 # Time (UTC) is of object type after read.
#                 "Time (UTC)": "00:33:07.007",
#                 #
#                 "Another-Column-That-Should-Be-Filtered": 0,
#             },
#             index=[0],
#         )

#         df = Schema(input_schema=input_schema, output_schema=output_schema).process(df)

#         # asserts names are equal to the schema
#         assert df["ask"]
#         assert df["bid"]
#         assert df["bid_size"]
#         assert df["ask_size"]

#     @pytest.mark.active
#     def test_schema_reading_file(self):
#         df = ParquetFile.from_path(
#             os.path.join(TEST_DATA_DIR, "RAW", "EURDKK-DUKA-FX-QUOTETICK-1-TICK-BID-2004.parquet")
#         )._read()

# TODOG: do we need these tests?
# def test_from_filename_return_expected_duka_tick(self):
#     format = Format.from_filename("EURUSD-DUKA-1-TICK-BID-EXTERNAL-2017")
#     assert format == _DUKA_TICK_FORMAT["data"]
#
# def test_from_filename_return_expected_duka_bar(self):
#     format = Format.from_filename("EURUSD-DUKA-1-HOUR-BID-EXTERNAL-2017")
#     assert format == _DUKA_BAR_FORMAT["data"]


# if __name__ == "__main__":
#     TestFormat().test_from_filename_return_expected_duka_tick()
#     TestFormat().test_from_filename_return_expected_duka_bar()
