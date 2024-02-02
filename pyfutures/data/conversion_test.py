# from pathlib import Path
# from tempfile import TemporaryDirectory

# from nautilus_trader.model.data import BarSpecification
# from nautilus_trader.model.identifiers import InstrumentId

# from pytower.config import SUPPORTED_SPECIFICATIONS
# from pytower.data.conversion import ConversionTask
# from pytower.data.files import CSVFile
# from pytower.tests.config import TEST_TEMP_DIR
# from pytower.tests.stubs import get_test_data_csv


# class TestConversionTask:
#     def setup(self):
#         pass

#     def test_data_exists_after_processing(self):
#         with TemporaryDirectory(dir=TEST_TEMP_DIR) as out_dir:
#             # Arrange
#             out_dir = Path(out_dir)
#             file = get_test_data_csv("GBP/USD.DUKA-1-TICK-BID-EXTERNAL", 2019)
#             expected_specs = SUPPORTED_SPECIFICATIONS
#             writer = ConversionTask(
#                 in_file=file, out_dir=out_dir, nrows=1, out_specs=expected_specs
#             )

#             # Act
#             writer.process()

#             # Assert
#             written_csvs = list(map(CSVFile, out_dir.iterdir()))
#             written_specs = [csv.bar_type.spec for csv in written_csvs]
#             assert sorted(written_specs) == sorted(expected_specs)


#     def test_data_exists_after_processing_same_spec_as_source_spec(self):
#         with TemporaryDirectory(dir=TEST_TEMP_DIR) as out_dir:
#             # Act
#             out_dir = Path(out_dir)
#             file = get_test_data_csv("GBP/USD.DUKA-1-TICK-BID-EXTERNAL", 2019)
#             out_spec = BarSpecification.from_str("1-TICK-BID")
#             writer = ConversionTask(in_file=file, out_dir=out_dir, nrows=1, out_specs=[out_spec])

#             # Act
#             writer.process()

#             # Assert
#             written_csvs = list(map(CSVFile, out_dir.iterdir()))
#             written_specs = [csv.bar_type.spec for csv in written_csvs]
#             assert out_spec in written_specs

#     def test_from_csvs_data_exists_after_processing(self):
#         with TemporaryDirectory(dir=TEST_TEMP_DIR) as out_dir:
#             # Arrange
#             out_dir = Path(out_dir)
#             expected_instrument_ids = [
#                 InstrumentId.from_str("GBP/USD.DUKA"),
#                 InstrumentId.from_str("EUR/USD.DUKA"),
#             ]
#             files = [
#                 get_test_data_csv(f"{expected_instrument_ids[0]}-1-TICK-BID-EXTERNAL", 2019),
#                 get_test_data_csv(f"{expected_instrument_ids[1]}-1-TICK-BID-EXTERNAL", 2019),
#             ]
#             tasks = ConversionTask.from_csvs(
#                 files, out_dir=out_dir, nrows=1, out_specs=SUPPORTED_SPECIFICATIONS
#             )

#             # Act
#             [task.write_objects() for task in tasks]

#             # Assert
#             written_csvs = list(map(CSVFile, out_dir.iterdir()))
#             written_specs = list({csv.bar_type.spec for csv in written_csvs})
#             written_instrument_ids = list({csv.bar_type.instrument_id for csv in written_csvs})

#             assert sorted(written_instrument_ids) == sorted(expected_instrument_ids)
#             assert sorted(written_specs) == sorted(list(SUPPORTED_SPECIFICATIONS))


# if __name__ == "__main__":
#     mod = TestConversionTask()
#     mod.setup()
#     mod.test_from_csvs_data_exists_after_processing()
