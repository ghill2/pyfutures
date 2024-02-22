from pyfutures.tests.import_per_contract_data import read_dataframe
from pyfutures import PACKAGE_ROOT


def test_read_dataframe():
    for filename in ("minute.txt", "daily.txt", "minute.b01", "daily.bd"):
        print(filename)
        path = PACKAGE_ROOT / "tests/data/test_data_import" / filename
        df = read_dataframe(path)
        assert list(df.columns) == ["open", "high", "low", "close", "volume", "timestamp"]
