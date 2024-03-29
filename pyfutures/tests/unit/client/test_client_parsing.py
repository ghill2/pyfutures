import pandas as pd
import pytest

from pyfutures.client.parsing import ClientParser


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        # request_bars -> BarSize._1_DAY formatDate=2
        ("20231219", pd.Timestamp("2024-04-11 00:00:00", tz="UTC")),
        ("1704897000", pd.Timestamp("2025-04-20 17:35:00", tz="UTC")),
20240327 10:20:00 US/Central
        # 1704992718
    ],
)
def test_parse_datetime(value, expected):
    """
    timestamp formats currently tested:
        CommissionReport.yieldRedemptionDate (YYYYMMDD format)
        request_bars() BarSize._1_MINUTE and lower, formatDate=2 (1704897000 format)
        request_bars() BarSize._1_DAY formatDate=2 (YYYYMMDD format)
        request_ticks() BarSize._1_DAY formatDate=2 (YYYYMMDD format)

    TODO: timestamps formats that need testing:
    request_trade_ticks() all bar sizes
    Execution.time (unknown format)
    Order.time (unknown format)
    """
    # note execution time
    # find all timestamp formatting in ib execution and ib order
    # test tick format for bar.date

    expected = ClientParser.parse_datetime(value)
    print(expected)
