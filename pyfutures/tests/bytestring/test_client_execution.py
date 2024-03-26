import pytest
from pytower.stats.margin_info import MarginInfo
from pyfutures.tests.test_kit import IBTestProviderStubs
import pandas as pd


# @pytest.mark.skip(reason="helper")
@pytest.mark.asyncio()
async def test_open_low_margin_instruments():
    rows = IBTestProviderStubs.universe_rows()
    print(len(rows))
    now = pd.Timestamp.utcnow()
    open_rows = []
    for row in rows:
        if row.liquid_schedule.is_open(now=now):
            open_rows.append(row)

    print(len(open_rows))
    print("open_market_rows[0]", open_rows[0])
    open_sorted_rows = MarginInfo().sort_by_margin(open_rows)
    print("open_sorted_rows[0]", open_sorted_rows[0])
