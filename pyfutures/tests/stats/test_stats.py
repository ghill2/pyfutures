import pytest

from pytower.stats.fees import FeeCalculator
from pyfutures.tests.demo.client.stubs import ClientStubs
from pyfutures.tests.test_kit import IBTestProviderStubs


@pytest.mark.asyncio()
async def test_all_fees_calculated(event_loop):
    # assert that the fees are not the same for the percent rows
    client = ClientStubs.client(loop=event_loop)

    fees_calc = FeeCalculator(client=client)
    rows = IBTestProviderStubs.universe_rows()
    fees = await fees_calc.calculate_rows(rows)
