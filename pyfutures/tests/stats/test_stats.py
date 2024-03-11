import pytest

from pytower.stats.fees import FeeCalculator
from pytower.stats.stats import Stats
from pyfutures.tests.demo.client.stubs import ClientStubs
from pyfutures.tests.test_kit import IBTestProviderStubs
from pathlib import Path


@pytest.mark.asyncio()
async def test_all_fees_calculated(event_loop):
    # assert that the fees are not the same for the percent rows
    client = ClientStubs.client(loop=event_loop)
    fees_calc = FeeCalculator(client=client)
    rows = IBTestProviderStubs.universe_rows()
    fees = await fees_calc.calculate_rows(rows)


@pytest.mark.asyncio()
async def test_stats_return_expected(event_loop):
    client = ClientStubs.client(loop=event_loop)
    await client.connect()
    adjusted_dir = Path.home() / "Desktop" / "adjusted"
    stats = Stats(client=client, adjusted_dir=adjusted_dir)

    rows = IBTestProviderStubs.universe_rows()
    stats_dict = await stats.calc(rows)
    outdir = Path.home() / "Desktop" / "stats"
    stats.write(stats=stats_dict, outdir=outdir)
