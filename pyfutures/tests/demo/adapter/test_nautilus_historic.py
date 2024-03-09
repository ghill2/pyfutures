import pandas as pd
import pytest
from nautilus_trader.adapters.interactive_brokers.common import IBContract
from nautilus_trader.adapters.interactive_brokers.historic.client import HistoricInteractiveBrokersClient


@pytest.mark.asyncio()
async def test_nautilus_historic():
    """To compare functionality with pyfutures historic client"""
    historic = HistoricInteractiveBrokersClient()
    await historic.request_bars(
        bar_specifications=["1-MINUTE-LAST"],
        end_date_time=(pd.Timestamp.utcnow() - pd.Timedelta(days=1)).floor("1D"),
        tz_name="",
        start_date_time=pd.Timestamp("2020-02-02 00:00:00+00:00", utc=True),
        # duration="1 min",
        contracts=[IBContract(secType="STK", exchange="ARCA", symbol="SPY")],
        instrument_ids=["SPY.ARCA"],
    )

    # for (
    #     segment_end_date_time,
    #     segment_duration,
    # ) in historic._calculate_duration_segments(
    #     start_date=pd.Timestamp("2020-02-02 00:00:00+00:00"),
    #     end_date=(pd.Timestamp.utcnow() - pd.Timedelta(days=1)).floor("1D"),
    #     duration=None,
    # ):
    #     print(segment_end_date_time, segment_duration)
    #
