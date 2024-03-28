import pytest
from nautilus_trader.model.data import BarType

from pyfutures.tests.unit.adapter.stubs import AdapterStubs


class TestContinuousDataReconcilicationDemo:
    @pytest.mark.asyncio()
    async def test_reconcile_no_position(self, event_loop):
        self.bar_type = BarType.from_str("MES.SIM-1-DAY-MID-EXTERNAL")
        self.data = AdapterStubs.continuous_data(reconciliation=True)
        self.node = AdapterStubs.trading_node()
        self.node.trader.add_actor(self.data)

        # Arrange

        # Act
        await self.node.run_async()
