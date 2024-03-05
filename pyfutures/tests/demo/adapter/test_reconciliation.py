import asyncio
from unittest.mock import AsyncMock

import pytest


"""
generate responses for each active order situation

"""


class TestLMAXReconciliation:
    @pytest.mark.asyncio()
    async def test_generate_trade_reports(self, exec_client):
        reports = await exec_client.generate_trade_reports()
        print(reports)

    @pytest.mark.asyncio()
    async def test_generate_position_status_report(self, exec_client):
        reports = await exec_client.generate_position_status_reports()
        print(reports)

    @pytest.mark.asyncio()
    async def test_generate_order_status_reports(self, exec_client):
        reports = await exec_client.generate_order_status_reports()
        print(reports)

    @pytest.mark.asyncio()
    async def test_recon_no_cached_positions(self, exec_engine, cache, client, exec_client):
        # create the positions from the
        # position.avgCost
        # last_qty
        # events = await client.request_portfolio()

        # get last_px from the accountUpdate
        # filled = OrderFilled(
        #     trader_id=order.trader_id,
        #     strategy_id=order.strategy_id,
        #     instrument_id=report.instrument_id,
        #     client_order_id=order.client_order_id,
        #     venue_order_id=report.venue_order_id,
        #     account_id=report.account_id,
        #     position_id=PositionId(f"{instrument.id}-EXTERNAL"),
        #     trade_id=TradeId(UUID4().value),
        #     order_side=order.side,
        #     order_type=order.order_type,
        #     last_qty=last_qty,
        #     last_px=last_px,
        #     currency=instrument.quote_currency,
        #     commission=commission,
        #     liquidity_side=liquidity_side,
        #     event_id=UUID4(),
        #     ts_event=report.ts_last,
        #     ts_init=self._clock.timestamp_ns(),
        #     reconciliation=True,
        # )

        # OrderFilled()
        # messages = [
        #     b'61\x003\x00DU1234567\x00586139726\x00MES\x00FUT\x0020231215\x000.0\x00\x005\x00\x00USD\x00MESZ3\x00MES\x003\x0022166.03666665\x00',
        #     b'61\x003\x00DU1234567\x00296349625\x00QM\x00FUT\x0020231117\x000.0\x00\x00500\x00\x00USD\x00QMZ3\x00QM\x0016\x0039148.16375\x00',
        #     b'61\x003\x00DU1234567\x00564400671\x00D\x00FUT\x0020240125\x000.0\x00\x0010\x00\x00USD\x00RCF4\x00RC\x008\x0024446.85\x00',
        #     b'62\x001\x00',
        # ]

        # async def send_messages(*args, **kwargs):
        #     while len(messages) > 0:
        #         client._handle_msg(messages.pop(0))

        # send_mock = Mock(side_effect=send_messages)
        # client._conn.sendMsg = send_mock

        # positions = await asyncio.wait_for(client.request_positions(), 2)

        # instrument_id = InstrumentId.from_str("ZB[Z23].CBOT")

        # quote = await exec_client.request_bars(instrument_id)

        # exec_client.generate_position_status_reports = AsyncMock(return_value=[])
        exec_client.generate_trade_reports = AsyncMock(return_value=[])
        exec_client.generate_order_status_reports = AsyncMock(return_value=[])
        # exec_client.generate_position_status_reports = AsyncMock(side_effect=send_messages)

        # msg = "8=FIX.4.4|35=8|1=1|11=C-001|48=100934|22=8|54=1|37=1|59=4|40=1|60=20230825-23:14:16.100|6=26045.58|17=0|527=0|790=1|39=4|150=I|14=0.01|151=0|38=0.02|10=0"
        # instrument = exec_client.instrument_provider.find_with_security_id(100934)
        # report: OrderStatusReport = string_to_message(msg).to_nautilus(instrument=instrument)

        # exec_client.generate_order_status_reports = AsyncMock(
        #     return_value=[report],
        # )

        result = await asyncio.wait_for(
            exec_engine.reconcile_state(),
            2,
        )

        await asyncio.sleep(2)

        # Assert
        assert result

        # assert len(cache.orders()) == 1
        # assert exec_client.cache.orders()[0].status == OrderStatus.CANCELED
