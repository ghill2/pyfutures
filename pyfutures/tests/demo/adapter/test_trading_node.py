import asyncio
import logging

from nautilus_trader.model.identifiers import TraderId
from pyfutures.logger import LoggerAttributes
from pyfutures.tests.test_kit import IBTestProviderStubs
from pyfutures.tests.demo.adapter.stubs import AdapterStubs

from nautilus_trader.model.data import BarType
from nautilus_trader.model.enums import OrderSide
from pytower.tests.stubs.strategies import BuyOnBarX


async def main():
    rows = IBTestProviderStubs.universe_rows()
    loop = asyncio.get_event_loop()

    tasks = []
    for row in rows[0:1]:
        LoggerAttributes.level = logging.DEBUG
        LoggerAttributes.id = row.trading_class
        node = AdapterStubs.trading_node(loop=loop, trader_id=TraderId("TRADER-001"), load_ids=[row.instrument_id.value])

        # bar_type = BarType.from_str("EUR.GBP=CASH.IDEALPRO-5-SECOND-BID-EXTERNAL")
        strategy = BuyOnBarX(
            index=1,
            bar_type=BarType.from_str(f"{row.instrument_id_live.value}-5-SECOND-BID-EXTERNAL"),
            order_side=OrderSide.BUY,
            quantity=1,
        )
        node.trader.add_strategy(strategy)

        tasks.append(loop.create_task(node.run_async()))

    # check if task was running
    # check if all instruments specified were loaded into the provider

    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.get_event_loop().run_until_complete(main())
